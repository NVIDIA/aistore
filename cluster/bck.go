// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/OneOfOne/xxhash"
)

const (
	aisBIDmask = uint64(1 << 63)

	nlpTryDefault = time.Second // nlp.TryLock default duration
)

type (
	Bck struct {
		cmn.Bck
	}
	noCopy       struct{}
	NameLockPair struct {
		noCopy    noCopy
		uname     string
		nlc       *nlc
		exclusive bool
	}
)

// interface guard
var _ cmn.NLP = (*NameLockPair)(nil)

var bckLocker nameLocker

func InitBckLocker() {
	bckLocker = make(nameLocker, cmn.MultiSyncMapCount)
	bckLocker.init()

	initDumpNameLocks() // when built with debug
}

func NewBck(name, provider string, ns cmn.Ns, optProps ...*cmn.BucketProps) *Bck {
	var (
		props *cmn.BucketProps
		bck   = cmn.Bck{Name: name, Provider: provider, Ns: ns}
	)

	if len(optProps) > 0 {
		props = optProps[0]
	}
	if !cmn.IsValidProvider(provider) {
		cmn.Assert(provider == "")
	}
	bck.Props = props
	b := &Bck{Bck: bck}
	return b
}

func NewBckEmbed(bck cmn.Bck) *Bck { return &Bck{Bck: bck} }
func BackendBck(bck *Bck) *Bck     { return &Bck{Bck: bck.Props.BackendBck} }

func parseUname(uname string) (b Bck, objName string) {
	var prev, itemIdx int
	for i := 0; i < len(uname); i++ {
		if uname[i] != filepath.Separator {
			continue
		}

		item := uname[prev:i]
		switch itemIdx {
		case 0:
			b.Provider = item
		case 1:
			b.Ns = cmn.ParseNsUname(item)
		case 2:
			b.Name = item
			objName = uname[i+1:]
			return
		}

		itemIdx++
		prev = i + 1
	}
	return
}

func (b *Bck) MakeUname(objName string) string {
	var (
		nsUname = b.Ns.Uname()
		l       = len(b.Provider) + 1 + len(nsUname) + 1 + len(b.Name) + 1 + len(objName)
		buf     = make([]byte, 0, l)
	)
	buf = append(buf, b.Provider...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, nsUname...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, b.Name...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return *(*string)(unsafe.Pointer(&buf))
}

func (b *Bck) MaskBID(i int64) uint64 {
	if b.IsAIS() {
		return uint64(i) | aisBIDmask
	}
	return uint64(i)
}

func (b *Bck) unmaskBID() uint64 {
	if b.Props == nil || b.Props.BID == 0 {
		return 0
	}
	if b.IsAIS() {
		return b.Props.BID ^ aisBIDmask
	}
	return b.Props.BID
}

func (b *Bck) String() string {
	var (
		s   string
		bid = b.unmaskBID()
	)
	if bid == 0 {
		return b.Bck.String()
	}
	if b.HasBackendBck() {
		s = ", backend=" + b.Props.BackendBck.String()
	}
	return fmt.Sprintf("%s(%#x%s)", b.Bck.String(), bid, s)
}

func (b *Bck) Equal(other *Bck, sameID, sameBackend bool) bool {
	left, right := b.Bck, other.Bck
	left.Props, right.Props = nil, nil
	if left != right {
		return false
	}
	if sameID && b.Props != nil && other.Props != nil && b.Props.BID != other.Props.BID {
		return false
	}
	if !sameBackend {
		return true
	}
	if b.HasBackendBck() && other.HasBackendBck() {
		left, right = *b.BackendBck(), *other.BackendBck()
		left.Props, right.Props = nil, nil
		return left == right
	}
	return true
}

// NOTE: when the specified bucket is not present in the BMD:
// - always returns the corresponding *DoesNotExist error
// - Cloud bucket: fills in the props with defaults from config
// - AIS bucket: sets the props to nil
// - Remote (Cloud or Remote AIS) bucket: caller can type-cast err.(*cmn.ErrorRemoteBucketDoesNotExist) and proceed
//
// NOTE: most of the above applies to a backend bucket, if specified
//
func (b *Bck) Init(bowner Bowner, si *Snode) (err error) {
	err = b.InitNoBackend(bowner, si)
	if err != nil {
		return
	}
	if !b.HasBackendBck() {
		return
	}
	backend := NewBckEmbed(b.Props.BackendBck)
	if !backend.IsCloud() {
		err = fmt.Errorf("bucket %s: invalid backend %s (not a Cloud bucket)", b, backend)
		return
	}
	err = backend.Init(bowner, si)
	if err == nil {
		cmn.Assert(!backend.HasBackendBck())
	}
	b.Props.BackendBck = backend.Bck
	return
}

func (b *Bck) InitNoBackend(bowner Bowner, si *Snode) (err error) {
	bmd := bowner.Get()
	if b.Provider == "" {
		bmd.initBckAnyProvider(b)
	} else if b.Bck.IsCloud() {
		debug.Assert(b.Ns == cmn.NsGlobal)
		bmd.initBckCloudProvider(b)
	} else if b.IsHTTP() {
		debug.Assert(b.Ns == cmn.NsGlobal)
		present := bmd.initBckCloudProvider(b)
		if debug.Enabled && present {
			var (
				origURL = b.Props.Extra.OrigURLBck
				bckName = cmn.OrigURLBck2Name(origURL)
			)
			debug.Assertf(b.Name == bckName, "%s != %s; original_url: %s", b.Name, bckName, origURL)
		}
	} else {
		b.Props, _ = bmd.Get(b)
	}
	if b.Props != nil {
		return
	}
	var name string
	if si != nil {
		name = si.Name()
	}
	if b.Bck.IsAIS() {
		return cmn.NewErrorBucketDoesNotExist(b.Bck, name)
	}
	return cmn.NewErrorRemoteBucketDoesNotExist(b.Bck, name)
}

func (b *Bck) CksumConf() (conf *cmn.CksumConf) { return &b.Props.Cksum }

func (b *Bck) VersionConf() cmn.VersionConf {
	if b.HasBackendBck() && b.Props.BackendBck.Props != nil {
		conf := b.Props.BackendBck.Props.Versioning
		conf.ValidateWarmGet = b.Props.Versioning.ValidateWarmGet
		return conf
	}
	return b.Props.Versioning
}

//
// access perms
//
func (b *Bck) Allow(bit int) error { return b.checkAccess(bit) }

func (b *Bck) checkAccess(bit int) (err error) {
	if b.Props.Access.Has(cmn.AccessAttrs(bit)) {
		return
	}
	op := cmn.AccessOp(bit)
	err = cmn.NewBucketAccessDenied(b.String(), op, b.Props.Access)
	return
}

//
// lock/unlock
//

func (b *Bck) GetNameLockPair() (nlp *NameLockPair) {
	nlp = &NameLockPair{uname: b.MakeUname("")}
	hash := xxhash.ChecksumString64S(nlp.uname, cmn.MLCG32)
	idx := int(hash & (cmn.MultiSyncMapCount - 1))
	nlp.nlc = &bckLocker[idx]
	return
}

func (nlp *NameLockPair) Lock() {
	nlp.nlc.Lock(nlp.uname, true)
	nlp.exclusive = true
}

func (nlp *NameLockPair) TryLock(timeout time.Duration) (ok bool) {
	if timeout == 0 {
		timeout = nlpTryDefault
	}
	ok = nlp.withRetry(timeout, true)
	nlp.exclusive = ok
	return
}

// TODO: ensure single-time usage (no ref counting!)
func (nlp *NameLockPair) TryRLock(timeout time.Duration) (ok bool) {
	if timeout == 0 {
		timeout = nlpTryDefault
	}
	ok = nlp.withRetry(timeout, false)
	cmn.Assert(!nlp.exclusive)
	return
}

func (nlp *NameLockPair) Unlock() {
	nlp.nlc.Unlock(nlp.uname, nlp.exclusive)
}

func (nlp *NameLockPair) withRetry(d time.Duration, exclusive bool) bool {
	if nlp.nlc.TryLock(nlp.uname, exclusive) {
		return true
	}
	i := d / 10
	for j := i; j < d; j += i {
		time.Sleep(i)
		if nlp.nlc.TryLock(nlp.uname, exclusive) {
			return true
		}
	}
	return false
}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
