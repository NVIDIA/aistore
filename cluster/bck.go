// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
		_         noCopy
		uname     string
		nlc       *nlc
		exclusive bool
	}
)

// interface guard
var _ cmn.NLP = (*NameLockPair)(nil)

var bckLocker nameLocker

func initBckLocker() {
	bckLocker = make(nameLocker, cos.MultiSyncMapCount)
	bckLocker.init()
}

func NewBck(name, provider string, ns cmn.Ns, optProps ...*cmn.BucketProps) *Bck {
	var (
		props *cmn.BucketProps
		err   error
	)

	provider, err = cmn.NormalizeProvider(provider)
	debug.AssertNoErr(err)

	bck := cmn.Bck{Name: name, Provider: provider, Ns: ns}
	if len(optProps) > 0 {
		props = optProps[0]
	}
	bck.Props = props
	b := &Bck{Bck: bck}
	return b
}

func BackendBck(bck *Bck) *Bck { return &Bck{Bck: bck.Props.BackendBck} }

// cluster.Bck <=> cmn.Bck
func NewBckEmbed(bck cmn.Bck) *Bck { return &Bck{Bck: bck} }
func (b *Bck) Bucket() cmn.Bck     { return b.Bck }

func (b *Bck) MakeUname(objName string) string { return b.Bck.MakeUname(objName) }

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
// - Remote (Cloud or Remote AIS) bucket: caller can type-cast err.(*cmn.ErrRemoteBckNotFound) and proceed
//
// NOTE: most of the above applies to a backend bucket, if specified
//
func (b *Bck) Init(bowner Bowner) (err error) {
	if err = b.InitNoBackend(bowner); err != nil {
		return
	}
	if !b.HasBackendBck() {
		return
	}
	backend := NewBckEmbed(b.Props.BackendBck)
	if backend.Provider == "" || backend.IsAIS() {
		return fmt.Errorf("bucket %s: invalid backend %s (must be remote)", b, backend)
	}
	backend.Props = nil // always re-init
	if err = backend.InitNoBackend(bowner); err != nil {
		return
	}
	if backend.HasBackendBck() {
		err = fmt.Errorf("bucket %s: invalid backend %s (recursion is not permitted)", b, backend)
	}
	return
}

func (b *Bck) InitNoBackend(bowner Bowner) (err error) {
	if err := b.Validate(); err != nil {
		return err
	}
	bmd := bowner.Get()
	if b.Provider == "" {
		bmd.initBckAnyProvider(b)
	} else if b.IsCloud() {
		debug.Assert(b.Ns == cmn.NsGlobal)
		bmd.initBckCloudProvider(b)
	} else if b.IsHTTP() {
		debug.Assert(b.Ns == cmn.NsGlobal)
		present := bmd.initBckCloudProvider(b)
		debug.Func(func() {
			if present {
				var (
					origURL = b.Props.Extra.HTTP.OrigURLBck
					bckName = cos.OrigURLBck2Name(origURL)
				)
				debug.Assertf(b.Name == bckName, "%s != %s; original_url: %s", b.Name, bckName, origURL)
			}
		})
	} else if b.IsHDFS() {
		debug.Assert(b.Ns == cmn.NsGlobal)
		present := bmd.initBckCloudProvider(b)
		debug.Assert(!present || b.Props.Extra.HDFS.RefDirectory != "")
	} else {
		b.Props, _ = bmd.Get(b)
	}
	if b.Props != nil {
		return
	}
	if b.Bck.IsAIS() {
		return cmn.NewErrBckNotFound(b.Bck)
	}
	return cmn.NewErrRemoteBckNotFound(b.Bck)
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
func (b *Bck) Allow(bit apc.AccessAttrs) error { return b.checkAccess(bit) }

func (b *Bck) checkAccess(bit apc.AccessAttrs) (err error) {
	if b.Props.Access.Has(bit) {
		return
	}
	op := apc.AccessOp(bit)
	err = cmn.NewBucketAccessDenied(b.String(), op, b.Props.Access)
	return
}

//
// lock/unlock
//

func (b *Bck) GetNameLockPair() (nlp *NameLockPair) {
	nlp = &NameLockPair{uname: b.MakeUname("")}
	hash := xxhash.ChecksumString64S(nlp.uname, cos.MLCG32)
	idx := int(hash & (cos.MultiSyncMapCount - 1))
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
	debug.Assert(!nlp.exclusive)
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
