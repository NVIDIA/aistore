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
	"github.com/OneOfOne/xxhash"
)

const (
	aisBIDmask = uint64(1 << 63)
)

type (
	Bck struct {
		cmn.Bck
		Props *cmn.BucketProps
	}
	NameLockPair struct {
		uname string
		nlc   *nlc
	}
)

var (
	bckLocker nameLocker
)

func InitProxy() {
	bckLocker = make(nameLocker, cmn.MultiSyncMapCount)
	bckLocker.init()
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
		cmn.Assert(provider == "" || provider == cmn.AnyCloud)
	}
	b := &Bck{Bck: bck, Props: props}
	return b
}

func NewBckEmbed(bck cmn.Bck) *Bck { return &Bck{Bck: bck} }

func parseUname(uname string) (b Bck, objName string) {
	var (
		prev, itemIdx int
	)
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
		bid = b.unmaskBID()
	)
	if bid == 0 {
		return b.Bck.String()
	}
	return fmt.Sprintf("%s(%#x)", b.Bck.String(), bid)
}

func (b *Bck) Equal(other *Bck, sameID bool) bool {
	if b.Name != other.Name {
		return false
	}
	if b.Ns != other.Ns {
		return false
	}
	if sameID {
		if b.Props != nil && other.Props != nil {
			if b.Props.BID != other.Props.BID {
				return false
			}
		}
	}
	if b.IsAIS() && other.IsAIS() {
		return true
	}
	if (b.HasBackendBck() && !other.HasBackendBck()) || (!b.HasBackendBck() && other.HasBackendBck()) {
		// Case when either bck has backend bucket - we say it's a match since
		// backend bucket was either connected or disconnected.
		return true
	}
	if b.IsCloud() && other.IsCloud() {
		return true
	}
	return b.IsRemoteAIS() && other.IsRemoteAIS()
}

func (b *Bck) IsAIS() bool { return b.Bck.IsAIS() && !b.HasBackendBck() }
func (b *Bck) IsCloud(anyCloud ...string) bool {
	return b.Bck.IsCloud(anyCloud...) || b.HasBackendBck()
}
func (b *Bck) IsRemote() bool      { return b.Bck.IsRemote() || b.HasBackendBck() }
func (b *Bck) HasBackendBck() bool { return !b.Props.BackendBck.IsEmpty() }
func (b *Bck) CloudBck() cmn.Bck {
	// NOTE: It's required that props are initialized for AIS bucket. It
	//  might not be the case for cloud buckets (see: `HeadBucket`).
	if b.Provider == cmn.ProviderAIS && b.HasBackendBck() {
		return b.Props.BackendBck
	}
	cmn.Assert(b.Bck.IsCloud())
	return b.Bck
}

// NOTE: when the specified bucket is not present in the BMD:
// - always returns the corresponding *DoesNotExist error
// - for Cloud bucket - fills in the props with defaults from config
// - for AIS bucket - sets the props to nil
// - for Remote (Cloud or Remote AIS) bucket, the caller can type-cast err.(*cmn.ErrorRemoteBucketDoesNotExist) and proceed
func (b *Bck) Init(bowner Bowner, si *Snode) (err error) {
	bmd := bowner.Get()
	if b.Provider == "" {
		bmd.initBckAnyProvider(b)
	} else if b.Bck.IsCloud(cmn.AnyCloud) {
		cloudConf := cmn.GCO.Get().Cloud
		b.Provider = cloudConf.Provider
		b.Ns = cloudConf.Ns // TODO -- FIXME: remove
		bmd.initBckCloudProvider(b)
	} else {
		b.Props, _ = bmd.Get(b)
	}
	if b.Props == nil {
		var name string
		if si != nil {
			name = si.Name()
		}
		if b.Bck.IsAIS() {
			return cmn.NewErrorBucketDoesNotExist(b.Bck, name)
		}
		return cmn.NewErrorRemoteBucketDoesNotExist(b.Bck, name)
	}
	return
}

func (b *Bck) CksumConf() (conf *cmn.CksumConf) { return &b.Props.Cksum }

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

func (b *Bck) GetNameLockPair() (nlp NameLockPair) {
	nlp.uname = b.MakeUname("")
	hash := xxhash.ChecksumString64S(nlp.uname, cmn.MLCG32)
	idx := int(hash & (cmn.MultiSyncMapCount - 1))
	nlp.nlc = &bckLocker[idx]
	return
}

const nlpTryDuration = 5 * time.Second

func (nlp *NameLockPair) Lock()          { nlp.nlc.Lock(nlp.uname, true) }
func (nlp *NameLockPair) Unlock()        { nlp.nlc.Unlock(nlp.uname, true) }
func (nlp *NameLockPair) TryLock() bool  { return nlp.withRetry(nlpTryDuration, true) }
func (nlp *NameLockPair) TryRLock() bool { return nlp.withRetry(nlpTryDuration, false) }
func (nlp *NameLockPair) RUnlock()       { nlp.nlc.Unlock(nlp.uname, false) }

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
