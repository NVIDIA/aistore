// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"net/url"
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
	Bck cmn.Bck

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
	var err error
	provider, err = cmn.NormalizeProvider(provider)
	debug.AssertNoErr(err)
	bck := &Bck{Name: name, Provider: provider, Ns: ns}
	if len(optProps) > 0 {
		bck.Props = optProps[0]
		debug.Assert(bck.Props != nil)
	}
	return bck
}

// clone (*cluster.Bck | *cmn.Bck) <=> (cmn.Bck | cluster.Bck) respectively
func (b *Bck) Clone() cmn.Bck    { return cmn.Bck(*b) }
func CloneBck(bck *cmn.Bck) *Bck { b := *bck; return (*Bck)(&b) }

// cast *cluster.Bck => *cmn.Bck
func (b *Bck) Bucket() *cmn.Bck { return (*cmn.Bck)(b) }

//
// inline delegations => cmn.Bck
//
func (b *Bck) IsAIS() bool                        { return (*cmn.Bck)(b).IsAIS() }
func (b *Bck) HasProvider() bool                  { return (*cmn.Bck)(b).HasProvider() }
func (b *Bck) IsHTTP() bool                       { return (*cmn.Bck)(b).IsHTTP() }
func (b *Bck) IsHDFS() bool                       { return (*cmn.Bck)(b).IsHDFS() }
func (b *Bck) IsCloud() bool                      { return (*cmn.Bck)(b).IsCloud() }
func (b *Bck) IsRemote() bool                     { return (*cmn.Bck)(b).IsRemote() }
func (b *Bck) IsRemoteAIS() bool                  { return (*cmn.Bck)(b).IsRemoteAIS() }
func (b *Bck) RemoteBck() *cmn.Bck                { return (*cmn.Bck)(b).RemoteBck() }
func (b *Bck) Validate() error                    { return (*cmn.Bck)(b).Validate() }
func (b *Bck) MakeUname(name string) string       { return (*cmn.Bck)(b).MakeUname(name) }
func (b *Bck) IsEmpty() bool                      { return (*cmn.Bck)(b).IsEmpty() }
func (b *Bck) AddToQuery(q url.Values) url.Values { return (*cmn.Bck)(b).AddToQuery(q) }

func (b *Bck) Backend() *Bck { backend := (*cmn.Bck)(b).Backend(); return (*Bck)(backend) }

func (b *Bck) AddUnameToQuery(q url.Values, uparam string) url.Values {
	bck := (*cmn.Bck)(b)
	return bck.AddUnameToQuery(q, uparam)
}

func (b *Bck) MaskBID(i int64) uint64 {
	bck := (*cmn.Bck)(b)
	if bck.IsAIS() {
		return uint64(i) | aisBIDmask
	}
	return uint64(i)
}

func (b *Bck) unmaskBID() uint64 {
	if b.Props == nil || b.Props.BID == 0 {
		return 0
	}
	bck := (*cmn.Bck)(b)
	if bck.IsAIS() {
		return b.Props.BID ^ aisBIDmask
	}
	return b.Props.BID
}

func (b *Bck) String() string {
	var (
		s   string
		bid = b.unmaskBID()
	)
	bck := (*cmn.Bck)(b)
	if bid == 0 {
		return bck.String()
	}
	if backend := bck.Backend(); backend != nil {
		s = ", backend=" + backend.String()
	}
	return fmt.Sprintf("%s(%#x%s)", bck, bid, s)
}

func (b *Bck) Equal(other *Bck, sameID, sameBackend bool) bool {
	left, right := (*cmn.Bck)(b), (*cmn.Bck)(other)
	if !left.Equal(right) {
		return false
	}
	if sameID && b.Props != nil && other.Props != nil && b.Props.BID != other.Props.BID {
		return false
	}
	if !sameBackend {
		return true
	}
	if backleft, backright := left.Backend(), right.Backend(); backleft != nil && backright != nil {
		return backleft.Equal(backright)
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
func (b *Bck) Init(bowner Bowner) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if err := b.InitNoBackend(bowner); err != nil {
		return err
	}
	backend := b.Backend()
	if backend == nil {
		return nil
	}
	// init backend
	if backend.Provider == "" || backend.IsAIS() {
		return fmt.Errorf("bucket %s: invalid backend %s (must be remote)", b, backend)
	}
	if err := backend.Validate(); err != nil {
		return err
	}
	backend.Props = nil // always reinitialize
	return backend.InitNoBackend(bowner)
}

// is used to init LOM, skips validations (compare with `Init` above)
func (b *Bck) initFast(bowner Bowner) error {
	if err := b.InitNoBackend(bowner); err != nil {
		return err
	}
	backend := b.Backend()
	if backend == nil {
		return nil
	}
	backend.Props = nil // ditto
	return backend.InitNoBackend(bowner)
}

func (b *Bck) InitNoBackend(bowner Bowner) error {
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
		return nil
	}
	if b.IsAIS() {
		return cmn.NewErrBckNotFound(b.Bucket())
	}
	return cmn.NewErrRemoteBckNotFound(b.Bucket())
}

func (b *Bck) CksumConf() (conf *cmn.CksumConf) { return &b.Props.Cksum }

func (b *Bck) VersionConf() cmn.VersionConf {
	if backend := b.Backend(); backend != nil && backend.Props != nil {
		conf := backend.Props.Versioning
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
