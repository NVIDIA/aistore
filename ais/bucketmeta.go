// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/fs"
)

// NOTE: to access bucket metadata and related structures, external
//       packages and HTTP clients must import aistore/cluster (and not ais)

// - bucketMD is a server-side extension of the cluster.BMD
// - bucketMD represents buckets (that store objects) and associated metadata
// - bucketMD (instance) can be obtained via bmdOwner.get()
// - bucketMD is immutable and versioned
// - bucketMD versioning is monotonic and incremental
//
// - bucketMD typical update transaction:
// lock -- clone() -- modify the clone -- bmdOwner.put(clone) -- unlock
//
// (*) for merges and conflict resolution, check the current version prior to put()
//     (note that version check must be protected by the same critical section)
//

const bmdCopies = 2 // local copies

type (
	bucketMD struct {
		cluster.BMD
		vstr  string     // itoa(Version), to have it handy for http redirects
		cksum *cos.Cksum // BMD checksum
	}
	bmdOwner interface {
		sync.Locker
		Get() *cluster.BMD

		init()
		get() (bmd *bucketMD)
		put(bmd *bucketMD) error
		persist() error
		modify(*bmdModifier) (*bucketMD, error)
	}
	bmdOwnerBase struct {
		sync.Mutex
		bmd atomic.Pointer
	}
	bmdOwnerPrx struct {
		bmdOwnerBase
		fpath string
	}
	bmdOwnerTgt struct{ bmdOwnerBase }

	bmdModifier struct {
		pre   func(*bmdModifier, *bucketMD) error
		final func(*bmdModifier, *bucketMD)

		smap  *smapX
		msg   *cmn.ActionMsg
		txnID string // transaction UUID
		bcks  []*cluster.Bck

		propsToUpdate *cmn.BucketPropsToUpdate // update existing props
		revertProps   *cmn.BucketPropsToUpdate // props to revert
		setProps      *cmn.BucketProps         // new props to set
		cloudProps    http.Header

		wait         bool
		needReMirror bool
		needReEC     bool
		terminate    bool
	}
)

// interface guard
var (
	_ revs           = (*bucketMD)(nil)
	_ cluster.Bowner = (*bmdOwnerBase)(nil)
	_ bmdOwner       = (*bmdOwnerPrx)(nil)
	_ bmdOwner       = (*bmdOwnerTgt)(nil)
)

// c-tor
func newBucketMD() *bucketMD {
	providers := make(cluster.Providers, 2)
	namespaces := make(cluster.Namespaces, 1)
	providers[cmn.ProviderAIS] = namespaces
	buckets := make(cluster.Buckets, 16)
	namespaces[cmn.NsGlobal.Uname()] = buckets
	return &bucketMD{BMD: cluster.BMD{Providers: providers, UUID: ""}}
}

func newClusterUUID() (uuid, created string) {
	return cos.GenUUID(), time.Now().String()
}

//////////////
// bucketMD //
//////////////

func (m *bucketMD) add(bck *cluster.Bck, p *cmn.BucketProps) bool {
	cos.AssertNoErr(bck.ValidateProvider())
	if _, present := m.Get(bck); present {
		return false
	}

	p.SetProvider(bck.Provider)
	p.BID = bck.MaskBID(m.Version)
	p.Created = time.Now().UnixNano()
	bck.Props = p

	m.Add(bck)
	m.Version++

	return true
}

func (m *bucketMD) del(bck *cluster.Bck) (deleted bool) {
	if !m.Del(bck) {
		return
	}
	m.Version++
	return true
}

func (m *bucketMD) set(bck *cluster.Bck, p *cmn.BucketProps) {
	cos.AssertNoErr(bck.ValidateProvider())
	prevProps, present := m.Get(bck)
	if !present {
		cos.Assertf(false, "%s: not present", bck)
	}
	cos.Assert(prevProps.BID != 0)

	p.SetProvider(bck.Provider)
	p.BID = prevProps.BID

	m.Set(bck, p)
	m.Version++
}

func (m *bucketMD) clone() *bucketMD {
	dst := &bucketMD{}
	m.deepCopy(dst)
	return dst
}

func (m *bucketMD) deepCopy(dst *bucketMD) {
	dst.vstr = m.vstr
	m.DeepCopy(&dst.BMD)
}

func (m *bucketMD) validateUUID(nbmd *bucketMD, si, nsi *cluster.Snode, caller string) (err error) {
	if nbmd == nil || nbmd.Version == 0 || m.Version == 0 {
		return
	}
	if !cos.IsValidUUID(m.UUID) || !cos.IsValidUUID(nbmd.UUID) {
		return
	}
	if m.UUID == nbmd.UUID {
		return
	}
	nsiname := caller
	if nsi != nil {
		nsiname = nsi.String()
	} else if nsiname == "" {
		nsiname = "???"
	}
	hname := si.Name()
	// FATAL: cluster integrity error (cie)
	s := fmt.Sprintf("%s: BMDs have different UUIDs: (%s, %s) vs (%s, %s)",
		ciError(40), hname, m.StringEx(), nsiname, nbmd.StringEx())
	err = &errPrxBmdUUIDDiffer{s}
	return
}

//
// Implementation of revs interface
//
func (m *bucketMD) tag() string     { return revsBMDTag }
func (m *bucketMD) version() int64  { return m.Version }
func (m *bucketMD) marshal() []byte { return cos.MustMarshal(m) }

//////////////////
// bmdOwnerBase //
//////////////////

func (bo *bmdOwnerBase) Get() *cluster.BMD    { return &bo.get().BMD }
func (bo *bmdOwnerBase) get() (bmd *bucketMD) { return (*bucketMD)(bo.bmd.Load()) }
func (bo *bmdOwnerBase) _put(bmd *bucketMD) {
	bmd.vstr = strconv.FormatInt(bmd.Version, 10)
	bo.bmd.Store(unsafe.Pointer(bmd))
}

/////////////////
// bmdOwnerPrx //
/////////////////

func newBMDOwnerPrx(config *cmn.Config) *bmdOwnerPrx {
	return &bmdOwnerPrx{fpath: filepath.Join(config.ConfigDir, fs.BmdPersistedFileName)}
}

func (bo *bmdOwnerPrx) init() {
	bmd := newBucketMD()
	_, err := jsp.LoadMeta(bo.fpath, bmd)
	if err != nil && !os.IsNotExist(err) {
		glog.Errorf("failed to load %s from %s, err: %v", bmd, bo.fpath, err)
	}
	bo._put(bmd)
}

func (bo *bmdOwnerPrx) put(bmd *bucketMD) (err error) {
	bo._put(bmd)
	return bo.persist()
}

func (bo *bmdOwnerPrx) persist() (err error) {
	bmd := bo.get()
	if err = jsp.SaveMeta(bo.fpath, bmd); err != nil {
		err = fmt.Errorf("failed to write %s as %s, err: %w", bmd, bo.fpath, err)
	}
	return
}

func (bo *bmdOwnerPrx) _pre(ctx *bmdModifier) (clone *bucketMD, err error) {
	bo.Lock()
	defer bo.Unlock()
	clone = bo.get().clone()
	if err = ctx.pre(ctx, clone); err != nil || ctx.terminate {
		return
	}
	err = bo.put(clone)
	return
}

func (bo *bmdOwnerPrx) modify(ctx *bmdModifier) (clone *bucketMD, err error) {
	if clone, err = bo._pre(ctx); err != nil || ctx.terminate {
		return
	}
	if ctx.final != nil {
		ctx.final(ctx, clone)
	}
	return
}

/////////////////
// bmdOwnerTgt //
/////////////////

func newBMDOwnerTgt() *bmdOwnerTgt {
	return &bmdOwnerTgt{}
}

func (bo *bmdOwnerTgt) init() {
	var (
		available, _ = fs.Get()
		bmd          *bucketMD
	)

	if bmd = loadBMD(available, fs.BmdPersistedFileName); bmd != nil {
		glog.Infof("BMD loaded from %q", fs.BmdPersistedFileName)
		goto finalize
	}

	if bmd = loadBMD(available, fs.BmdPersistedPrevious); bmd != nil {
		glog.Infof("BMD loaded from %q", fs.BmdPersistedPrevious)
		goto finalize
	}

	bmd = newBucketMD()
	glog.Info("BMD freshly created")

finalize:
	bo._put(bmd)
}

func (bo *bmdOwnerTgt) put(bmd *bucketMD) (err error) {
	bo._put(bmd)
	return bo.persist()
}

func (bo *bmdOwnerTgt) persist() (err error) {
	bmd := bo.get()
	cnt, availCnt :=
		fs.PersistOnMpaths(fs.BmdPersistedFileName, fs.BmdPersistedPrevious, bmd, bmdCopies, bmd.JspOpts())
	if cnt > 0 {
		return
	}
	if availCnt == 0 {
		glog.Errorf("Cannot store %s: %v", bmd, fs.ErrNoMountpaths)
		return
	}
	err = fmt.Errorf("failed to store %s on any of the mountpaths (%d)", bmd, availCnt)
	glog.Error(err)
	return
}

func (bo *bmdOwnerTgt) modify(_ *bmdModifier) (*bucketMD, error) {
	// Method should not be used on targets.
	cos.Assert(false)
	return nil, nil
}

func loadBMD(mpaths fs.MPI, path string) (mainBMD *bucketMD) {
	for _, mpath := range mpaths {
		bmd := loadBMDFromMpath(mpath, path)
		if bmd == nil {
			continue
		}
		if mainBMD != nil {
			if !mainBMD.cksum.Equal(bmd.cksum) {
				cos.ExitLogf("BMD is different (%q): %v vs %v", mpath, mainBMD, bmd)
			}
			continue
		}
		mainBMD = bmd
	}
	return
}

func loadBMDFromMpath(mpath *fs.MountpathInfo, path string) (bmd *bucketMD) {
	var (
		fpath = filepath.Join(mpath.Path, path)
		err   error
	)
	bmd = newBucketMD()
	bmd.cksum, err = jsp.LoadMeta(fpath, bmd)
	if err == nil {
		return bmd
	}
	if !os.IsNotExist(err) {
		// Should never be NotExist error as mpi should include only mpaths with relevant bmds stored.
		glog.Errorf("failed to load %s from %s, err: %v", bmd, fpath, err)
	}
	return nil
}

func hasEnoughBMDCopies() bool {
	return len(fs.FindPersisted(fs.BmdPersistedFileName)) >= bmdCopies
}

//////////////////////////
// default bucket props //
//////////////////////////

type bckPropsArgs struct {
	bck *cluster.Bck // Base bucket for determining default bucket props.
	hdr http.Header  // Header with remote bucket properties.
}

func defaultBckProps(args bckPropsArgs) *cmn.BucketProps {
	var (
		skipValidate bool
		c            = cmn.GCO.Get()
		props        = cmn.DefaultBckProps(c)
	)
	debug.Assert(args.bck != nil)
	debug.AssertNoErr(cos.ValidateCksumType(c.Cksum.Type))
	props.SetProvider(args.bck.Provider)

	if args.bck.IsAIS() || args.bck.IsRemoteAIS() || args.bck.HasBackendBck() {
		debug.Assert(args.hdr == nil)
	} else if args.bck.IsCloud() || args.bck.IsHTTP() {
		debug.Assert(args.hdr != nil)
		props.Versioning.Enabled = false
		props = mergeRemoteBckProps(props, args.hdr)
	} else if args.bck.IsHDFS() {
		props.Versioning.Enabled = false
		if args.hdr != nil {
			props = mergeRemoteBckProps(props, args.hdr)
		}
		// Preserve HDFS related information.
		if args.bck.Props != nil {
			props.Extra.HDFS = args.bck.Props.Extra.HDFS
		} else {
			// Since the original bucket does not have the HDFS related info,
			// the validate will fail so we must skip.
			skipValidate = true
		}
	} else {
		cos.Assert(false)
	}

	if !skipValidate {
		// For debugging purposes we can set large value - we don't need to be precise here.
		debug.AssertNoErr(props.Validate(1000 /*targetCnt*/))
	}
	return props
}

func mergeRemoteBckProps(props *cmn.BucketProps, header http.Header) *cmn.BucketProps {
	cos.Assert(len(header) > 0)
	switch props.Provider {
	case cmn.ProviderAmazon:
		props.Extra.AWS.CloudRegion = header.Get(cmn.HeaderCloudRegion)
	case cmn.ProviderHTTP:
		props.Extra.HTTP.OrigURLBck = header.Get(cmn.HeaderOrigURLBck)
	}

	if verStr := header.Get(cmn.HeaderBucketVerEnabled); verStr != "" {
		versioning, err := cos.ParseBool(verStr)
		cos.AssertNoErr(err)
		props.Versioning.Enabled = versioning
	}
	return props
}
