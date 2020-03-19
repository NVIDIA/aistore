// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
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

const (
	bmdFname    = ".ais.bmd" // BMD basename
	bmdFext     = ".prev"    // suffix: previous version
	bmdTermName = "BMD"      // display name
	bmdCopies   = 2          // local copies
)

type (
	bucketMD struct {
		cluster.BMD
		vstr string // itoa(Version), to have it handy for http redirects
	}
	bmdOwner interface {
		sync.Locker
		init()
		put(bmd *bucketMD)
		get() (bmd *bucketMD)
		Get() *cluster.BMD
	}
	bmdOwnerBase struct {
		sync.Mutex
		bucketmd atomic.Pointer
	}
	bmdOwnerPrx struct {
		bmdOwnerBase
		fpath string
	}
	bmdOwnerTgt struct{ bmdOwnerBase }
)

var (
	// interface guard
	_ revs           = &bucketMD{}
	_ cluster.Bowner = &bmdOwnerBase{}
	_ bmdOwner       = &bmdOwnerPrx{}
	_ bmdOwner       = &bmdOwnerTgt{}
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
	return cmn.GenUUID(), time.Now().String()
}

//////////////
// bucketMD //
//////////////

func (m *bucketMD) add(bck *cluster.Bck, p *cmn.BucketProps) bool {
	if !cmn.IsValidProvider(bck.Provider) {
		cmn.AssertMsg(false, bck.String()+": invalid provider")
	}
	if _, present := m.Get(bck); present {
		return false
	}
	m.Version++
	p.BID = bck.MaskBID(m.Version)
	p.Provider = bck.Provider
	bck.Props = p

	m.Add(bck)
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
	if !cmn.IsValidProvider(bck.Provider) {
		cmn.AssertMsg(false, bck.String()+": invalid provider")
	}
	prevProps, present := m.Get(bck)
	if !present {
		cmn.AssertMsg(false, bck.String()+": not present")
	}
	cmn.Assert(prevProps.BID != 0)

	p.BID = prevProps.BID
	p.Provider = bck.Provider
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
	if m.UUID == "" || nbmd.UUID == "" {
		return
	}
	if m.UUID == nbmd.UUID {
		return
	}
	nsiname := caller
	if nsi != nil {
		nsiname = nsi.Name()
	} else if nsiname == "" {
		nsiname = "???"
	}
	hname := si.Name()
	// FATAL: cluster integrity error (cie)
	s := fmt.Sprintf("%s: BMDs have different uuids: [%s: %s] vs [%s: %s]",
		ciError(40), hname, m.StringEx(), nsiname, nbmd.StringEx())
	err = &errPrxBmdUUIDDiffer{s}
	return
}

//
// Implementation of revs interface
//
func (m *bucketMD) tag() string    { return revsBMDTag }
func (m *bucketMD) version() int64 { return m.Version }
func (m *bucketMD) marshal() []byte {
	b, err := jsonCompat.Marshal(m) // jsoniter + sorting
	cmn.AssertNoErr(err)
	return b
}

//////////////////
// bmdOwnerBase //
//////////////////

func (bo *bmdOwnerBase) _put(bucketmd *bucketMD) {
	bucketmd.vstr = strconv.FormatInt(bucketmd.Version, 10)
	bo.bucketmd.Store(unsafe.Pointer(bucketmd))
}

func (bo *bmdOwnerBase) Get() *cluster.BMD         { return &bo.get().BMD }
func (bo *bmdOwnerBase) get() (bucketmd *bucketMD) { return (*bucketMD)(bo.bucketmd.Load()) }

/////////////////
// bmdOwnerPrx //
/////////////////

func newBMDOwnerPrx(config *cmn.Config) *bmdOwnerPrx {
	return &bmdOwnerPrx{fpath: filepath.Join(config.Confdir, bmdFname)}
}

func (bo *bmdOwnerPrx) init() {
	var bmd = newBucketMD()
	err := jsp.Load(bo.fpath, bmd, jsp.CCSign())
	if err != nil && !os.IsNotExist(err) {
		glog.Errorf("failed to load %s from %s, err: %v", bmdTermName, bo.fpath, err)
	}
	bo._put(bmd)
}

func (bo *bmdOwnerPrx) put(bmd *bucketMD) {
	bo._put(bmd)
	err := jsp.Save(bo.fpath, bmd, jsp.CCSign())
	if err != nil {
		glog.Errorf("failed to write %s as %s, err: %v", bmdTermName, bo.fpath, err)
	}
}

/////////////////
// bmdOwnerTgt //
/////////////////

func newBMDOwnerTgt() *bmdOwnerTgt {
	return &bmdOwnerTgt{}
}

func (bo *bmdOwnerTgt) find() (avail, curr, prev fs.MPI) {
	avail, _ = fs.Mountpaths.Get()
	curr, prev = make(fs.MPI, 2), make(fs.MPI, 2)
	for mpath, mpathInfo := range avail {
		fpath := filepath.Join(mpath, bmdFname)
		if err := fs.Access(fpath); err == nil {
			curr[mpath] = mpathInfo
		}
		fpath += bmdFext
		if err := fs.Access(fpath); err == nil {
			prev[mpath] = mpathInfo
		}
	}
	return
}

func (bo *bmdOwnerTgt) init() {
	load := func(mpi fs.MPI, suffix bool) (bmd *bucketMD) {
		bmd = newBucketMD()
		for mpath := range mpi {
			fpath := filepath.Join(mpath, bmdFname)
			if suffix {
				fpath += bmdFext
			}
			err := jsp.Load(fpath, bmd, jsp.CCSign())
			if err == nil {
				break
			}
			if !os.IsNotExist(err) {
				glog.Errorf("failed to load %s from %s, err: %v", bmdTermName, fpath, err)
			}
		}
		return
	}

	var (
		bmd           *bucketMD
		_, curr, prev = bo.find()
	)
	if len(curr) > 0 {
		bmd = load(curr, false)
	}
	if bmd == nil && len(prev) > 0 {
		glog.Errorf("attempting to load older %s version...", bmdTermName)
		bmd = load(prev, true)
	}
	if bmd == nil {
		glog.Infof("instantiating empty %s", bmdTermName)
		bmd = newBucketMD()
	}
	bo._put(bmd)
}

func (bo *bmdOwnerTgt) put(bmd *bucketMD) {
	var (
		avail, curr, prev = bo.find()
		cnt               int
	)
	bo._put(bmd)
	// write new
	for mpath := range avail {
		fpath := filepath.Join(mpath, bmdFname)
		if err := jsp.Save(fpath, bmd, jsp.CCSign()); err != nil {
			glog.Errorf("failed to store %s as %s, err: %v", bmdTermName, fpath, err)
			continue
		}
		cnt++
		delete(curr, mpath)
		if cnt >= bmdCopies {
			break
		}
	}
	if cnt == 0 {
		glog.Errorf("failed to store %s (have zero copies)", bmdTermName)
		return
	}
	// rename remaining prev
	for mpath := range curr {
		from := filepath.Join(mpath, bmdFname)
		to := from + bmdFext
		if err := os.Rename(from, to); err != nil {
			glog.Errorf("failed to rename %s prev version, err: %v", bmdTermName, err)
		}
		delete(prev, mpath)
	}
	// remove remaining older
	for mpath := range prev {
		fpath := filepath.Join(mpath, bmdFname) + bmdFext
		if err := os.Remove(fpath); err != nil {
			glog.Errorf("failed to remove %s prev version, err: %v", bmdTermName, err)
		}
	}
}
