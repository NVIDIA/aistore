// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/json"
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
	"github.com/NVIDIA/aistore/fs"

	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
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
	aisBckIDbit = uint64(1 << 63)
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
	_ json.Marshaler   = &bucketMD{}
	_ json.Unmarshaler = &bucketMD{}
	_ cluster.Bowner   = &bmdOwnerBase{}
	_ bmdOwner         = &bmdOwnerPrx{}
	_ bmdOwner         = &bmdOwnerTgt{}
)

// c-tor
func newBucketMD() *bucketMD {
	providers := make(cluster.Providers, 2)
	namespaces := make(cluster.Namespaces, 1)
	providers[cmn.ProviderAIS] = namespaces
	buckets := make(cluster.Buckets, 16)
	namespaces[cmn.NsGlobal.Uname()] = buckets
	return &bucketMD{BMD: cluster.BMD{Providers: providers, UUID: 0}}
}

func newClusterUUID() (uuid uint64, created string) {
	var (
		now    = time.Now()
		u, err = cmn.GenUUID64()
	)
	if err == nil {
		uuid = uint64(u)
	} else {
		glog.Error(err)
		uuid = uint64(now.UnixNano())
	}
	created = now.String()
	return
}

//////////////
// bucketMD //
//////////////

func (m *bucketMD) genBucketID(isais bool) uint64 {
	if !isais {
		return uint64(m.Version)
	}
	return uint64(m.Version) | aisBckIDbit
}

func (m *bucketMD) add(bck *cluster.Bck, p *cmn.BucketProps) bool {
	if !cmn.IsValidProvider(bck.Provider) {
		cmn.AssertMsg(false, bck.String()+": invalid provider")
	}
	if _, present := m.Get(bck); present {
		return false
	}
	m.Version++
	p.BID = m.genBucketID(bck.IsAIS())
	p.CloudProvider = bck.Provider
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
	cmn.Assert(!p.InProgress)
	if !cmn.IsValidProvider(bck.Provider) {
		cmn.AssertMsg(false, bck.String()+": invalid provider")
	}
	prevProps, present := m.Get(bck)
	if !present {
		cmn.AssertMsg(false, bck.String()+": not present")
	}
	cmn.Assert(prevProps.BID != 0)

	p.CloudProvider = bck.Provider
	p.BID = prevProps.BID
	m.Set(bck, p)
	m.Version++
}

func (m *bucketMD) toggleInProgress(bck *cluster.Bck, toggle bool) {
	p, present := m.Get(bck)
	cmn.Assert(present)
	if !toggle {
		cmn.Assert(p.InProgress)
	}
	p.InProgress = toggle
	m.Set(bck, p)
	m.Version++
}

func (m *bucketMD) downgrade(bck *cluster.Bck) {
	m.toggleInProgress(bck, true)
}

func (m *bucketMD) upgrade(bck *cluster.Bck) {
	m.toggleInProgress(bck, false)
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
	if m.UUID == 0 || nbmd.UUID == 0 {
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
// revs interface
//
func (m *bucketMD) tag() string    { return bmdtag }
func (m *bucketMD) version() int64 { return m.Version }

func (m *bucketMD) marshal() ([]byte, error) {
	return jsonCompat.Marshal(m) // jsoniter + sorting
}

// Extracts JSON payload and its checksum from []byte.
func (m *bucketMD) UnmarshalJSON(b []byte) error {
	aux := &struct {
		BMD   *cluster.BMD `json:"bmd"`
		Cksum uint64       `json:"cksum,string"`
	}{
		BMD: &m.BMD,
	}

	if err := jsoniter.Unmarshal(b, &aux); err != nil {
		return err
	}

	payload := cmn.MustMarshal(m.BMD)
	expectedCksum := xxhash.Checksum64S(payload, 0)
	if aux.Cksum != expectedCksum {
		return fmt.Errorf("checksum %v mismatches, expected %v", aux.Cksum, expectedCksum)
	}
	return nil
}

// Marshals bucketMD into JSON, calculates JSON checksum.
func (m *bucketMD) MarshalJSON() ([]byte, error) {
	payload := cmn.MustMarshal(m.BMD)
	cksum := xxhash.Checksum64S(payload, 0)

	return cmn.MustMarshal(&struct {
		BMD   cluster.BMD `json:"bmd"`
		Cksum uint64      `json:"cksum,string"`
	}{
		BMD:   m.BMD,
		Cksum: cksum,
	}), nil
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
	err := cmn.LocalLoad(bo.fpath, bmd, true /*compression*/)
	if err != nil && !os.IsNotExist(err) {
		glog.Errorf("failed to load %s from %s, err: %v", bmdTermName, bo.fpath, err)
	}
	bo._put(bmd)
}

func (bo *bmdOwnerPrx) put(bmd *bucketMD) {
	bo._put(bmd)
	err := cmn.LocalSave(bo.fpath, bmd, true /*compression*/)
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
			err := cmn.LocalLoad(fpath, bmd, true /*compression*/)
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
		if err := cmn.LocalSave(fpath, bmd, true /*compression*/); err != nil {
			glog.Errorf("failed to store %s as %s, err: %v", bmdTermName, fpath, err)
			continue
		}
		cnt++
		if _, ok := curr[mpath]; ok {
			delete(curr, mpath)
		}
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
		if _, ok := prev[mpath]; ok {
			delete(prev, mpath)
		}
	}
	// remove remaining older
	for mpath := range prev {
		fpath := filepath.Join(mpath, bmdFname) + bmdFext
		if err := os.Remove(fpath); err != nil {
			glog.Errorf("failed to remove %s prev version, err: %v", bmdTermName, err)
		}
	}
}
