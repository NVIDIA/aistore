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
	lbmap := make(map[string]*cmn.BucketProps)
	cbmap := make(map[string]*cmn.BucketProps)
	return &bucketMD{BMD: cluster.BMD{LBmap: lbmap, CBmap: cbmap, Origin: 0}} // only proxy can generate
}

func newOriginMD() (origin uint64, created string) {
	var (
		now    = time.Now()
		o, err = cmn.GenUUID64()
	)
	if err == nil {
		origin = uint64(o)
	} else {
		glog.Error(err)
		origin = uint64(now.UnixNano())
	}
	created = now.String()
	return
}

//////////////
// bucketMD //
//////////////

func (m *bucketMD) add(bck *cluster.Bck, p *cmn.BucketProps) bool {
	cmn.Assert(p != nil)
	mm := m.LBmap
	if !bck.IsAIS() {
		mm = m.CBmap
	}
	p.CloudProvider = bck.Provider
	cmn.AssertMsg(cmn.IsValidProvider(p.CloudProvider), p.CloudProvider)

	if _, exists := mm[bck.Name]; exists {
		return false
	}

	m.Version++
	p.BID = m.GenBucketID(bck.IsAIS())
	mm[bck.Name] = p
	bck.Props = p
	return true
}

func (m *bucketMD) del(bck *cluster.Bck) bool {
	mm := m.LBmap
	if !bck.IsAIS() {
		mm = m.CBmap
	}
	if _, ok := mm[bck.Name]; !ok {
		return false
	}
	m.Version++
	delete(mm, bck.Name)
	return true
}

func (m *bucketMD) set(bck *cluster.Bck, p *cmn.BucketProps) {
	mm := m.LBmap
	if !bck.IsAIS() {
		mm = m.CBmap
	}
	p.CloudProvider = bck.Provider
	cmn.AssertMsg(cmn.IsValidProvider(p.CloudProvider), p.CloudProvider)

	prevProps, ok := mm[bck.Name]
	cmn.Assert(ok)
	cmn.Assert(!p.InProgress)

	m.Version++
	p.BID = prevProps.BID // Always use previous BID
	cmn.Assert(p.BID != 0)
	mm[bck.Name] = p
}

func (m *bucketMD) toggleInProgress(bck *cluster.Bck, toggle bool) {
	mm := m.LBmap
	if !bck.IsAIS() {
		mm = m.CBmap
	}
	p, ok := mm[bck.Name]
	cmn.Assert(ok)
	if !toggle {
		cmn.Assert(p.InProgress)
	}

	p.InProgress = toggle
	m.Version++
	mm[bck.Name] = p
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
	cmn.CopyStruct(dst, m)
	dst.LBmap = make(map[string]*cmn.BucketProps, len(m.LBmap))
	dst.CBmap = make(map[string]*cmn.BucketProps, len(m.CBmap))
	inmaps := [2]map[string]*cmn.BucketProps{m.LBmap, m.CBmap}
	outmaps := [2]map[string]*cmn.BucketProps{dst.LBmap, dst.CBmap}
	for i := 0; i < len(inmaps); i++ {
		mm := outmaps[i]
		for name, props := range inmaps[i] {
			p := &cmn.BucketProps{}
			*p = *props
			mm[name] = p
		}
	}
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

func (m *bucketMD) Dump() string {
	s := fmt.Sprintf("BMD Version %d\nais buckets: [", m.Version)
	for name := range m.LBmap {
		s += name + ", "
	}

	s += "]\nCloud buckets: ["
	for name := range m.CBmap {
		s += name + ", "
	}
	s += "]"

	return s
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
