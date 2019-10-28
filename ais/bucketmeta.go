// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
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
// - bucketMD (instance) can be obtained via bmdowner.get()
// - bucketMD is immutable and versioned
// - bucketMD versioning is monotonic and incremental
//
// - bucketMD typical update transaction:
// lock -- clone() -- modify the clone -- bmdowner.put(clone) -- unlock
//
// (*) for merges and conflict resolution, check the current version prior to put()
//     (note that version check must be protected by the same critical section)
//

const bmdTermName = "BMD"

type bucketMD struct {
	cluster.BMD
	vstr string // itoa(Version), to have it handy for http redirects
}

var (
	_ json.Marshaler   = &bucketMD{}
	_ json.Unmarshaler = &bucketMD{}
)

// c-tor
func newBucketMD() *bucketMD {
	lbmap := make(map[string]*cmn.BucketProps)
	cbmap := make(map[string]*cmn.BucketProps)
	return &bucketMD{BMD: cluster.BMD{LBmap: lbmap, CBmap: cbmap}}
}

func (m *bucketMD) add(bck *cluster.Bck, p *cmn.BucketProps) bool {
	cmn.Assert(p != nil)
	mm := m.LBmap
	if !bck.IsAIS() {
		mm = m.CBmap
	}
	if _, exists := mm[bck.Name]; exists {
		return false
	}

	m.Version++
	p.BID = m.GenBucketID(bck.IsAIS())
	mm[bck.Name] = p
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
	if _, ok := mm[bck.Name]; !ok {
		cmn.Assert(false)
	}
	cmn.Assert(!p.InProgress)

	m.Version++
	p.BID = m.GenBucketID(bck.IsAIS())
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

func (m *bucketMD) ecUsed() bool {
	for _, bck := range m.LBmap {
		if bck.EC.Enabled {
			return true
		}
	}
	for _, bck := range m.CBmap {
		if bck.EC.Enabled {
			return true
		}
	}

	return false
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
func (m *bucketMD) tag() string    { return bucketmdtag }
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

// Selects a mountpath with highest weight and reads the bmd from the file.
func (m *bucketMD) LoadFromFS() error {
	mpath, err := fs.Mountpaths.MpathForMetadata()
	if err != nil {
		return err
	}
	bmdFullPath := filepath.Join(mpath.Path, cmn.BucketmdBackupFile)
	return cmn.LocalLoad(bmdFullPath, m)
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

//=====================================================================
//
// bmdowner: implements cluster.Bowner interface
//
//=====================================================================
var _ cluster.Bowner = &bmdowner{}

type bmdowner struct {
	sync.Mutex
	node     string
	bucketmd atomic.Pointer
}

func newBmdowner(node string) *bmdowner {
	return &bmdowner{
		node: node,
	}
}

func (r *bmdowner) _put(bucketmd *bucketMD) {
	bucketmd.vstr = strconv.FormatInt(bucketmd.Version, 10)
	r.bucketmd.Store(unsafe.Pointer(bucketmd))
}

func (r *bmdowner) init() {
	bmd := newBucketMD()
	switch r.node {
	case cmn.Target:
		break
	case cmn.Proxy:
		bmdFullPath := filepath.Join(cmn.GCO.Get().Confdir, cmn.BucketmdBackupFile)
		if err := cmn.LocalLoad(bmdFullPath, bmd); err != nil {
			// Create empty
			bmd.Version = 1
			if err := cmn.LocalSave(bmdFullPath, bmd); err != nil {
				glog.Fatalf("FATAL: cannot store %s, err: %v", bmdTermName, err)
			}
		}
	default:
		cmn.AssertMsg(false, r.node)
	}

	r._put(bmd)
}

func (r *bmdowner) put(bmd *bucketMD) {
	r._put(bmd)

	var (
		bmdFullPath string
		err         error
	)
	switch r.node {
	case cmn.Target:
		var mpath *fs.MountpathInfo
		mpath, err = fs.Mountpaths.MpathForMetadata()
		if err != nil {
			break
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Saving %s v%d copy to %s of %s", bmdTermName, bmd.Version, cmn.XattrBMD, mpath.Path)
		}
		bmdFullPath = filepath.Join(mpath.Path, cmn.BucketmdBackupFile)
		err = cmn.LocalSave(bmdFullPath, bmd)
	case cmn.Proxy:
		bmdFullPath = filepath.Join(cmn.GCO.Get().Confdir, cmn.BucketmdBackupFile)
		err = cmn.LocalSave(bmdFullPath, bmd)
	default:
		cmn.AssertMsg(false, r.node)
	}

	if err != nil {
		glog.Errorf("failed to store %s at %s, err: %v", bmdTermName, bmdFullPath, err)
	}
}

// implements cluster.Bowner.Get
func (r *bmdowner) Get() *cluster.BMD {
	return &r.get().BMD
}
func (r *bmdowner) get() (bucketmd *bucketMD) {
	return (*bucketMD)(r.bucketmd.Load())
}
