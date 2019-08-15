// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/binary"
	"fmt"
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
	vstr      string // itoa(Version), to have it handy for http redirects
	renamedLB cmn.SimpleKVs
}

// c-tor
func newBucketMD() *bucketMD {
	lbmap := make(map[string]*cmn.BucketProps)
	cbmap := make(map[string]*cmn.BucketProps)
	return &bucketMD{BMD: cluster.BMD{LBmap: lbmap, CBmap: cbmap}}
}

func (m *bucketMD) add(b string, local bool, p *cmn.BucketProps) bool {
	cmn.Assert(p != nil)
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; ok {
		return false
	}
	m.Version++
	p.BID = m.GenBucketID(local)
	mm[b] = p
	return true
}

func (m *bucketMD) del(b string, local bool) bool {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; !ok {
		return false
	}
	delete(mm, b)
	m.Version++
	return true
}

func (m *bucketMD) set(b string, local bool, p *cmn.BucketProps) {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; !ok {
		cmn.Assert(false)
	}

	m.Version++
	mm[b] = p
}

func (m *bucketMD) ecUsed() bool {
	for _, bck := range m.LBmap {
		if bck.EC.Enabled {
			return true
		}
	}

	return false
}

// ecEnabled returns whether or not erasure coding is enabled
// for the bucket. Returns false if bucket not found
//nolint:unused
func (m *bucketMD) ecEnabled(bucket string) bool {
	p, ok := m.Get(bucket, m.IsLocal(bucket))
	return ok && p.EC.Enabled
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
	dst.renamedLB = m.renamedLB
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
// Checksum is the first line, the other lines are JSON payload
// that represents marshaled bucketMD structure
func (m *bucketMD) UnmarshalXattr(b []byte) error {
	const emptyPayloadLen = len("\n{}")
	cmn.AssertMsg(len(b) >= cmn.SizeofI64+emptyPayloadLen, "Incomplete bucketMD payload")

	expectedCksm, mdJSON := binary.BigEndian.Uint64(b[:cmn.SizeofI64]), b[cmn.SizeofI64+1:]
	actualCksum := xxhash.Checksum64S(mdJSON, 0)
	if actualCksum != expectedCksm {
		return fmt.Errorf("checksum %v mismatches, expected %v", actualCksum, expectedCksm)
	}
	err := jsoniter.Unmarshal(mdJSON, m)
	if err == nil && glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Restored BMD copy version %d", m.Version)
	}
	return err
}

// Marshals bucketMD into JSON, calculates JSON checksum and generates
// a payload as [checksum] + "\n" + JSON
func (m *bucketMD) MarshalXattr() []byte {
	payload := cmn.MustMarshal(m)
	cksum := xxhash.Checksum64S(payload, 0)

	bufLen := cmn.SizeofI64 + 1 + len(payload)
	body := make([]byte, bufLen)
	binary.BigEndian.PutUint64(body, cksum)
	body[cmn.SizeofI64] = '\n'
	copy(body[cmn.SizeofI64+1:], payload)

	return body
}

// Selects a mountpath with highest weight and reads xattr of the
// directory where mountpath is mounted
func (m *bucketMD) LoadFromFS() error {
	slab, err := nodeCtx.mm.GetSlab2(maxBMDXattrSize)
	if err != nil {
		return err
	}
	buf := slab.Alloc()
	defer slab.Free(buf)

	mpath, err := fs.Mountpaths.MpathForXattr()
	if err != nil {
		return err
	}
	b, err := fs.GetXattrBuf(mpath.Path, cmn.XattrBMD, buf)
	if err != nil {
		return fmt.Errorf("%s: %v", mpath, err)
	}
	if len(b) > 0 {
		return m.UnmarshalXattr(b)
	}
	return nil
}

// Selects a mountpath with highest weight and saves BMD to its xattr
func (m *bucketMD) Persist() error {
	mpath, err := fs.Mountpaths.MpathForXattr()
	if err != nil {
		return err
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Saving %s v%d copy to %s of %s", bmdTermName, m.Version, cmn.XattrBMD, mpath.Path)
	}
	b := m.MarshalXattr()
	err = fs.SetXattr(mpath.Path, cmn.XattrBMD, b)
	if err != nil {
		return fmt.Errorf("failed to save xattr to %q: %v", mpath.Path, err)
	}
	return nil
}

func (m *bucketMD) Dump() string {
	s := fmt.Sprintf("BMD Version %d\nLocal buckets: [", m.Version)
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
	bucketmd atomic.Pointer
}

func newBmdowner() *bmdowner {
	return &bmdowner{}
}

func (r *bmdowner) put(bucketmd *bucketMD) {
	bucketmd.vstr = strconv.FormatInt(bucketmd.Version, 10)
	r.bucketmd.Store(unsafe.Pointer(bucketmd))
}

// implements cluster.Bowner.Get
func (r *bmdowner) Get() *cluster.BMD {
	return &r.get().BMD
}
func (r *bmdowner) get() (bucketmd *bucketMD) {
	return (*bucketMD)(r.bucketmd.Load())
}
