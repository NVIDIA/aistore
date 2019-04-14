// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

// EC module provides data protection on a per bucket basis. By default, the
// data protection is off. To enable it, set the bucket EC configuration:
//	ECConf:
//		Enable: true|false    # enables or disables protection
//		DataSlices: [2-32]    # the number of data slices
//		ParitySlices: [2-32]  # the number of parity slices
//		ObjSizeLimit: 0       # replication versus erasure coding
//
// NOTE: replicating small object is cheaper than erasure encoding.
// The ObjSizeLimit option sets the corresponding threshold. Set it to the
// size (in bytes), or 0 (zero) to use the AIStore default 256KiB.
//
// NOTE: ParitySlices defines the maximum number of storage targets a cluster
// can loose but it is still able to restore the original object
//
// NOTE: Since small objects are always replicated, they always have only one
// data slice and #ParitySlices replicas
//
// NOTE: All slices and replicas must be on the different targets. The target
// list is calculated by HrwTargetList. The first target in the list is the
// "main" target that keeps the full object, the others keep only slices/replicas
//
// NOTE: All slices must be of the same size. So, the last slice can be padded
// with zeros. In most cases, padding results in the total size of data
// replicas being a bit bigger than than the size of the original object.
//
// NOTE: Every slice and replica must have corresponding metadata file that is
// located in the same mountpath as its slice/replica
//
//
// EC local storage directories inside mountpaths:
//		/obj/  - for main object and its replicas
//		/ec/   - for object data and parity slices
//		/meta/ - for metadata files
//
//
// Metadata content:
//		size - size of the original object (required for correct restoration)
//		data - the number of data slices (unused if the object was replicated)
//		parity - the number of parity slices
//		copy - whether the object was replicated or erasure encoded
//		chk - original object checksum (used to choose the correct slices when
//			restoring the object, sort of versioning)
//		sliceid - used if the object was encoded, the ordinal number of slice
//			starting from 1 (0 means 'full copy' - either orignal object or
//			its replica)
//
//
// How protection works.
//
// Object PUT:
// 1. The main target - the target that is responsible for keeping the full object
//	  data and for restoring the object in case of it is damaged - is selected by
//	  HrwTarget. A proxy delegates object PUT request to it.
// 2. The main target calculates all other targets to keep slices/replicas. For
//	  small files it is #ParitySlices, for big ones it #DataSlices+#ParitySlices
//	  targets.
// 3. If the object is small, the main target broadcast the replicas.
//    Otherwise, the target calculates data and parity slices, then sends them.
//
//Object GET:
// 1. The main target - the target that is responsible for keeping the full object
//	  data and for restoring the object becomes damaged - is determined by
//	  HrwTarget algorithm. A proxy delegates object GET request to it.
// 2. If the main target has the original object, it sends the data back
//    Otherwise it tries to look up it inside other mountpaths(if local rebalance
//	  is running) or on remote targets(if global rebalance is running).
// 3. If everything fails and EC is enabled for the bucket, the main target
//	  initiates object restoration process:
//    - First, the main target requests for object's metafile from all targets
//	    in the cluster. If no target responds with a valid metafile, the object
//		is considered missing.
//    - Otherwise, the main target tries to download and restore the original data:
//      Replica case:
//	        The main target request targets which have valid metafile for a replica
//			one by one. When a target sends a valid object, the main target saves
//			the object to local storage and reuploads its replicas to the targets.
//      EC case:
//			The main target requests targets which have valid metafile for slices
//			in parallel. When all the targets respond, the main target starts
//			restoring the object, and, in case of success, saves the restored object
//			to local storage and sends recalculated data and parity slices to the
//			targets which must have a slice but are 'empty' at this moment.
// NOTE: the slices are stored on targets in random order, except the first
//	     PUT when the main target stores the slices in the order of HrwTargetList
//		 algorithm returns.

const (
	SliceType = "ec"   // object slice prefix
	MetaType  = "meta" // metafile prefix

	DefaultSizeLimit = 256 * cmn.KiB // default minimal object size for EC

	ActSplit   = "split"
	ActRestore = "restore"
	ActDelete  = "delete"

	RespStreamName = "ec-resp"
	ReqStreamName  = "ec-req"

	ActClearRequests  = "clear-requests"
	ActEnableRequests = "enable-requests"

	// EC switches to disk from SGL when memory pressure is high and the amount of
	// memory required to encode an object exceeds the limit
	objSizeHighMem = 50 * cmn.MiB
)

// type of EC request between targets. If the destination has to respond it
// must set the same request type in response header
type intraReqType = int

const (
	// a target sends a replica or slice to store on another target
	// the destionation does not have to respond
	ReqPut intraReqType = iota
	// response for requested slice/replica by another target
	RespPut
	// a target requests a slice or replica from another target
	// if the destination has the object/slice it sends it back, otherwise
	//    it sets Exists=false in response header
	reqGet
	// a target cleans up the object and notifies all other targets to do
	// cleanup as well. Destinations do not have to respond
	reqDel
	// a target requests a metadata of an object
	// if the destination has the object/slice it sends it back, otherwise
	//    it sets Exists=false in response header
	ReqMeta
)

type (
	// Metadata - EC information stored in metafiles for every encoded object
	Metadata struct {
		Size        int64  `json:"size"`              // size of original file (after EC'ing the total size of slices differs from original)
		Data        int    `json:"data"`              // the number of data slices
		Parity      int    `json:"parity"`            // the number of parity slices
		SliceID     int    `json:"sliceid,omitempty"` // 0 for full replica, 1 to N for slices
		ObjChecksum string `json:"obj_chk"`           // checksum of the original object
		IsCopy      bool   `json:"copy"`              // object is replicated(true) or encoded(false)
	}

	// request - structure to request an object to be EC'ed or restored
	Request struct {
		LOM    *cluster.LOM // object info
		Action string       // what to do with the object (see Act* consts)
		ErrCh  chan error   // for final EC result
		IsCopy bool         // replicate or use erasure coding

		// private properties
		putTime time.Time // time when the object is put into main queue
		tm      time.Time // to measure different steps
	}

	RequestsControlMsg struct {
		Action string
	}
)

type (
	// An EC request sent via transport using Opaque field of transport.Header
	// between targets inside a cluster
	IntraReq struct {
		// request type
		Act intraReqType `json:"act"`
		// Sender's daemonID, used by the destination to send the response
		// to the correct target
		Sender string `json:"sender"`
		// object metadata, used when a target copies replicas/slices after
		// encoding or restoring the object data
		Meta *Metadata `json:"meta"`
		// used only by destination to answer to the sender if the destination
		// has the requested metafile or replica/slice
		Exists bool `json:"exists"`
		// the sent data is slice or full replica
		IsSlice bool `json:"slice,omitempty"`
	}

	// keeps temporarily a slice of object data until it is sent to remote node
	slice struct {
		obj     cmn.ReadOpenCloser // the whole object or its replica
		reader  cmn.ReadOpenCloser // used in encoding - a slice of `obj`
		writer  io.Writer          // for parity slices and downloading slices from other targets when restoring
		wg      *cmn.TimeoutGroup  // for synchronous download (for restore)
		lom     *cluster.LOM       // for xattrs
		n       int64              // number of byte sent/received
		refCnt  int32              // number of references
		workFQN string             // FQN for temporary slice/replica
		cksum   cmn.Cksummer       // checksum of the slice
	}

	// a source for data response: the data to send to the caller
	// If obj is not nil then after the reader is sent to the remote target,
	// the obj's counter is decreased. And if its value drops to zero the
	// allocated SGL is freed. This logic is required to send a set of
	// sliceReaders that point to the same SGL (broadcasting data slices)
	dataSource struct {
		reader   cmn.ReadOpenCloser // a reader to sent to a remote target
		size     int64              // size of the data
		obj      *slice             // internal info about SGL slice
		metadata *Metadata          // object's metadata
		isSlice  bool               // is it slice or replica
		reqType  intraReqType       // request's type, slice/meta request/response
	}
)

// frees all allocated memory and removes slice's temporary file
func (s *slice) free() {
	freeObject(s.obj)
	s.obj = nil
	if s.reader != nil {
		s.reader.Close()
	}
	if s.workFQN != "" {
		os.RemoveAll(s.workFQN)
	}
}

// decreases the number of links to the object (the initial number is set
// at slice creation time). If the number drops to zero the allocated
// memory/temporary file is cleaned up
func (s *slice) release() {
	if s.obj != nil || s.workFQN != "" {
		refCnt := atomic.AddInt32(&s.refCnt, -1)
		if refCnt < 1 {
			s.free()
		}
	}
}

func (r *IntraReq) Marshal() ([]byte, error) {
	return jsoniter.Marshal(r)
}

func (r *IntraReq) Unmarshal(b []byte) error {
	return jsoniter.Unmarshal(b, r)
}

func (m *Metadata) marshal() ([]byte, error) {
	return jsoniter.Marshal(m)
}

//nolint:unused
func (m *Metadata) unmarshal(b []byte) error {
	return jsoniter.Unmarshal(b, m)
}

var (
	mem2         = &memsys.Mem2{Name: "ec", MinPctFree: 10}
	slicePadding = make([]byte, 64) // for padding EC slices

	ErrorECDisabled          = errors.New("EC is disabled for bucket")
	ErrorNoMetafile          = errors.New("no metafile")
	ErrorNotFound            = errors.New("not found")
	ErrorInsufficientTargets = errors.New("insufficient targets")
)

func Init() {
	if err := mem2.Init(true); err != nil {
		glog.Fatalf("Failed to initialize EC: %v", err)
	}
	fs.CSM.RegisterFileType(SliceType, &SliceSpec{})
	fs.CSM.RegisterFileType(MetaType, &MetaSpec{})
	go mem2.Run()
}

// SliceSize returns the size of one slice that EC will create for the object
func SliceSize(fileSize int64, slices int) int64 {
	return (fileSize + int64(slices) - 1) / int64(slices)
}

// Monitoring the background transferring of replicas and slices requires
// a unique ID for each of them. Because of all replicas/slices of an object have
// the same names, cluster.Uname is not enough to generate unique ID. Adding an
// extra prefix - an identifier of the destination - solves the issue
func unique(prefix, bucket, objname string) string {
	return prefix + "/" + cluster.Uname(bucket, objname)
}

// Reads local file to SGL
// Used by a target when responding to request for metafile/replica/slice
func readFile(lom *cluster.LOM) (sgl *memsys.SGL, err error) {
	f, err := os.Open(lom.FQN)
	if err != nil {
		return nil, err
	}

	sgl = mem2.NewSGL(lom.Size())
	buf, slab := mem2.AllocFromSlab2(cmn.KiB * 32)
	_, err = io.CopyBuffer(sgl, f, buf)
	f.Close()
	slab.Free(buf)

	if err != nil {
		sgl.Free()
		return nil, err
	}

	return sgl, nil
}

func IsECCopy(size int64, ecConf *cmn.ECConf) bool {
	return size < ecConf.ObjSizeLimit || (ecConf.ObjSizeLimit == 0 && size < DefaultSizeLimit)
}

// returns whether EC must use disk instead of keeping everything in memory.
// Depends on available free memory and size of an object to process
func useDisk(objSize int64) bool {
	switch mem2.MemPressure() {
	case memsys.OOM, memsys.MemPressureExtreme:
		return true
	case memsys.MemPressureHigh:
		return objSize > objSizeHighMem
	default:
		return false
	}
}

// Frees allocated memory if it is SGL or closes the file handle in case of regular file
func freeObject(r interface{}) {
	if r == nil {
		return
	}
	if sgl, ok := r.(*memsys.SGL); ok {
		sgl.Free()
		return
	}
	if f, ok := r.(*cmn.FileHandle); ok {
		f.Close()
		return
	}
	cmn.AssertFmt(false, "Invalid object type", r)
}

// removes all temporary slices in case of erasure coding fails in the middle
func freeSlices(slices []*slice) {
	for _, s := range slices {
		if s != nil {
			s.free()
		}
	}
}
