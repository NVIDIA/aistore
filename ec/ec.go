// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// EC module provides data protection on a per bucket basis. By default, the
// data protection is off. To enable it, set the bucket EC configuration:
//	ECConf:
//		Enable: true|false    # enables or disables protection
//		DataSlices: [1-32]    # the number of data slices
//		ParitySlices: [1-32]  # the number of parity slices
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
//		/%ob/ - for main object and its replicas
//		/%ec/ - for object data and parity slices
//		/%mt/ - for metadata files
//
// How protection works.
//
// Object PUT:
// 1. The main target - the target responsible for keeping the full object
//	  data and for restoring the object if damaged - is selected by
//	  HrwTarget. A proxy delegates object PUT request to it.
// 2. The main target calculates all other targets to keep slices/replicas. For
//	  small files it is #ParitySlices, for big ones it #DataSlices+#ParitySlices
//	  targets.
// 3. If the object is small, the main target broadcast the replicas.
//    Otherwise, the target calculates data and parity slices, then sends them.
//
// Object GET:
// 1. The main target - the target that is responsible for keeping the full object
//	  data and for restoring the object becomes damaged - is determined by
//	  HrwTarget algorithm. A proxy delegates object GET request to it.
// 2. If the main target has the original object, it sends the data back
//    Otherwise it tries to look up it inside other mountpaths (if resilver
//	  is running) or on remote targets (if rebalance is running).
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
	ActSplit   = "split"
	ActRestore = "restore"
	ActDelete  = "delete"

	RespStreamName = "ec-resp"
	ReqStreamName  = "ec-req"

	ActClearRequests  = "clear-requests"
	ActEnableRequests = "enable-requests"

	URLCT   = "ct"   // for using in URL path - requests for slices/replicas
	URLMeta = "meta" /// .. - metadata requests

	// EC switches to disk from SGL when memory pressure is high and the amount of
	// memory required to encode an object exceeds the limit
	objSizeHighMem = 50 * cos.MiB
)

type (
	// request - structure to request an object to be EC'ed or restored
	request struct {
		LIF      cluster.LIF // object info
		Action   string      // what to do with the object (see Act* consts)
		ErrCh    chan error  // for final EC result (used only in restore)
		Callback cluster.OnFinishObj

		putTime time.Time // time when the object is put into main queue
		tm      time.Time // to measure different steps
		IsCopy  bool      // replicate or use erasure coding
		rebuild bool      // true - internal request to reencode, e.g., from ec-encode xaction
	}

	RequestsControlMsg struct {
		Action string
	}

	WriteArgs struct {
		MD         []byte     // CT's metafile content
		Reader     io.Reader  // CT content
		BID        uint64     // bucket ID
		Cksum      *cos.Cksum // object checksum
		Generation int64      // EC Generation
	}

	// keeps temporarily a slice of object data until it is sent to remote node
	slice struct {
		obj     cos.ReadOpenCloser // the whole object or its replica
		reader  cos.ReadOpenCloser // used in encoding - a slice of `obj`
		writer  io.Writer          // for parity slices and downloading slices from other targets when restoring
		wg      *cos.TimeoutGroup  // for synchronous download (for restore)
		lom     *cluster.LOM       // for xattrs
		n       int64              // number of byte sent/received
		refCnt  atomic.Int32       // number of references
		workFQN string             // FQN for temporary slice/replica
		cksum   *cos.Cksum         // checksum of the slice
		version string             // version of the remote object
	}

	// a source for data response: the data to send to the caller
	// If obj is not nil then after the reader is sent to the remote target,
	// the obj's counter is decreased. And if its value drops to zero the
	// allocated SGL is freed. This logic is required to send a set of
	// sliceReaders that point to the same SGL (broadcasting data slices)
	dataSource struct {
		reader   cos.ReadOpenCloser // a reader to sent to a remote target
		size     int64              // size of the data
		obj      *slice             // internal info about SGL slice
		metadata *Metadata          // object's metadata
		isSlice  bool               // is it slice or replica
		reqType  intraReqType       // request's type, slice/meta request/response
	}
)

var (
	emptyReq request
	reqPool  sync.Pool
	mm       *memsys.MMSA // memory manager and slab/SGL allocator

	ErrorECDisabled = errors.New("EC is disabled for bucket")
	ErrorNoMetafile = errors.New("no metafile")
	ErrorNotFound   = errors.New("not found")
)

func allocateReq(action string, lif cluster.LIF) (req *request) {
	if v := reqPool.Get(); v != nil {
		req = v.(*request)
	} else {
		req = &request{}
	}
	req.Action = action
	req.LIF = lif
	return
}

func freeReq(req *request) {
	*req = emptyReq
	reqPool.Put(req)
}

// Free allocated memory and removes slice's temporary file
func (s *slice) free() {
	freeObject(s.obj)
	s.obj = nil
	if s.reader != nil {
		cos.Close(s.reader)
	}
	if s.writer != nil {
		switch w := s.writer.(type) {
		case *os.File:
			cos.Close(w)
		case *memsys.SGL:
			w.Free()
		default:
			cos.Assertf(false, "%T", w)
		}
	}
	if s.workFQN != "" {
		errRm := os.RemoveAll(s.workFQN)
		debug.AssertNoErr(errRm)
	}
}

// Decrease the number of links to the object (the initial number is set
// at slice creation time). If the number drops to zero the allocated
// memory/temporary file is cleaned up
func (s *slice) release() {
	if s.obj != nil || s.workFQN != "" {
		refCnt := s.refCnt.Dec()
		if refCnt < 1 {
			s.free()
		}
	}
}

func (s *slice) reopenReader() (reader cos.ReadOpenCloser, err error) {
	if s.reader != nil {
		var rc io.ReadCloser
		reader = s.reader
		switch r := reader.(type) {
		case *memsys.Reader:
			_, err = r.Seek(0, io.SeekStart)
		case *cos.SectionHandle:
			rc, err = r.Open()
			if err == nil {
				reader = rc.(cos.ReadOpenCloser)
			}
		default:
			err = fmt.Errorf("unsupported reader type: %T", s.reader)
			debug.Assertf(false, "unsupported reader type: %T", s.reader)
		}
		return reader, err
	}

	if sgl, ok := s.obj.(*memsys.SGL); ok {
		reader = memsys.NewReader(sgl)
	} else if s.workFQN != "" {
		reader, err = cos.NewFileHandle(s.workFQN)
	} else {
		err = fmt.Errorf("unsupported obj type: %T", s.obj)
		debug.Assertf(false, "unsupported obj type: %T", s.obj)
	}
	return reader, err
}

func Init(t cluster.Target) {
	mm = t.PageMM()

	fs.CSM.Reg(fs.ECSliceType, &fs.ECSliceContentResolver{})
	fs.CSM.Reg(fs.ECMetaType, &fs.ECMetaContentResolver{})

	xreg.RegBckXact(&getFactory{})
	xreg.RegBckXact(&putFactory{})
	xreg.RegBckXact(&rspFactory{})
	xreg.RegBckXact(&encFactory{})

	if err := initManager(t); err != nil {
		cos.ExitLogf("Failed to init manager: %v", err)
	}
}

// SliceSize returns the size of one slice that EC will create for the object
func SliceSize(fileSize int64, slices int) int64 {
	return (fileSize + int64(slices) - 1) / int64(slices)
}

// Monitoring the background transferring of replicas and slices requires
// a unique ID for each of them. Because of all replicas/slices of an object have
// the same names, cluster.Uname is not enough to generate unique ID. Adding an
// extra prefix - an identifier of the destination - solves the issue
func unique(prefix string, bck *cluster.Bck, objName string) string {
	return prefix + string(filepath.Separator) + bck.MakeUname(objName)
}

func IsECCopy(size int64, ecConf *cmn.ECConf) bool {
	return size < ecConf.ObjSizeLimit
}

// returns whether EC must use disk instead of keeping everything in memory.
// Depends on available free memory and size of an object to process
func useDisk(objSize int64) bool {
	if cmn.GCO.Get().EC.DiskOnly {
		return true
	}
	memPressure := mm.Pressure()
	switch memPressure {
	case memsys.OOM, memsys.PressureExtreme:
		return true
	case memsys.PressureHigh:
		return objSize > objSizeHighMem
	default:
		return false
	}
}

// Frees allocated memory if it is SGL or closes the file handle if regular file
func freeObject(r interface{}) {
	if r == nil {
		return
	}
	switch handle := r.(type) {
	case *memsys.SGL:
		if handle != nil {
			handle.Free()
		}
	case *cos.FileHandle:
		if handle != nil {
			// few slices share the same handle, on error all release everything
			_ = handle.Close()
		}
	case *os.File:
		if handle != nil {
			cos.Close(handle)
		}
	default:
		debug.Assertf(false, "invalid object type: %T", r)
	}
}

// removes all temporary slices in case of erasure coding failure
func freeSlices(slices []*slice) {
	for _, s := range slices {
		if s != nil {
			s.free()
		}
	}
}

// RequestECMeta returns an EC metadata found on a remote target.
func RequestECMeta(bck cmn.Bck, objName string, si *cluster.Snode, client *http.Client) (md *Metadata, err error) {
	path := cmn.URLPathEC.Join(URLMeta, bck.Name, objName)
	query := url.Values{}
	query = cmn.AddBckToQuery(query, bck)
	url := si.URL(cmn.NetworkIntraData) + path
	rq, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	rq.URL.RawQuery = query.Encode()
	resp, err := client.Do(rq) // nolint:bodyclose // closed inside cos.Close
	if err != nil {
		return nil, err
	}
	defer cos.Close(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, cmn.NewErrNotFound("object %s/%s", bck, objName)
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to read %s GET request: %v", objName, err)
	}
	return MetaFromReader(resp.Body)
}

// Saves the main replica to local drives
func writeObject(t cluster.Target, lom *cluster.LOM, reader io.Reader, size int64) error {
	if size > 0 {
		reader = io.LimitReader(reader, size)
	}
	readCloser := io.NopCloser(reader)
	bdir := lom.MpathInfo().MakePathBck(lom.Bucket())
	if err := fs.Access(bdir); err != nil {
		return err
	}
	params := cluster.PutObjectParams{
		Tag:        "ec",
		Reader:     readCloser,
		OWT:        cmn.OwtMigrate, // to avoid changing version
		SkipEncode: true,
		Atime:      time.Now(),
	}
	return t.PutObject(lom, params)
}

func validateBckBID(t cluster.Target, bck cmn.Bck, bid uint64) error {
	if bid == 0 {
		return nil
	}
	newBck := cluster.NewBckEmbed(bck)
	err := newBck.Init(t.Bowner())
	if err == nil && newBck.Props.BID != bid {
		err = fmt.Errorf("bucket ID mismatch: local %d, sender %d", newBck.Props.BID, bid)
	}
	return err
}

// WriteSliceAndMeta saves slice and its metafile
func WriteSliceAndMeta(t cluster.Target, hdr *transport.ObjHdr, args *WriteArgs) error {
	ct, err := cluster.NewCTFromBO(hdr.Bck, hdr.ObjName, t.Bowner(), fs.ECSliceType)
	if err != nil {
		return err
	}
	ct.Lock(true)
	ctMeta := ct.Clone(fs.ECMetaType)
	defer func() {
		ct.Unlock(true)
		if err == nil {
			return
		}
		if rmErr := cos.RemoveFile(ct.FQN()); rmErr != nil {
			glog.Errorf("nested error: save replica -> remove replica: %v", rmErr)
		}
		if rmErr := cos.RemoveFile(ctMeta.FQN()); rmErr != nil {
			glog.Errorf("nested error: save replica -> remove metafile: %v", rmErr)
		}
	}()
	if args.Generation != 0 {
		if oldMeta, oldErr := LoadMetadata(ctMeta.FQN()); oldErr == nil && oldMeta.Generation > args.Generation {
			cos.DrainReader(args.Reader)
			return nil
		}
	}
	tmpFQN := ct.Make(fs.WorkfileType)
	if err := ct.Write(t, args.Reader, hdr.ObjAttrs.Size, tmpFQN); err != nil {
		return err
	}
	if err := ctMeta.Write(t, bytes.NewReader(args.MD), -1); err != nil {
		return err
	}
	if _, exists := t.Bowner().Get().Get(ctMeta.Bck()); !exists {
		err = fmt.Errorf("%s metafile saved while bucket %s was being destroyed", ctMeta.ObjectName(), ctMeta.Bucket())
		return err
	}
	err = validateBckBID(t, hdr.Bck, args.BID)
	return err
}

// WriteReplicaAndMeta saves replica and its metafile
func WriteReplicaAndMeta(t cluster.Target, lom *cluster.LOM, args *WriteArgs) (err error) {
	lom.Lock(false)
	if args.Generation != 0 {
		ctMeta := cluster.NewCTFromLOM(lom, fs.ECMetaType)
		if oldMeta, oldErr := LoadMetadata(ctMeta.FQN()); oldErr == nil && oldMeta.Generation > args.Generation {
			lom.Unlock(false)
			cos.DrainReader(args.Reader)
			return nil
		}
	}
	lom.Unlock(false)
	if err = writeObject(t, lom, args.Reader, lom.SizeBytes(true)); err != nil {
		return
	}
	if !args.Cksum.IsEmpty() && args.Cksum.Value() != "" { // NOTE: empty value
		if !lom.EqCksum(args.Cksum) {
			return fmt.Errorf("mismatched hash for %s/%s, version %s, hash calculated %s/md %s",
				lom.Bucket(), lom.ObjName, lom.Version(), args.Cksum, lom.Checksum())
		}
	}
	ctMeta := cluster.NewCTFromLOM(lom, fs.ECMetaType)
	ctMeta.Lock(true)
	defer func() {
		ctMeta.Unlock(true)
		if err == nil {
			return
		}
		if rmErr := cos.RemoveFile(lom.FQN); rmErr != nil {
			glog.Errorf("nested error: save replica -> remove replica: %v", rmErr)
		}
		if rmErr := cos.RemoveFile(ctMeta.FQN()); rmErr != nil {
			glog.Errorf("nested error: save replica -> remove metafile: %v", rmErr)
		}
	}()
	if err = ctMeta.Write(t, bytes.NewReader(args.MD), -1); err != nil {
		return
	}
	if _, exists := t.Bowner().Get().Get(ctMeta.Bck()); !exists {
		err = fmt.Errorf("%s metafile saved while bucket %s was being destroyed", ctMeta.ObjectName(), ctMeta.Bucket())
		return
	}
	err = validateBckBID(t, lom.Bucket(), args.BID)
	return
}
