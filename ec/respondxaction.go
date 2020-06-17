// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
)

type (
	// Xaction responsible for responding to EC requests of other targets
	// Should not be stopped if number of known targets is small
	XactRespond struct {
		xactECBase
	}
)

func NewRespondXact(t cluster.Target, smap cluster.Sowner, si *cluster.Snode,
	bck cmn.Bck, reqBundle, respBundle *transport.StreamBundle) *XactRespond {
	XactCount.Inc()
	runner := &XactRespond{
		xactECBase: newXactECBase(t, smap, si, bck, reqBundle, respBundle),
	}

	return runner
}

func (r *XactRespond) Run() (err error) {
	glog.Infoln(r.String())

	var (
		cfg         = cmn.GCO.Get()
		lastAction  = time.Now()
		idleTimeout = 3 * cfg.Timeout.SendFile
		ticker      = time.NewTicker(cfg.Periodic.StatsTime)
	)
	defer ticker.Stop()

	// as of now all requests are equal. Some may get throttling later
	for {
		select {
		case <-ticker.C:
			if s := fmt.Sprintf("%v", r.stats.stats()); s != "" {
				glog.Info(s)
			}
		case <-r.ChanCheckTimeout():
			idleEnds := lastAction.Add(idleTimeout)
			if idleEnds.Before(time.Now()) && r.Timeout() {
				if glog.V(4) {
					glog.Infof("Idle time is over: %v. Last action at: %v",
						time.Now(), lastAction)
				}

				r.stop()
				return nil
			}
		case <-r.ChanAbort():
			r.stop()
			return cmn.NewAbortedError(r.String())
		}
	}
}

// Utility function to cleanup both object/slice and its meta on the local node
// Used when processing object deletion request
func (r *XactRespond) removeObjAndMeta(bck *cluster.Bck, objName string) error {
	if glog.V(4) {
		glog.Infof("Delete request for %s/%s", bck.Name, objName)
	}

	// to be consistent with PUT, object's files are deleted in a reversed
	// order: first Metafile is removed, then Replica/Slice
	// Why: the main object is gone already, so we do not want any target
	// responds that it has the object because it has metafile. We delete
	// metafile that makes remained slices/replicas outdated and can be cleaned
	// up later by LRU or other runner
	for _, tp := range []string{MetaType, fs.ObjectType, SliceType} {
		fqnMeta, _, err := cluster.HrwFQN(bck, tp, objName)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(fqnMeta); err != nil {
			return fmt.Errorf("error removing %s %q: %w", tp, fqnMeta, err)
		}
	}

	return nil
}

// DispatchReq is responsible for handling request from other targets
func (r *XactRespond) DispatchReq(iReq intraReq, bck *cluster.Bck, objName string) {
	daemonID := iReq.sender
	switch iReq.act {
	case reqDel:
		// object cleanup request: delete replicas, slices and metafiles
		if err := r.removeObjAndMeta(bck, objName); err != nil {
			glog.Errorf("Failed to delete %s/%s: %v", bck.Name, objName, err)
		}
	case reqGet:
		// slice or replica request: send the object's data to the caller
		var (
			fqn, metaFQN string
			md           *Metadata
			err          error
		)
		if iReq.isSlice {
			if glog.V(4) {
				glog.Infof("Received request for slice %d of %s", iReq.meta.SliceID, objName)
			}
			fqn, _, err = cluster.HrwFQN(bck, SliceType, objName)
			if err != nil {
				glog.Error(err)
				return
			}
			metaFQN, _, err = cluster.HrwFQN(bck, MetaType, objName)
			if err == nil {
				md, err = LoadMetadata(metaFQN)
			}
		} else {
			if glog.V(4) {
				glog.Infof("Received request for replica %s", objName)
			}
			// FIXME: (redundant) r.dataResponse() does not need it as it constructs
			//        LOM right away
			fqn, _, err = cluster.HrwFQN(bck, fs.ObjectType, objName)
		}
		if err != nil {
			glog.Error(err)
			return
		}

		if err := r.dataResponse(respPut, fqn, bck, objName, daemonID, md); err != nil {
			glog.Errorf("Failed to send back [GET req] %q: %v", fqn, err)
		}
	default:
		// invalid request detected
		glog.Errorf("Invalid request type %d", iReq.act)
	}
}

func (r *XactRespond) writeReplica(bck *cluster.Bck, objName string, objAttrs transport.ObjectAttrs, object io.Reader) (string, error) {
	lom := &cluster.LOM{T: r.t, ObjName: objName}
	err := lom.Init(bck.Bck)
	if err != nil {
		return "", err
	}
	lom.SetVersion(objAttrs.Version)
	lom.SetAtimeUnix(objAttrs.Atime)
	if objAttrs.CksumType != "" {
		lom.SetCksum(cmn.NewCksum(objAttrs.CksumType, objAttrs.CksumValue))
	}
	err = WriteObject(r.t, lom, object, objAttrs.Size, cmn.ChecksumNone)
	return lom.FQN, err
}

func (r *XactRespond) writeSlice(bck *cluster.Bck, objName string, objAttrs transport.ObjectAttrs, object io.Reader) (string, error) {
	buf, slab := mm.Alloc()
	defer slab.Free(buf)
	sliceFQN, _, err := cluster.HrwFQN(bck, SliceType, objName)
	if err != nil {
		return "", err
	}
	tmpFQN := fs.CSM.GenContentFQN(sliceFQN, fs.WorkfileType, "ec")
	ct, err := cluster.NewCTFromFQN(sliceFQN, r.t.GetBowner())
	if err != nil {
		return "", err
	}
	bdir := ct.ParsedFQN().MpathInfo.MakePathBck(bck.Bck)
	_, err = cmn.SaveReaderSafe(tmpFQN, sliceFQN, object, buf, cmn.ChecksumNone, objAttrs.Size, bdir)
	return sliceFQN, err
}

func (r *XactRespond) DispatchResp(iReq intraReq, bck *cluster.Bck, objName string, objAttrs transport.ObjectAttrs, object io.Reader) {
	drain := func() {
		if err := cmn.DrainReader(object); err != nil {
			glog.Warningf("Failed to drain reader %s/%s: %v", bck.Name, objName, err)
		}
	}

	switch iReq.act {
	case reqPut:
		// a remote target sent a replica/slice while it was
		// encoding or restoring an object. In this case it just saves
		// the sent replica or slice to a local file along with its metadata
		// look for metadata in request

		// Check if the request is valid: it must contain metadata
		meta := iReq.meta
		if meta == nil {
			drain()
			glog.Errorf("No metadata in request for %s/%s", bck.Name, objName)
			return
		}

		// Check if the request is valid (e.g, a request may come after
		// the bucket is destroyed.
		var (
			objFQN string
			err    error
		)
		if iReq.isSlice {
			if glog.V(4) {
				glog.Infof("Got slice response from %s (#%d of %s/%s) v%s",
					iReq.sender, iReq.meta.SliceID, bck.Name, objName, meta.ObjVersion)
			}
			objFQN, err = r.writeSlice(bck, objName, objAttrs, object)
		} else {
			if glog.V(4) {
				glog.Infof("Got replica response from %s (%s/%s) v%s", iReq.sender, bck.Name, objName, meta.ObjVersion)
			}
			objFQN, err = r.writeReplica(bck, objName, objAttrs, object)
		}
		if err != nil {
			drain()
			glog.Error(err)
			return
		}

		// save its metadata
		buf, slab := mm.Alloc()
		defer slab.Free(buf)
		metaFQN := fs.CSM.GenContentFQN(objFQN, MetaType, "")
		ct, err := cluster.NewCTFromFQN(metaFQN, r.t.GetBowner())
		if err != nil {
			return
		}
		bdir := ct.ParsedFQN().MpathInfo.MakePathBck(bck.Bck)
		metaBuf := meta.Marshal()
		_, err = cmn.SaveReader(metaFQN, bytes.NewReader(metaBuf), buf, cmn.ChecksumNone, -1, bdir)
		if err != nil {
			glog.Errorf("Failed to save metadata to %q: %v", metaFQN, err)
		}
	default:
		// should be unreachable
		glog.Errorf("Invalid request type: %d", iReq.act)
	}
}

func (r *XactRespond) Stop(error) { r.Abort() }

func (r *XactRespond) stop() {
	if r.Finished() {
		glog.Warningf("%s - not running, nothing to do", r)
		return
	}

	XactCount.Dec()
	r.XactDemandBase.Stop()
	r.SetEndTime(time.Now())
}
