// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/registry"
)

type (
	// FIXME: Does `XactRespond` needs to be a `XactDemand`?
	//  - it doesn't use `incPending()`

	// Implements `registry.BucketEntryProvider` and `registry.BucketEntry` interface.
	xactRespondProvider struct {
		registry.BaseBckEntry
		xact *XactRespond
	}

	// Xaction responsible for responding to EC requests of other targets.
	// Should not be stopped if number of known targets is small.
	XactRespond struct {
		xactECBase
	}
)

func (p *xactRespondProvider) New(_ registry.XactArgs) registry.BucketEntry {
	return &xactRespondProvider{}
}

func (p *xactRespondProvider) Start(bck cmn.Bck) error {
	var (
		xec      = ECM.NewRespondXact(bck)
		idleTime = cmn.GCO.Get().Timeout.SendFile
	)
	xec.XactDemandBase = *xaction.NewXactDemandBaseBck(p.Kind(), bck, idleTime)
	xec.InitIdle()
	p.xact = xec
	go xec.Run()
	return nil
}
func (*xactRespondProvider) Kind() string        { return cmn.ActECRespond }
func (p *xactRespondProvider) Get() cluster.Xact { return p.xact }

func NewRespondXact(t cluster.Target, bck cmn.Bck, reqBundle, respBundle *bundle.Streams) *XactRespond {
	XactCount.Inc()
	smap, si := t.Sowner(), t.Snode()
	runner := &XactRespond{
		xactECBase: newXactECBase(t, smap, si, bck, reqBundle, respBundle),
	}

	return runner
}

func (r *XactRespond) Run() (err error) {
	glog.Infoln(r.String())

	var (
		cfg    = cmn.GCO.Get()
		ticker = time.NewTicker(cfg.Periodic.StatsTime)
	)
	defer ticker.Stop()

	// as of now all requests are equal. Some may get throttling later
	for {
		select {
		case <-ticker.C:
			if s := fmt.Sprintf("%v", r.stats.stats()); s != "" {
				glog.Info(s)
			}
		case <-r.IdleTimer():
			r.stop()
			return nil
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
			glog.Errorf("%s failed to delete %s/%s: %v", r.t.Snode(), bck.Name, objName, err)
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
			ct, err := cluster.NewCTFromBO(bck.Bck.Name, bck.Bck.Provider, objName, r.t.Bowner(), SliceType)
			if err != nil {
				glog.Error(err)
				return
			}
			fqn = ct.FQN()
			metaFQN = ct.Make(MetaType)
			md, err = LoadMetadata(metaFQN)
			if err != nil {
				glog.Error(err)
				return
			}
		} else if glog.V(4) {
			glog.Infof("Received request for replica %s", objName)
		}

		if err = r.dataResponse(respPut, fqn, bck, objName, daemonID, md); err != nil {
			glog.Errorf("%s failed to send back [GET req] %q: %v", r.t.Snode(), fqn, err)
		}
	default:
		// invalid request detected
		glog.Errorf("Invalid request type %d", iReq.act)
	}
}

func (r *XactRespond) DispatchResp(iReq intraReq, hdr transport.ObjHdr, object io.Reader) {
	drain := func() {
		if err := cmn.DrainReader(object); err != nil {
			glog.Warningf("Failed to drain reader %s/%s: %v", hdr.Bck, hdr.ObjName, err)
		}
	}

	switch iReq.act {
	case reqPut:
		// a remote target sent a replica/slice while it was
		// encoding or restoring an object. In this case it just saves
		// the sent replica or slice to a local file along with its metadata

		// Check if the request is valid: it must contain metadata
		var (
			err  error
			meta = iReq.meta
		)
		if meta == nil {
			drain()
			glog.Errorf("%s no metadata in request for %s/%s", r.t.Snode(), hdr.Bck, hdr.ObjName)
			return
		}

		if glog.V(4) {
			glog.Infof("Got slice=%t from %s (#%d of %s/%s) v%s",
				iReq.isSlice, iReq.sender, iReq.meta.SliceID, hdr.Bck, hdr.ObjName, meta.ObjVersion)
		}
		md := meta.Marshal()
		if iReq.isSlice {
			err = WriteSliceAndMeta(r.t, hdr, object, md)
		} else {
			var lom *cluster.LOM
			lom, err = LomFromHeader(r.t, hdr)
			if err == nil {
				err = WriteReplicaAndMeta(r.t, lom, object, md, hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue)
			}
		}
		if err != nil {
			drain()
			glog.Error(err)
			return
		}
		r.ObjectsInc()
		r.BytesAdd(hdr.ObjAttrs.Size)
	default:
		// should be unreachable
		glog.Errorf("Invalid request type: %d", iReq.act)
	}
}

func (r *XactRespond) Stop(error) { r.Abort() }

func (r *XactRespond) stop() {
	XactCount.Dec()
	r.XactDemandBase.Stop()
	r.Finish()
}
