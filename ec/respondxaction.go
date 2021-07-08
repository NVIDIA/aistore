// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	rspFactory struct {
		xreg.BaseBckEntry
		xact *XactRespond
	}
	// Xaction responsible for responding to EC requests of other targets.
	// Should not be stopped if number of known targets is small.
	XactRespond struct {
		xactECBase
	}
)

// interface guard
var (
	_ xaction.XactDemand = (*XactRespond)(nil)
	_ xreg.BckFactory    = (*rspFactory)(nil)
)

////////////////
// rspFactory //
////////////////

func (*rspFactory) New(_ *xreg.Args) xreg.BucketEntry { return &rspFactory{} }
func (*rspFactory) Kind() string                      { return cmn.ActECRespond }
func (p *rspFactory) Get() cluster.Xact               { return p.xact }

func (p *rspFactory) Start(bck cmn.Bck) error {
	var (
		xec         = ECM.NewRespondXact(bck)
		config      = cmn.GCO.Get()
		totallyIdle = config.Timeout.SendFile.D()
		likelyIdle  = config.Timeout.MaxKeepalive.D()
	)
	xec.XactDemandBase = *xaction.NewXDB(cos.GenUUID(), p.Kind(), &bck, totallyIdle, likelyIdle)
	xec.InitIdle()
	p.xact = xec
	go xec.Run()
	return nil
}

/////////////////
// XactRespond //
/////////////////

func NewRespondXact(t cluster.Target, bck cmn.Bck, mgr *Manager) *XactRespond {
	smap, si := t.Sowner(), t.Snode()
	runner := &XactRespond{
		xactECBase: newXactECBase(t, smap, si, bck, mgr),
	}

	return runner
}

func (r *XactRespond) Run() {
	glog.Infoln(r.String())

	var (
		cfg    = cmn.GCO.Get()
		ticker = time.NewTicker(cfg.Periodic.StatsTime.D())
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
			return
		case <-r.ChanAbort():
			r.stop()
			return
		}
	}
}

// Utility function to cleanup both object/slice and its meta on the local node
// Used when processing object deletion request
func (r *XactRespond) removeObjAndMeta(bck *cluster.Bck, objName string) error {
	if glog.FastV(4, glog.SmoduleEC) {
		glog.Infof("Delete request for %s/%s", bck.Name, objName)
	}

	ct, err := cluster.NewCTFromBO(bck.Bck, objName, r.t.Bowner(), fs.ECSliceType)
	if err != nil {
		return err
	}
	ct.Lock(true)
	defer ct.Unlock(true)

	// to be consistent with PUT, object's files are deleted in a reversed
	// order: first Metafile is removed, then Replica/Slice
	// Why: the main object is gone already, so we do not want any target
	// responds that it has the object because it has metafile. We delete
	// metafile that makes remained slices/replicas outdated and can be cleaned
	// up later by LRU or other runner
	for _, tp := range []string{fs.ECMetaType, fs.ObjectType, fs.ECSliceType} {
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

func (r *XactRespond) trySendCT(iReq intraReq, bck *cluster.Bck, objName string) error {
	var (
		fqn, metaFQN string
		md           *Metadata
	)
	if glog.FastV(4, glog.SmoduleEC) {
		glog.Infof("Received request for slice %d of %s", iReq.meta.SliceID, objName)
	}
	if iReq.isSlice {
		ct, err := cluster.NewCTFromBO(bck.Bck, objName, r.t.Bowner(), fs.ECSliceType)
		if err != nil {
			return err
		}
		ct.Lock(false)
		defer ct.Unlock(false)
		fqn = ct.FQN()
		metaFQN = ct.Make(fs.ECMetaType)
		if md, err = LoadMetadata(metaFQN); err != nil {
			return err
		}
	}

	return r.dataResponse(respPut, fqn, bck, objName, iReq.sender, md)
}

// DispatchReq is responsible for handling request from other targets
func (r *XactRespond) DispatchReq(iReq intraReq, bck *cluster.Bck, objName string) {
	switch iReq.act {
	case reqDel:
		// object cleanup request: delete replicas, slices and metafiles
		if err := r.removeObjAndMeta(bck, objName); err != nil {
			glog.Errorf("%s failed to delete %s/%s: %v", r.t.Snode(), bck.Name, objName, err)
		}
	case reqGet:
		err := r.trySendCT(iReq, bck, objName)
		if err != nil {
			glog.Error(err)
		}
	default:
		// invalid request detected
		glog.Errorf("Invalid request type %d", iReq.act)
	}
}

func (r *XactRespond) DispatchResp(iReq intraReq, hdr transport.ObjHdr, object io.Reader) {
	r.IncPending()
	defer r.DecPending() // no async operation, so DecPending is deferred
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
			cos.DrainReader(object)
			glog.Errorf("%s: no metadata for %s", r.t.Snode(), hdr.FullName())
			return
		}

		if glog.FastV(4, glog.SmoduleEC) {
			glog.Infof("Got slice=%t from %s (#%d of %s) v%s, cksum: %s", iReq.isSlice, iReq.sender,
				iReq.meta.SliceID, hdr.FullName(), meta.ObjVersion, meta.CksumValue)
		}
		md := meta.NewPack()
		if iReq.isSlice {
			args := &WriteArgs{Reader: object, MD: md, BID: iReq.bid, Generation: meta.Generation}
			err = WriteSliceAndMeta(r.t, &hdr, args)
		} else {
			var lom *cluster.LOM
			lom, err = cluster.AllocLomFromHdr(&hdr)
			if err == nil {
				args := &WriteArgs{
					Reader:     object,
					MD:         md,
					Cksum:      hdr.ObjAttrs.Cksum,
					BID:        iReq.bid,
					Generation: meta.Generation,
				}
				err = WriteReplicaAndMeta(r.t, lom, args)
			}
			cluster.FreeLOM(lom)
		}
		if err != nil {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				cos.DrainReader(object)
			}
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
	r.XactDemandBase.Stop()
	r.Finish(nil)
}

func (r *XactRespond) Stats() cluster.XactStats {
	baseStats := r.XactDemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: r.Pending() == 0}
	return baseStats
}
