// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	rspFactory struct {
		xreg.RenewBase
		xctn *XactRespond
	}
	// Xaction responsible for responding to EC requests of other targets.
	// Should not be stopped if number of known targets is small.
	XactRespond struct {
		xactECBase
	}
)

// interface guard
var (
	_ xact.Demand    = (*XactRespond)(nil)
	_ xreg.Renewable = (*rspFactory)(nil)
)

////////////////
// rspFactory //
////////////////

func (*rspFactory) New(_ xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &rspFactory{RenewBase: xreg.RenewBase{Bck: bck}}
	return p
}

func (*rspFactory) Kind() string        { return apc.ActECRespond }
func (p *rspFactory) Get() cluster.Xact { return p.xctn }

func (p *rspFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

func (p *rspFactory) Start() error {
	xec := ECM.NewRespondXact(p.Bck.Bucket())
	xec.DemandBase.Init(cos.GenUUID(), p.Kind(), p.Bck, 0 /*use default*/)
	p.xctn = xec
	go xec.Run(nil)
	return nil
}

/////////////////
// XactRespond //
/////////////////

func NewRespondXact(t cluster.Target, bck *cmn.Bck, mgr *Manager) *XactRespond {
	var (
		config   = cmn.GCO.Get()
		smap, si = t.Sowner(), t.Snode()
	)
	return &XactRespond{xactECBase: newXactECBase(t, smap, si, config, bck, mgr)}
}

func (r *XactRespond) Run(*sync.WaitGroup) {
	nlog.Infoln(r.Name())

	ticker := time.NewTicker(r.config.Periodic.StatsTime.D())
	defer ticker.Stop()

	// as of now all requests are equal (TODO: throttle)
	for {
		select {
		case <-ticker.C:
			if s := r.stats.stats().String(); s != "" {
				nlog.Infoln(s)
			}
		case <-r.IdleTimer():
			r.stop(nil)
			return
		case errCause := <-r.ChanAbort():
			r.stop(errCause)
			return
		}
	}
}

// Utility function to cleanup both object/slice and its meta on the local node
// Used when processing object deletion request
func (r *XactRespond) removeObjAndMeta(bck *meta.Bck, objName string) error {
	if r.config.FastV(4, cos.SmoduleEC) {
		nlog.Infof("Delete request for %s", bck.Cname(objName))
	}

	ct, err := cluster.NewCTFromBO(bck.Bucket(), objName, r.t.Bowner(), fs.ECSliceType)
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
		fqnMeta, _, err := cluster.HrwFQN(bck.Bucket(), tp, objName)
		if err != nil {
			return err
		}
		if err := os.Remove(fqnMeta); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing %s %q: %w", tp, fqnMeta, err)
		}
	}

	return nil
}

func (r *XactRespond) trySendCT(iReq intraReq, hdr *transport.ObjHdr, bck *meta.Bck) error {
	var (
		fqn, metaFQN string
		md           *Metadata
		objName      = hdr.ObjName
	)
	if r.config.FastV(4, cos.SmoduleEC) {
		nlog.Infof("Received request for slice %d of %s", iReq.meta.SliceID, objName)
	}
	if iReq.isSlice {
		ct, err := cluster.NewCTFromBO(bck.Bucket(), objName, r.t.Bowner(), fs.ECSliceType)
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

	return r.dataResponse(respPut, hdr, fqn, bck, objName, md)
}

// DispatchReq is responsible for handling request from other targets
func (r *XactRespond) DispatchReq(iReq intraReq, hdr *transport.ObjHdr, bck *meta.Bck) {
	switch hdr.Opcode {
	case reqDel:
		// object cleanup request: delete replicas, slices and metafiles
		if err := r.removeObjAndMeta(bck, hdr.ObjName); err != nil {
			nlog.Errorf("%s failed to delete %s: %v", r.t, bck.Cname(hdr.ObjName), err)
		}
	case reqGet:
		err := r.trySendCT(iReq, hdr, bck)
		if err != nil {
			nlog.Errorln(err)
		}
	default:
		// invalid request detected
		nlog.Errorf("Invalid request type %d", hdr.Opcode)
	}
}

func (r *XactRespond) DispatchResp(iReq intraReq, hdr *transport.ObjHdr, object io.Reader) {
	r.IncPending()
	defer r.DecPending() // no async operation, so DecPending is deferred
	switch hdr.Opcode {
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
			nlog.Errorf("%s: no metadata for %s", r.t, hdr.Cname())
			return
		}

		if r.config.FastV(4, cos.SmoduleEC) {
			nlog.Infof("Got slice=%t from %s (#%d of %s) v%s, cksum: %s", iReq.isSlice, hdr.SID,
				iReq.meta.SliceID, hdr.Cname(), meta.ObjVersion, meta.CksumValue)
		}
		md := meta.NewPack()
		if iReq.isSlice {
			args := &WriteArgs{Reader: object, MD: md, BID: iReq.bid, Generation: meta.Generation, Xact: r}
			err = WriteSliceAndMeta(r.t, hdr, args)
		} else {
			var lom *cluster.LOM
			lom, err = cluster.AllocLomFromHdr(hdr)
			if err == nil {
				args := &WriteArgs{
					Reader:     object,
					MD:         md,
					Cksum:      hdr.ObjAttrs.Cksum,
					BID:        iReq.bid,
					Generation: meta.Generation,
					Xact:       r,
				}
				err = WriteReplicaAndMeta(r.t, lom, args)
			}
			cluster.FreeLOM(lom)
		}
		if err != nil {
			nlog.Errorln(err)
			return
		}
		r.ObjsAdd(1, hdr.ObjAttrs.Size)
	default:
		// should be unreachable
		nlog.Errorf("Invalid request type: %d", hdr.Opcode)
	}
}

func (r *XactRespond) Stop(err error) { r.Abort(err) }

func (r *XactRespond) stop(err error) {
	r.DemandBase.Stop()
	r.Finish(err)
}

// (compare w/ XactGet/Put)
func (r *XactRespond) Snap() *cluster.Snap { return r.baseSnap() }
