// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
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

func (*rspFactory) Kind() string     { return apc.ActECRespond }
func (p *rspFactory) Get() core.Xact { return p.xctn }

func (p *rspFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

func (p *rspFactory) Start() error {
	xec := ECM.NewRespondXact(p.Bck.Bucket())
	xec.DemandBase.Init(cos.GenUUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, 0 /*use default*/)
	p.xctn = xec

	xact.GoRunW(xec)
	return nil
}

/////////////////
// XactRespond //
/////////////////

func newRespondXact(bck *cmn.Bck, mgr *Manager) *XactRespond {
	xctn := &XactRespond{}
	xctn.xactECBase.init(cmn.GCO.Get(), bck, mgr)
	return xctn
}

func (r *XactRespond) Run(gowg *sync.WaitGroup) {
	nlog.Infoln(r.Name())

	ECM.incActive(r)
	gowg.Done()

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
func (*XactRespond) removeObjAndMeta(bck *meta.Bck, objName string) error {
	if cmn.Rom.FastV(4, cos.SmoduleEC) {
		nlog.Infof("Delete request for %s", bck.Cname(objName))
	}

	ct, err := core.NewCTFromBO(bck, objName, fs.ECSliceCT)
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
	for _, tp := range []string{fs.ECMetaCT, fs.ObjCT, fs.ECSliceCT} {
		fqnMeta, _, err := core.HrwFQN(bck.Bucket(), tp, objName)
		if err != nil {
			return err
		}
		if err := os.Remove(fqnMeta); err != nil && !cos.IsNotExist(err) {
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
	if cmn.Rom.FastV(4, cos.SmoduleEC) {
		nlog.Infof("Received request for slice %d of %s", iReq.meta.SliceID, objName)
	}
	if iReq.isSlice {
		ct, err := core.NewCTFromBO(bck, objName, fs.ECSliceCT)
		if err != nil {
			return err
		}
		ct.Lock(false)
		defer ct.Unlock(false)
		fqn = ct.FQN()
		metaFQN = ct.GenFQN(fs.ECMetaCT)
		if md, err = LoadMetadata(metaFQN); err != nil {
			return err
		}
	}

	return r.dataResponse(respPut, hdr, fqn, bck, objName, md)
}

// DispatchReq is responsible for handling request from other targets
func (r *XactRespond) dispatchReq(iReq intraReq, hdr *transport.ObjHdr, bck *meta.Bck) {
	switch hdr.Opcode {
	case reqDel:
		// object cleanup request: delete replicas, slices and metafiles
		if err := r.removeObjAndMeta(bck, hdr.ObjName); err != nil {
			err = cmn.NewErrFailedTo(core.T, "delete", bck.Cname(hdr.ObjName), err)
			r.AddErr(err, 0)
		}
	case reqGet:
		err := r.trySendCT(iReq, hdr, bck)
		if err != nil {
			r.AddErr(err, 0)
		}
	default:
		debug.Assert(false, invalOpcode, " ", hdr.Opcode)
		nlog.Errorln(r.Name(), invalOpcode, hdr.Opcode)
	}
}

func (r *XactRespond) dispatchResp(iReq intraReq, hdr *transport.ObjHdr, object io.Reader) {
	switch hdr.Opcode {
	case reqPut:
		// a remote target sent a replica/slice while it was
		// encoding or restoring an object. In this case it just saves
		// the sent replica or slice to a local file along with its metadata

		// Check if the request is valid: it must contain metadata
		var (
			err error
			md  = iReq.meta
		)
		if md == nil {
			nlog.Errorln(core.T.String(), "no metadata for", hdr.Cname())
			return
		}
		if cmn.Rom.FastV(4, cos.SmoduleEC) {
			nlog.Infof("Got slice=%t from %s (#%d of %s) v%s, cksum: %s", iReq.isSlice, hdr.SID,
				iReq.meta.SliceID, hdr.Cname(), md.ObjVersion, md.CksumValue)
		}

		mdbytes := md.NewPack()
		if iReq.isSlice {
			args := &WriteArgs{Reader: object, MD: mdbytes, BID: iReq.bid, Generation: md.Generation, Xact: r}
			err = WriteSliceAndMeta(hdr, args)
		} else {
			var lom *core.LOM
			lom, err = AllocLomFromHdr(hdr)
			if err == nil {
				args := &WriteArgs{
					Reader:     object,
					MD:         mdbytes,
					Cksum:      hdr.ObjAttrs.Cksum,
					BID:        iReq.bid,
					Generation: md.Generation,
					Xact:       r,
				}
				err = WriteReplicaAndMeta(lom, args)
			}
			core.FreeLOM(lom)
		}
		if err != nil {
			r.AddErr(err, 0)
			return
		}
		r.ObjsAdd(1, hdr.ObjAttrs.Size)
	default:
		debug.Assert(false, "opcode", hdr.Opcode)
		nlog.Errorf("Invalid request type: %d", hdr.Opcode)
	}
}

func (r *XactRespond) Stop(err error) { r.Abort(err) }

func (r *XactRespond) stop() {
	r.DemandBase.Stop()
	r.Finish()
}

// (compare w/ XactGet/Put)
func (r *XactRespond) Snap() *core.Snap { return r.baseSnap() }
