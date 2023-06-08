// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
)

// TODO: currently, cannot return errors from the receive handlers, here and elsewhere
//       (see `_regRecv` for "static lifecycle")

func (reb *Reb) _recvErr(err error) error {
	if err == nil {
		return err
	}
	if xreb := reb.xctn(); xreb != nil {
		xreb.Abort(err)
	}
	return nil
}

func (reb *Reb) recvObj(hdr transport.ObjHdr, objReader io.Reader, err error) error {
	defer transport.DrainAndFreeReader(objReader)
	if err != nil {
		glog.Error(err)
		return err
	}

	smap, err := reb._waitForSmap()
	if err != nil {
		return reb._recvErr(err)
	}
	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		glog.Errorf("Failed to read message type: %v", err)
		return reb._recvErr(err)
	}
	if act == rebMsgRegular {
		err := reb.recvObjRegular(hdr, smap, unpacker, objReader)
		return reb._recvErr(err)
	}
	debug.Assertf(act == rebMsgEC, "act=%d", act)
	err = reb.recvECData(hdr, unpacker, objReader)
	return reb._recvErr(err)
}

func (reb *Reb) recvAck(hdr transport.ObjHdr, _ io.Reader, err error) error {
	if err != nil {
		glog.Error(err)
		return err
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		err = fmt.Errorf("failed to read message type: %v", err)
		return reb._recvErr(err)
	}
	if act == rebMsgEC {
		err := reb.recvECAck(hdr, unpacker)
		return reb._recvErr(err)
	}
	debug.Assertf(act == rebMsgRegular, "act=%d", act)
	err = reb.recvRegularAck(hdr, unpacker)
	return reb._recvErr(err)
}

func (reb *Reb) recvStageNtfn(hdr transport.ObjHdr, _ io.Reader, errRx error) error {
	if errRx != nil {
		glog.Errorf("%s: %v", reb.t, errRx)
		return errRx
	}
	ntfn, err := reb.decodeStageNtfn(hdr.Opaque)
	if err != nil {
		return reb._recvErr(err)
	}

	var (
		rebID      = reb.RebID()
		rsmap      = reb.smap.Load()
		otherStage = stages[ntfn.stage]
		xreb       = reb.xctn()
	)
	if xreb == nil {
		if reb.stages.stage.Load() != rebStageInactive {
			glog.Errorf("%s: nil rebalancing xaction", reb.logHdr(rebID, (*meta.Smap)(rsmap)))
		}
		return nil
	}
	if xreb.IsAborted() {
		return nil
	}

	// TODO: see "static lifecycle" comment above

	// eq
	if rebID == ntfn.rebID {
		reb.stages.setStage(ntfn.daemonID, ntfn.stage)
		if ntfn.stage == rebStageAbort {
			err := fmt.Errorf("abort stage notification from %s(%s)", meta.Tname(ntfn.daemonID), otherStage)
			glog.Error(err)
			xreb.Abort(cmn.NewErrAborted(xreb.Name(), reb.logHdr(rebID, (*meta.Smap)(rsmap)), err))
		}
		return nil
	}
	// other's old
	if rebID > ntfn.rebID {
		glog.Warningf("%s: stage notification from %s(%s): %s", reb.logHdr(rebID, (*meta.Smap)(rsmap)),
			meta.Tname(ntfn.daemonID), otherStage, reb.warnID(ntfn.rebID, ntfn.daemonID))
		return nil
	}

	xreb.Abort(cmn.NewErrAborted(xreb.Name(), reb.logHdr(rebID, (*meta.Smap)(rsmap)), err))
	return nil
}

//
// regular (non-EC) receive
//

func (reb *Reb) recvObjRegular(hdr transport.ObjHdr, smap *meta.Smap, unpacker *cos.ByteUnpack, objReader io.Reader) error {
	ack := &regularAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse ACK: %v", err)
		return err
	}
	if ack.rebID != reb.RebID() {
		glog.Warningf("received %s: %s", hdr.Cname(), reb.warnID(ack.rebID, ack.daemonID))
		return nil
	}
	tsid := ack.daemonID // the sender
	// Rx
	lom := cluster.AllocLOM(hdr.ObjName)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(&hdr.Bck); err != nil {
		glog.Error(err)
		return nil
	}
	if stage := reb.stages.stage.Load(); stage >= rebStageFin {
		reb.laterx.Store(true)
		if stage > rebStageFin && cmn.FastV(4, glog.SmoduleReb) {
			glog.Infof("Warning: %s: post stage-fin receive from %s %s (stage %s)",
				reb.t.Snode(), meta.Tname(tsid), lom, stages[stage])
		}
	} else if stage < rebStageTraverse {
		glog.Errorf("%s: early receive from %s %s (stage %s)", reb.t, meta.Tname(tsid), lom, stages[stage])
	}
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip-checksum*/) // see "PUT is a no-op"
	xreb := reb.xctn()
	if xreb.IsAborted() {
		return nil
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfilePut
		params.Reader = io.NopCloser(objReader)
		params.OWT = cmn.OwtMigrate
		params.Cksum = hdr.ObjAttrs.Cksum
		params.Atime = lom.Atime()
		params.Xact = xreb
	}
	erp := reb.t.PutObject(lom, params)
	cluster.FreePutObjParams(params)
	if erp != nil {
		glog.Error(erp)
		return erp
	}
	// stats
	xreb.InObjsAdd(1, hdr.ObjAttrs.Size)

	// ACK
	tsi := smap.GetTarget(tsid)
	if tsi == nil {
		err := fmt.Errorf("%s is not in the %s", meta.Tname(tsid), smap)
		glog.Error(err)
		return err
	}
	if stage := reb.stages.stage.Load(); stage < rebStageFinStreams && stage != rebStageInactive {
		ack := &regularAck{rebID: reb.RebID(), daemonID: reb.t.SID()}
		hdr.Opaque = ack.NewPack()
		hdr.ObjAttrs.Size = 0
		if err := reb.dm.ACK(hdr, nil, tsi); err != nil {
			glog.Error(err)
			return err
		}
	}
	return nil
}

func (reb *Reb) recvRegularAck(hdr transport.ObjHdr, unpacker *cos.ByteUnpack) error {
	ack := &regularAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse ACK: %v", err)
		return err
	}
	if ack.rebID != reb.rebID.Load() {
		glog.Warningf("ACK from %s: %s", ack.daemonID, reb.warnID(ack.rebID, ack.daemonID))
		return nil
	}

	lom := cluster.AllocLOM(hdr.ObjName)
	if err := lom.InitBck(&hdr.Bck); err != nil {
		cluster.FreeLOM(lom)
		glog.Error(err)
		return nil
	}

	// No immediate file deletion: let LRU cleanup the "misplaced" object
	// TODO: mark the object "Deleted"

	reb.delLomAck(lom, ack.rebID, true /*free pending (orig) transmitted LOM*/)
	cluster.FreeLOM(lom)
	return nil
}

//
// EC receive
//

func (*Reb) recvECAck(hdr transport.ObjHdr, unpacker *cos.ByteUnpack) (err error) {
	ack := &ecAck{}
	err = unpacker.ReadAny(ack)
	if err != nil {
		glog.Errorf("Failed to unmarshal EC ACK for %s: %v", hdr.Cname(), err)
	}
	return
}

// Receive MD update. Handling includes partially updating local information:
// only the list of daemons and the _main_ target.
func (reb *Reb) receiveMD(req *stageNtfn, hdr transport.ObjHdr) error {
	ctMeta, err := cluster.NewCTFromBO(&hdr.Bck, hdr.ObjName, reb.t.Bowner(), fs.ECMetaType)
	if err != nil {
		return err
	}
	md, err := ec.LoadMetadata(ctMeta.FQN())
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}
	if md.Generation != req.md.Generation {
		return nil
	}
	md.FullReplica = req.md.FullReplica
	md.Daemons = req.md.Daemons
	mdBytes := md.NewPack()
	return ctMeta.Write(reb.t, bytes.NewReader(mdBytes), -1)
}

func (reb *Reb) receiveCT(req *stageNtfn, hdr transport.ObjHdr, reader io.Reader) error {
	ct, err := cluster.NewCTFromBO(&hdr.Bck, hdr.ObjName, reb.t.Bowner(), fs.ECSliceType)
	if err != nil {
		return err
	}
	md, err := reb.detectLocalCT(req, ct)
	if err != nil {
		glog.Errorf("%s: %v", ct.FQN(), err)
		return err
	}
	// Fix the metadata: update CT locations
	delete(req.md.Daemons, req.daemonID)
	if md != nil && req.md.Generation < md.Generation {
		// Local CT is newer - do not save anything
		return nil
	}
	// Check for slice conflict
	workFQN, moveTo, err := reb.renameLocalCT(req, ct, md)
	if err != nil {
		return err
	}
	req.md.FullReplica = reb.t.SID()
	req.md.Daemons[reb.t.SID()] = uint16(req.md.SliceID)
	if moveTo != nil {
		req.md.Daemons[moveTo.ID()] = uint16(md.SliceID)
	}
	// Save received CT to local drives
	err = reb.saveCTToDisk(req, &hdr, reader)
	if err != nil {
		if errRm := os.Remove(ct.FQN()); errRm != nil {
			glog.Errorf("Failed to remove %s: %v", ct.FQN(), errRm)
		}
		if moveTo != nil {
			if errMv := os.Rename(workFQN, ct.FQN()); errMv != nil {
				glog.Errorf("Error restoring slice: %v", errMv)
			}
		}
		return err
	}
	// Send local slice
	if moveTo != nil {
		req.md.SliceID = md.SliceID
		if err = reb.sendFromDisk(ct, req.md, moveTo, workFQN); err != nil {
			glog.Errorf("Failed to move slice to %s: %v", moveTo, err)
		}
	}
	// Broadcast updated MD
	ntfnMD := stageNtfn{daemonID: reb.t.SID(), stage: rebStageTraverse, rebID: reb.rebID.Load(), md: req.md, action: rebActUpdateMD}
	nodes := req.md.RemoteTargets(reb.t)
	for _, tsi := range nodes {
		if moveTo != nil && moveTo.ID() == tsi.ID() {
			continue
		}
		reb.onAir.Inc()
		xreb := reb.xctn()
		if xreb.IsAborted() {
			break
		}
		o := transport.AllocSend()
		o.Hdr = transport.ObjHdr{ObjName: ct.ObjectName(), ObjAttrs: cmn.ObjAttrs{Size: 0}}
		o.Hdr.Bck.Copy(ct.Bck().Bucket())
		o.Hdr.Opaque = ntfnMD.NewPack(rebMsgEC)
		o.Callback = reb.transportECCB
		if errSend := reb.dm.Send(o, nil, tsi); errSend != nil && err == nil {
			err = fmt.Errorf("failed to send updated metafile: %v", err)
		}
	}
	return err
}

// receiving EC CT
func (reb *Reb) recvECData(hdr transport.ObjHdr, unpacker *cos.ByteUnpack, reader io.Reader) error {
	req := &stageNtfn{}
	err := unpacker.ReadAny(req)
	if err != nil {
		glog.Errorf("invalid stage notification %s: %v", hdr.ObjName, err)
		return err
	}
	if req.rebID != reb.rebID.Load() {
		glog.Warningf("%s: not yet started or already finished rebalancing (%d, %d)",
			reb.t.Snode(), req.rebID, reb.rebID.Load())
		return nil
	}
	if req.action == rebActUpdateMD {
		err := reb.receiveMD(req, hdr)
		if err != nil {
			glog.Errorf("failed to receive MD for %s: %v", hdr.Cname(), err)
		}
		return err
	}
	if err := reb.receiveCT(req, hdr, reader); err != nil {
		glog.Errorf("failed to receive CT for %s: %v", hdr.Cname(), err)
		return err
	}
	return nil
}
