// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xreg"
)

func (reb *Reb) recvObj(hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil {
		glog.Error(err)
		return
	}
	smap, err := reb._waitForSmap()
	if err != nil {
		glog.Errorf("%v: dropping %s", err, hdr.FullName())
		return
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		glog.Errorf("Failed to read message type: %v", err)
		return
	}
	if act == rebMsgRegular {
		reb.recvObjRegular(hdr, smap, unpacker, objReader)
		return
	}
	if act != rebMsgEC {
		glog.Errorf("Invalid ACK type %d, expected %d", act, rebMsgEC)
	}
	reb.recvECData(hdr, unpacker, objReader)
}

func (reb *Reb) recvAck(hdr transport.ObjHdr, _ io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		glog.Errorf("failed to read message type: %v", err)
		return
	}
	if act == rebMsgEC {
		reb.recvECAck(hdr, unpacker)
		return
	}
	if act != rebMsgRegular {
		glog.Errorf("Invalid ACK type %d, expected %d", act, rebMsgRegular)
	}
	reb.recvRegularAck(hdr, unpacker)
}

///////////////
// Rx non-EC //
///////////////

func (reb *Reb) recvObjRegular(hdr transport.ObjHdr, smap *cluster.Smap, unpacker *cos.ByteUnpack, objReader io.Reader) {
	defer cos.DrainReader(objReader)

	ack := &regularAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse acknowledgement: %v", err)
		return
	}
	if ack.rebID != reb.RebID() {
		glog.Warningf("received %s: %s", hdr.FullName(), reb.rebIDMismatchMsg(ack.rebID))
		return
	}
	tsid := ack.daemonID // the sender
	// Rx
	lom := cluster.AllocLOM(hdr.ObjName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	marked := xreg.GetRebMarked()
	if marked.Interrupted || marked.Xact == nil {
		return
	}

	if stage := reb.stages.stage.Load(); stage >= rebStageFin {
		reb.laterx.Store(true)
		if stage > rebStageFin && glog.FastV(4, glog.SmoduleReb) {
			glog.Errorf("%s: post stage-fin receive from %s %s (stage %s)",
				reb.t.Snode(), tsid, lom, stages[stage])
		}
	} else if stage < rebStageTraverse {
		glog.Errorf("%s: early receive from %s %s (stage %s)", reb.t.Snode(), tsid, lom, stages[stage])
	}

	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip-checksum*/) // see "PUT is a no-op"
	params := cluster.PutObjectParams{
		Tag:      fs.WorkfilePut,
		Reader:   io.NopCloser(objReader),
		RecvType: cluster.Migrated,
		Cksum:    hdr.ObjAttrs.Cksum,
		Started:  time.Now(),
	}
	if err := reb.t.PutObject(lom, params); err != nil {
		glog.Error(err)
		return
	}

	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof("%s: from %s %s", reb.t.Snode(), tsid, lom)
	}
	xreb := reb.xact()
	xreb.InObjsAdd(1)
	xreb.InBytesAdd(hdr.ObjAttrs.Size)
	// ACK
	tsi := smap.GetTarget(tsid)
	if tsi == nil {
		glog.Errorf("%s target is not found in smap", tsid)
		return
	}
	if stage := reb.stages.stage.Load(); stage < rebStageFinStreams && stage != rebStageInactive {
		ack := &regularAck{rebID: reb.RebID(), daemonID: reb.t.SID()}
		hdr.Opaque = ack.NewPack()
		hdr.ObjAttrs.Size = 0
		if err := reb.dm.ACK(hdr, nil, tsi); err != nil {
			glog.Error(err) // TODO: collapse same-type errors e.g. "src-id=>network: destination mismatch"
		}
	}
}

func (reb *Reb) recvRegularAck(hdr transport.ObjHdr, unpacker *cos.ByteUnpack) {
	ack := &regularAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse acknowledge: %v", err)
		return
	}
	if ack.rebID != reb.rebID.Load() {
		glog.Warningf("ACK from %s: %s", ack.daemonID, reb.rebIDMismatchMsg(ack.rebID))
		return
	}

	lom := cluster.AllocLOM(hdr.ObjName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof("%s: ack from %s on %s", reb.t.Snode(), string(hdr.Opaque), lom)
	}
	// No immediate file deletion: let LRU cleanup the "misplaced" object
	// TODO: mark the object "Deleted"
	reb.delLomAck(lom)
}

///////////
// Rx EC //
///////////

func (reb *Reb) recvPush(hdr transport.ObjHdr, _ io.Reader, err error) {
	if err != nil {
		glog.Errorf("%s: failed to receive notification %s from %s: %v", reb.t.Snode(), hdr.ObjName, hdr.Bck, err)
		return
	}

	req, err := reb.decodePushReq(hdr.Opaque)
	if err != nil {
		glog.Error(err)
		return
	}

	if req.stage == rebStageAbort && reb.RebID() <= req.rebID {
		// a target aborted its xaction and sent the signal to others
		glog.Warningf("%s: abort notification from %s", reb.t.Snode(), req.daemonID)
		if reb.xact() != nil {
			reb.xact().Abort(nil)
		}
		return
	}

	if reb.RebID() != req.rebID {
		glog.Warningf("%s: stage %v push notification %s", reb.t.Snode(), stages[req.stage], reb.rebIDMismatchMsg(req.rebID))
		return
	}

	reb.stages.setStage(req.daemonID, req.stage)
}

func (*Reb) recvECAck(hdr transport.ObjHdr, unpacker *cos.ByteUnpack) {
	ack := &ecAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to unmarshal EC ACK for %s: %v", hdr.FullName(), err)
		return
	}
}

// A sender sent an MD update. This target must update local information partially:
// only list of daemons and the "main" target.
func (reb *Reb) receiveMD(req *pushReq, hdr transport.ObjHdr) error {
	ctMeta, err := cluster.NewCTFromBO(hdr.Bck, hdr.ObjName, reb.t.Bowner(), fs.ECMetaType)
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

func (reb *Reb) receiveCT(req *pushReq, hdr transport.ObjHdr, reader io.Reader) error {
	defer cos.DrainReader(reader)
	ct, err := cluster.NewCTFromBO(hdr.Bck, hdr.ObjName, reb.t.Bowner(), fs.ECSliceType)
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
	req.md.FullReplica = reb.t.Snode().ID()
	req.md.Daemons[reb.t.Snode().ID()] = uint16(req.md.SliceID)
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
	reqMD := pushReq{daemonID: reb.t.SID(), stage: rebStageTraverse, rebID: reb.rebID.Load(), md: req.md, action: rebActUpdateMD}
	nodes := req.md.RemoteTargets(reb.t)
	for _, tsi := range nodes {
		if moveTo != nil && moveTo.ID() == tsi.ID() {
			continue
		}
		reb.onAir.Inc()
		xreb := reb.xact()
		if xreb.Aborted() {
			err = fmt.Errorf("failed to send updated metafile: %s", xreb)
			break
		}
		o := transport.AllocSend()
		o.Hdr = transport.ObjHdr{
			Bck:      ct.Bck().Bck,
			ObjName:  ct.ObjectName(),
			ObjAttrs: cmn.ObjAttrs{Size: 0},
		}
		o.Hdr.Opaque = reqMD.NewPack(rebMsgEC)
		o.Callback = reb.transportECCB
		if errSend := reb.dm.Send(o, nil, tsi); errSend != nil && err == nil {
			err = fmt.Errorf("failed to send updated metafile: %v", err)
		}
	}
	return err
}

// receiving EC CT
func (reb *Reb) recvECData(hdr transport.ObjHdr, unpacker *cos.ByteUnpack, reader io.Reader) {
	defer cos.DrainReader(reader)

	req := &pushReq{}
	err := unpacker.ReadAny(req)
	if err != nil {
		glog.Errorf("invalid push notification %s: %v", hdr.ObjName, err)
		return
	}
	if req.rebID != reb.rebID.Load() {
		glog.Warningf("%s: not yet started or already finished rebalancing (%d, %d)",
			reb.t.Snode(), req.rebID, reb.rebID.Load())
		return
	}
	if req.action == rebActUpdateMD {
		if err := reb.receiveMD(req, hdr); err != nil {
			glog.Errorf("failed to receive MD for %s: %v", hdr.FullName(), err)
		}
		return
	}
	if err := reb.receiveCT(req, hdr, reader); err != nil {
		glog.Errorf("failed to receive CT for %s: %v", hdr.FullName(), err)
	}
}
