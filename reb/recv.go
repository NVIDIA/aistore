// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
)

func (reb *Reb) _recvErr(err error) error {
	if err == nil {
		return nil
	}
	if xreb := reb.xctn(); xreb != nil {
		xreb.Abort(err)
		reb.lazydel.stop()
	}
	return nil
}

func (reb *Reb) recvObj(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	defer transport.DrainAndFreeReader(objReader)
	if err != nil {
		nlog.Errorln(err)
		return err
	}
	reb.lastrx.Store(mono.NanoTime())

	smap, err := reb._waitForSmap()
	if err != nil {
		return reb._recvErr(err)
	}
	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		nlog.Errorf("g[%d]: failed to recv recv-obj action (regular or EC): %v", reb.RebID(), err)
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

func (reb *Reb) recvAckNtfn(hdr *transport.ObjHdr, _ io.Reader, err error) error {
	if err != nil {
		nlog.Errorln(err)
		return err
	}
	reb.lastrx.Store(mono.NanoTime())

	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		err := fmt.Errorf("g[%d]: failed to unpack control (ack, ntfn) message type: %v", reb.RebID(), err)
		return reb._recvErr(err)
	}

	switch act {
	case rebMsgEC:
		err = reb.recvECAck(hdr, unpacker)
	case rebMsgRegular:
		err = reb.recvRegularAck(hdr, unpacker)
	case rebMsgNtfn:
		var ntfn stageNtfn
		err = unpacker.ReadAny(&ntfn)
		if err == nil {
			reb._handleNtfn(&ntfn)
		}
	default:
		err = fmt.Errorf("g[%d]: invalid ACK message type '%d' (expecting '%d')", reb.RebID(), act, rebMsgRegular)
	}

	return reb._recvErr(err)
}

func (reb *Reb) _handleNtfn(ntfn *stageNtfn) {
	var (
		rebID = reb.RebID()
		rsmap = reb.smap.Load()
		xreb  = reb.xctn()
	)
	if xreb == nil {
		if reb.stages.stage.Load() != rebStageInactive {
			nlog.Errorln(reb.logHdr(rebID, rsmap), "nil rebalancing xaction")
		}
		return
	}
	if xreb.IsAborted() {
		return
	}

	reb.lastrx.Store(mono.NanoTime())

	switch {
	case rebID == ntfn.rebID: // same stage
		reb.stages.setStage(ntfn.daemonID, ntfn.stage)
		if ntfn.stage == rebStageAbort {
			otherStage := stages[ntfn.stage]
			loghdr := reb.logHdr(rebID, rsmap)
			err := fmt.Errorf("abort stage notification from %s(%s)", meta.Tname(ntfn.daemonID), otherStage)
			xreb.Abort(cmn.NewErrAborted(xreb.Name(), loghdr, err))
		}
	case rebID > ntfn.rebID: // other's old
		loghdr := reb.logHdr(rebID, rsmap)
		nlog.Warningln(loghdr, reb.warnID(ntfn.rebID, ntfn.daemonID))
	default: // other's newer
		loghdr := reb.logHdr(rebID, rsmap)
		err := fmt.Errorf("%s: %s", loghdr, reb.warnID(ntfn.rebID, ntfn.daemonID))
		xreb.Abort(err)
		reb.lazydel.stop()
	}
}

//
// regular (non-EC) receive
//

func (reb *Reb) recvObjRegular(hdr *transport.ObjHdr, smap *meta.Smap, unpacker *cos.ByteUnpack, objReader io.Reader) error {
	ack := &regularAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		nlog.Errorf("g[%d]: failed to parse ACK: %v", reb.RebID(), err)
		return err
	}
	if ack.rebID != reb.RebID() {
		nlog.Warningln("received", hdr.Cname(), reb.warnID(ack.rebID, ack.daemonID))
		return nil
	}
	tsid := ack.daemonID // the sender
	// Rx
	lom := core.AllocLOM(hdr.ObjName)
	defer core.FreeLOM(lom)
	if err := lom.InitBck(&hdr.Bck); err != nil {
		nlog.Errorln(err)
		return nil
	}

	// log warn
	if stage := reb.stages.stage.Load(); stage >= rebStageFin {
		if stage > rebStageFin {
			warn := fmt.Sprintf("%s g[%d]: post stage-fin receive from %s %s (stage %s)", core.T, ack.rebID, meta.Tname(tsid), lom, stages[stage])
			nlog.Warningln(warn)
		}
	} else if stage < rebStageTraverse {
		nlog.Errorf("%s g[%d]: early receive from %s %s (stage %s)", core.T, reb.RebID(), meta.Tname(tsid), lom, stages[stage])
	}

	xreb := reb.xctn()
	latestVer, sync := _latestVer(lom.VersionConf(), xreb.Args.Flags)

	//
	// when destination exists:
	// VA (local)  <--> VB (from tsid sender) [ <--> VC (from cloud ]
	//
	if lom.Load(false, false) == nil {
		if lom.CheckEq(&hdr.ObjAttrs) == nil {
			// no-op: optimize-out duplicated write
			goto drainOk
		}

		if lom.Bck().IsRemote() && latestVer {
			oa, ecode, err := core.T.HeadCold(lom, nil)
			if err == nil {
				switch {
				case oa.CheckEq(&hdr.ObjAttrs) == nil:
					goto rx // receiving latest-ver from tsid (the sender)
				case oa.CheckEq(lom.ObjAttrs()) == nil:
					if cmn.Rom.V(5, cos.ModReb) {
						nlog.Infof("%s g[%d]: sender stale (%s), keeping local == latest %s", core.T, xreb.ID(), hdr.Cname(), lom.Cname())
					}
					goto drainOk
				default:
					if cmn.Rom.V(5, cos.ModReb) {
						nlog.Infoln("cold-get latest (when VC differs)", xreb.ID(), lom.Cname())
					}
					ecodeCold, errCold := core.T.GetCold(context.Background(), lom, xreb.Kind(), cmn.OwtGetTryLock)
					if errCold == nil {
						if cmn.Rom.V(5, cos.ModReb) {
							nlog.Infoln("cold-get ok")
						}
						goto drainOk // ok
					}
					nlog.Warningln("cold-get fail", xreb.ID(), lom.Cname(), ecodeCold, errCold)
				}
			}

			// --sync to maybe delete
			if cos.IsNotExist(err, ecode) && sync {
				// try to delete in place (TODO: compare with lom.CheckRemoteMD; unify)
				locked := lom.TryLock(true)
				errDel := lom.RemoveObj(true)
				if locked {
					lom.Unlock(true)
				}
				if errDel != nil {
					nlog.Errorf("%s g[%d]: failed to sync-delete %s: %v", core.T, reb.RebID(), lom, errDel)
				} else {
					// TODO -- FIXME: optimize vlabs out
					vlabs := map[string]string{"bucket": lom.Bck().Cname("")}
					core.T.StatsUpdater().IncWith(stats.RemoteDeletedDelCount, vlabs)
					if cmn.Rom.V(5, cos.ModReb) {
						nlog.Infof("%s g[%d]: sync-deleted %s", core.T, reb.RebID(), lom)
					}
				}
				goto drain
			}

			goto ambiguity // proceeding to "ambiguity"
		}

		if lom.Bck().IsAIS() && lom.VersionConf().Enabled {
			if remSrc, ok := lom.GetCustomKey(cmn.SourceObjMD); !ok || remSrc == "" {
				va, vb := lom.Version(), hdr.ObjAttrs.Version()
				vera, erra := strconv.ParseUint(va, 10, 64)
				verb, errb := strconv.ParseUint(vb, 10, 64)
				if erra == nil && errb == nil && vera > 0 && verb > 0 {
					switch {
					case verb > vera:
						goto rx // sender newer
					case verb < vera:
						goto drainOk // we’re newer
					}
				}
			}
			goto ambiguity // proceeding to "ambiguity"
		}

	drainOk: // success paths that require only draining
		xreb.InObjsAdd(1, hdr.ObjAttrs.Size)
		goto drain

	ambiguity:
		// cannot decide between the source and the destination
		nlog.Warningln("recv ambiguity - dropping/discarding [", xreb.ID(), lom.Cname(), lom.ObjAttrs().String(), hdr.ObjAttrs.String(), "]")

	drain: // drop/discard paths (no stats)
		cos.DrainReader(objReader)
		return reb.regACK(smap, hdr, tsid)
	}

rx:
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip-checksum*/) // see "PUT is a no-op"

	if xreb.IsAborted() {
		return nil
	}

	params := core.AllocPutParams()
	{
		params.WorkTag = fs.WorkfilePut
		params.Reader = io.NopCloser(objReader)
		params.OWT = cmn.OwtRebalance
		params.Cksum = hdr.ObjAttrs.Cksum
		params.Atime = lom.Atime()
		params.Xact = xreb
	}
	erp := core.T.PutObject(lom, params)
	core.FreePutParams(params)
	if erp != nil {
		nlog.Errorln(erp)
		return erp
	}
	// stats
	xreb.InObjsAdd(1, hdr.ObjAttrs.Size)

	// ACK
	return reb.regACK(smap, hdr, tsid)
}

func _latestVer(conf cmn.VersionConf, flags uint32) (latestVer, sync bool) {
	switch {
	case (flags&xact.FlagSync != 0) || conf.Sync:
		return true, true
	case (flags&xact.FlagLatestVer != 0) || conf.ValidateWarmGet:
		return true, false
	default:
		return false, false
	}
}

func (reb *Reb) regACK(smap *meta.Smap, hdr *transport.ObjHdr, tsid string) error {
	tsi := smap.GetTarget(tsid)
	if tsi == nil {
		err := fmt.Errorf("g[%d]: %s is not in the %s", reb.RebID(), meta.Tname(tsid), smap)
		nlog.Errorln(err)
		return err
	}
	if stage := reb.stages.stage.Load(); stage < rebStageFinStreams && stage != rebStageInactive {
		ack := &regularAck{rebID: reb.RebID(), daemonID: core.T.SID()}
		hdr.Opaque = ack.NewPack()
		hdr.ObjAttrs.Size = 0
		if err := reb.dm.ACK(hdr, nil, tsi); err != nil {
			nlog.Errorln(err)
			return err
		}
	}
	return nil
}

func (reb *Reb) recvRegularAck(hdr *transport.ObjHdr, unpacker *cos.ByteUnpack) error {
	var (
		rebID = reb.RebID()
		ack   = &regularAck{}
	)
	if err := unpacker.ReadAny(ack); err != nil {
		return fmt.Errorf("g[%d]: failed to unpack regular ACK: %v", rebID, err)
	}
	if ack.rebID == 0 {
		return fmt.Errorf("g[%d]: invalid g[0] ACK from %s", rebID, meta.Tname(ack.daemonID))
	}
	if ack.rebID != rebID {
		nlog.Warningln("ACK from", ack.daemonID, "[", reb.warnID(ack.rebID, ack.daemonID), "]")
		return nil
	}

	lom := core.AllocLOM(hdr.ObjName)
	if err := lom.InitBck(&hdr.Bck); err != nil {
		core.FreeLOM(lom)
		nlog.Errorln(err)
		return nil
	}

	// [NOTE]
	// - remove migrated object and copies (unless disallowed by feature flag)
	// - free pending (original) transmitted LOM
	reb.ackLomAck(lom, rebID)
	core.FreeLOM(lom)

	return nil
}

//
// EC receive
//

func (reb *Reb) recvECAck(hdr *transport.ObjHdr, unpacker *cos.ByteUnpack) (err error) {
	ack := &ecAck{}
	err = unpacker.ReadAny(ack)
	if err != nil {
		nlog.Errorf("g[%d]: failed to unpack EC ACK for %s: %v", reb.RebID(), hdr.Cname(), err)
	}
	return
}

// Receive MD update. Handling includes partially updating local information:
// only the list of daemons and the _main_ target.
func receiveMD(req *stageNtfn, hdr *transport.ObjHdr) error {
	ctMeta, err := core.NewCTFromBO(meta.CloneBck(&hdr.Bck), hdr.ObjName, fs.ECMetaCT)
	if err != nil {
		return err
	}
	md, err := ec.LoadMetadata(ctMeta.FQN())
	if err != nil {
		if cos.IsNotExist(err) {
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

	return ctMeta.Write(bytes.NewReader(mdBytes), -1, "" /*work fqn*/)
}

func (reb *Reb) receiveCT(req *stageNtfn, hdr *transport.ObjHdr, reader io.Reader) error {
	ct, err := core.NewCTFromBO(meta.CloneBck(&hdr.Bck), hdr.ObjName, fs.ECSliceCT)
	if err != nil {
		return err
	}
	md, err := detectLocalCT(req, ct)
	if err != nil {
		nlog.Errorf("%s: %v", ct.FQN(), err)
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
	req.md.FullReplica = core.T.SID()
	req.md.Daemons[core.T.SID()] = uint16(req.md.SliceID)
	if moveTo != nil {
		req.md.Daemons[moveTo.ID()] = uint16(md.SliceID)
	}
	// Save received CT to local drives
	err = reb.saveCTToDisk(req, hdr, reader)
	if err != nil {
		if errRm := cos.RemoveFile(ct.FQN()); errRm != nil {
			nlog.Errorln(err, "nested err: failed to remove", ct.FQN(), "[", errRm, "]")
		}
		if moveTo != nil {
			if errMv := os.Rename(workFQN, ct.FQN()); errMv != nil {
				nlog.Errorln(err, "nested err: failed to rename slice", ct.FQN(), "[", errMv, "]")
			}
		}
		return err
	}
	// Send local slice
	if moveTo != nil {
		req.md.SliceID = md.SliceID
		if err = reb.sendFromDisk(ct, req.md, moveTo, workFQN); err != nil {
			nlog.Errorln("failed to move slice to", moveTo, "[", err, "]")
		}
	}
	// Broadcast updated MD
	ntfnMD := stageNtfn{daemonID: core.T.SID(), stage: rebStageTraverse, rebID: reb.rebID.Load(), md: req.md, action: ecActUpdateMD}
	nodes := req.md.RemoteTargets()

	err = nil // keep the first errSend (TODO: count failures)
	for _, tsi := range nodes {
		if moveTo != nil && moveTo.ID() == tsi.ID() {
			continue
		}
		xreb := reb.xctn()
		if xreb.IsAborted() {
			break
		}
		o := transport.AllocSend()
		o.Hdr = transport.ObjHdr{ObjName: ct.ObjectName(), ObjAttrs: cmn.ObjAttrs{Size: 0}}
		o.Hdr.Bck.Copy(ct.Bck().Bucket())
		o.Hdr.Opaque = ntfnMD.NewPack(rebMsgEC)
		if errSend := reb.dm.Send(o, nil, tsi); errSend != nil && err == nil {
			// TODO: consider r.AddErr(errSend)
			err = fmt.Errorf("%s %s: failed to send updated EC MD: %v", core.T, xreb.ID(), err)
		}
	}
	return err
}

// receiving EC CT
func (reb *Reb) recvECData(hdr *transport.ObjHdr, unpacker *cos.ByteUnpack, reader io.Reader) error {
	req := &stageNtfn{}
	err := unpacker.ReadAny(req)
	if err != nil {
		err = fmt.Errorf("%s recvECData: invalid stage notification from t[%s] for %s: %v", core.T, hdr.SID, hdr.Cname(), err)
		return err
	}
	if req.rebID != reb.rebID.Load() {
		nlog.Warningf("%s: not yet started or already finished rebalancing (%d, %d) - dropping EC MD for %s from t[%s]",
			core.T, req.rebID, reb.rebID.Load(), hdr.Cname(), hdr.SID)
		return nil
	}
	if req.action == ecActUpdateMD {
		err := receiveMD(req, hdr)
		if err != nil {
			nlog.Errorf("Warning: %s g[%d]: failed to receive EC MD from t[%s] for %s: [%v]", core.T, req.rebID, hdr.SID, hdr.Cname(), err)
		}
		return nil
	}
	if err := reb.receiveCT(req, hdr, reader); err != nil {
		err = fmt.Errorf("%s g[%d]: failed to receive CT from t[%s] for %s: %v", core.T, req.rebID, hdr.SID, hdr.Cname(), err)
		nlog.Errorln(err)
		return err
	}
	return nil
}
