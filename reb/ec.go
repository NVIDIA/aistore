// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
)

// High level overview of how EC rebalance works.
// 1. EC traverses only metafile(%mt) directories. A jogger per mountpath.
// 2. A jogger skips a metafile if:
//    - its `FullReplica` is not the local target ID
//    - its `FullReplica` equals the local target ID and HRW chooses local target
// 3. Otherwise, a jogger calculates a correct target using HRW and moves CT there
// 4. A target on receiving:
// 4.1. Preparation:
//      - Update metadata: fix `Daemons` and `FullReplica` fields
// 4.1. If the target has another CT of the same object and generation:
//      - move local CT to a working directory
// 4.2. If the target contains another CT of the same object and generation:
//      - save sent CT and metafile
// 4.3 If anything was moved to working directory at step 4.1:
//      - select a target that has no valid CT of the object
//      - moves the local CT to the selected target
// 4.3. Finalization:
//      - broadcast new metadata to all targets in `Daemons` field for them to
//        update their metafiles. Targets do not overwrite their metafiles with a new
//        one. They update only `Daemons` and `FullReplica` fields.

func (reb *Reb) runECjoggers() {
	var (
		wg             = &sync.WaitGroup{}
		availablePaths = fs.GetAvail()
		cfg            = cmn.GCO.Get()
		b              = reb.xctn().Bck()
	)
	for _, mpathInfo := range availablePaths {
		bck := cmn.Bck{Provider: cmn.ProviderAIS}
		if b != nil {
			bck = cmn.Bck{Name: b.Name, Provider: cmn.ProviderAIS, Ns: b.Ns}
		}
		wg.Add(1)
		go reb.jogEC(mpathInfo, bck, wg)
	}
	for _, provider := range cfg.Backend.Providers {
		for _, mpathInfo := range availablePaths {
			bck := cmn.Bck{Provider: provider.Name}
			if b != nil {
				bck = cmn.Bck{Name: bck.Name, Provider: provider.Name, Ns: bck.Ns}
			}
			wg.Add(1)
			go reb.jogEC(mpathInfo, bck, wg)
		}
	}
	wg.Wait()
}

// mountpath walker - walks through files in /meta/ directory
func (reb *Reb) jogEC(mpathInfo *fs.MountpathInfo, bck cmn.Bck, wg *sync.WaitGroup) {
	defer wg.Done()
	opts := &fs.Options{
		Mi:       mpathInfo,
		Bck:      bck,
		CTs:      []string{fs.ECMetaType},
		Callback: reb.walkEC,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		xreb := reb.xctn()
		if xreb.IsAborted() || xreb.Finished() {
			glog.Infof("aborting traversal")
		} else {
			glog.Warningf("failed to traverse, err: %v", err)
		}
	}
}

// Sends local CT along with EC metadata to default target.
// The CT is on a local drive and not loaded into SGL. Just read and send.
func (reb *Reb) sendFromDisk(ct *cluster.CT, meta *ec.Metadata, target *cluster.Snode, workFQN ...string) (err error) {
	var (
		lom    *cluster.LOM
		roc    cos.ReadOpenCloser
		fqn    = ct.FQN()
		action = uint32(rebActRebCT)
	)
	debug.Assert(meta != nil)
	if len(workFQN) != 0 {
		fqn = workFQN[0]
		action = rebActMoveCT
	}
	// FIXME: We should unify acquiring a reader for LOM and CT. Both should be
	//  locked and handled similarly.
	if ct.ContentType() == fs.ObjectType {
		lom = cluster.AllocLOM(ct.ObjectName())
		if err = lom.Init(ct.Bck().Bck); err != nil {
			cluster.FreeLOM(lom)
			return
		}
		lom.Lock(false)
		if err = lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			lom.Unlock(false)
			cluster.FreeLOM(lom)
			return
		}
	} else {
		lom = nil // sending slice; TODO: rlock
	}
	// open
	if roc, err = cos.NewFileHandle(fqn); err != nil {
		if lom != nil {
			lom.Unlock(false)
			cluster.FreeLOM(lom)
		}
		return
	}
	if lom != nil {
		roc = cos.NewDeferROC(roc, func() {
			lom.Unlock(false)
			cluster.FreeLOM(lom)
		})
	}
	// transmit
	req := pushReq{daemonID: reb.t.SID(), stage: rebStageTraverse, rebID: reb.rebID.Load(), md: meta, action: action}
	o := transport.AllocSend()
	o.Hdr = transport.ObjHdr{
		Bck:      ct.Bck().Bck,
		ObjName:  ct.ObjectName(),
		ObjAttrs: cmn.ObjAttrs{Size: meta.Size},
	}
	if lom != nil {
		o.Hdr.ObjAttrs.CopyFrom(lom.ObjAttrs())
	}
	if meta.SliceID != 0 {
		o.Hdr.ObjAttrs.Size = ec.SliceSize(meta.Size, meta.Data)
	}
	reb.onAir.Inc()
	o.Hdr.Opaque = req.NewPack(rebMsgEC)
	o.Callback = reb.transportECCB
	if err = reb.dm.Send(o, roc, target); err != nil {
		err = fmt.Errorf("failed to send slices to nodes [%s..]: %v", target.ID(), err)
		return
	}
	xreb := reb.xctn()
	xreb.OutObjsAdd(1, o.Hdr.ObjAttrs.Size)
	return
}

func (reb *Reb) transportECCB(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
	reb.onAir.Dec()
}

// Saves received CT to a local drive if needed:
//   1. Full object/replica is received
//   2. A CT is received and this target is not the default target (it
//      means that the CTs came from default target after EC had been rebuilt)
func (reb *Reb) saveCTToDisk(req *pushReq, hdr *transport.ObjHdr, data io.Reader) error {
	cos.Assert(req.md != nil)
	var (
		err error
		bck = cluster.NewBckEmbed(hdr.Bck)
	)
	if err := bck.Init(reb.t.Bowner()); err != nil {
		return err
	}
	md := req.md.NewPack()
	if req.md.SliceID != 0 {
		args := &ec.WriteArgs{Reader: data, MD: md}
		err = ec.WriteSliceAndMeta(reb.t, hdr, args)
	} else {
		var lom *cluster.LOM
		lom, err = cluster.AllocLomFromHdr(hdr)
		if err == nil {
			args := &ec.WriteArgs{Reader: data, MD: md, Cksum: hdr.ObjAttrs.Cksum}
			err = ec.WriteReplicaAndMeta(reb.t, lom, args)
		}
		cluster.FreeLOM(lom)
	}
	return err
}

// Used when slice conflict is detected: a target receives a new slice and it already
// has a slice of the same generation with different ID
func (*Reb) renameAsWorkFile(ct *cluster.CT) (string, error) {
	fqn := ct.Make(fs.WorkfileType)
	// Using os.Rename is safe as both CT and Workfile on the same mountpath
	if err := os.Rename(ct.FQN(), fqn); err != nil {
		return "", err
	}
	return fqn, nil
}

// Find a target that has either obsolete slice or no slice of an object.
// Used when in slice conflict: this target is a "main" one and receieves a full
// replica but this target contains a slice of the object. So, the existing slice
// send to any free target.
func (reb *Reb) findEmptyTarget(md *ec.Metadata, ct *cluster.CT, sender string) (*cluster.Snode, error) {
	sliceCnt := md.Data + md.Parity + 2
	hrwList, err := cluster.HrwTargetList(ct.Bck().MakeUname(ct.ObjectName()), reb.t.Sowner().Get(), sliceCnt)
	if err != nil {
		return nil, err
	}
	for _, tsi := range hrwList {
		if tsi.ID() == sender || tsi.ID() == reb.t.Snode().ID() {
			continue
		}
		remoteMD, err := ec.RequestECMeta(ct.Bucket(), ct.ObjectName(), tsi, reb.t.DataClient())
		if remoteMD != nil && remoteMD.Generation < md.Generation {
			return tsi, nil
		}
		if remoteMD != nil && remoteMD.Generation == md.Generation {
			_, ok := md.Daemons[tsi.ID()]
			if !ok {
				if glog.FastV(4, glog.SmoduleReb) {
					glog.Infof("%s: %s[%d] not found - overwrite with new slice %d",
						tsi.StringEx(), ct.ObjectName(), remoteMD.SliceID, md.SliceID)
				}
				return tsi, nil
			}
		}
		if err != nil && cmn.IsObjNotExist(err) {
			return tsi, nil
		}
		if err != nil {
			glog.Errorf("Failed to read metadata from %s: %v", tsi.StringEx(), err)
		}
	}
	return nil, errors.New("no free target")
}

// Check if this target has a metadata for the received CT
func (reb *Reb) detectLocalCT(req *pushReq, ct *cluster.CT) (*ec.Metadata, error) {
	if req.action == rebActMoveCT {
		// internal CT move after slice conflict - save always
		return nil, nil
	}
	if _, ok := req.md.Daemons[reb.t.Snode().ID()]; !ok {
		return nil, nil
	}
	mdCT, err := cluster.NewCTFromBO(ct.Bck().Bck, ct.ObjectName(), reb.t.Bowner(), fs.ECMetaType)
	if err != nil {
		return nil, err
	}
	locMD, err := ec.LoadMetadata(mdCT.FQN())
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	return locMD, err
}

// When a target receives a slice and the target has a slice with different ID:
// - move slice to a workfile directory
// - return Snode that must receive the local slice, and workfile path
// - the caller saves received CT to local drives, and then sends workfile
func (reb *Reb) renameLocalCT(req *pushReq, ct *cluster.CT, md *ec.Metadata) (
	workFQN string, moveTo *cluster.Snode, err error) {
	if md == nil || req.action == rebActMoveCT {
		return
	}
	if md.SliceID == 0 || md.SliceID == req.md.SliceID || req.md.Generation != md.Generation {
		return
	}
	if workFQN, err = reb.renameAsWorkFile(ct); err != nil {
		return
	}
	if moveTo, err = reb.findEmptyTarget(md, ct, req.daemonID); err != nil {
		if errMv := os.Rename(workFQN, ct.FQN()); errMv != nil {
			glog.Errorf("Error restoring slice: %v", errMv)
		}
	}
	return
}

func (reb *Reb) walkEC(fqn string, de fs.DirEntry) (err error) {
	xreb := reb.xctn()
	if err := xreb.Aborted(); err != nil {
		// notify `dir.Walk` to stop iterations
		return cmn.NewErrAborted(xreb.Name(), "walk-ec", err)
	}

	if de.IsDir() {
		return nil
	}

	ct, err := cluster.NewCTFromFQN(fqn, reb.t.Bowner())
	if err != nil {
		return nil
	}
	// do not touch directories for buckets with EC disabled (for now)
	if !ct.Bck().Props.EC.Enabled {
		return filepath.SkipDir
	}

	md, err := ec.LoadMetadata(fqn)
	if err != nil {
		glog.Warningf("failed to load metadata from %q: %v", fqn, err)
		return nil
	}

	// Skip a CT if this target is not the 'main' one
	if md.FullReplica != reb.t.Snode().ID() {
		return nil
	}

	hrwTarget, err := cluster.HrwTarget(ct.Bck().MakeUname(ct.ObjectName()), reb.t.Sowner().Get())
	if err != nil || hrwTarget.ID() == reb.t.Snode().ID() {
		return err
	}

	// check if both slice/replica and metafile exist
	isReplica := md.SliceID == 0
	var fileFQN string
	if isReplica {
		fileFQN = ct.Make(fs.ObjectType)
	} else {
		fileFQN = ct.Make(fs.ECSliceType)
	}
	if err := fs.Access(fileFQN); err != nil {
		glog.Warningf("%s no CT for metadata[%d]: %s", reb.t.Snode(), md.SliceID, fileFQN)
		return nil
	}

	ct, err = cluster.NewCTFromFQN(fileFQN, reb.t.Bowner())
	if err != nil {
		return nil
	}
	return reb.sendFromDisk(ct, md, hrwTarget)
}
