// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/prob"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact/xs"
)

const (
	tstBck  = "reb-test-bucket"
	tstPeer = "t-peer"
)

// a per-object failure must not truncate the mountpath walk
func TestJoggerWalkContinuesOnObjErr(t *testing.T) {
	const numObjs = 3 // NOTE: must stay under cos.Errs cap

	mi, bck := tstInitMpath(t)

	for range numObjs {
		tstCorruptedObj(t, bck, "corrupted-"+cos.GenTie()+".bin")
	}

	var (
		visits int
		rargs  = tstRargs(t)
		rj     = &rebJogger{rargs: rargs, ver: rargs.smap.Version, wg: &sync.WaitGroup{}}
	)
	// keep in sync with rebJogger.jog
	rj.opts.Mi = mi
	rj.opts.CTs = []string{fs.ObjCT}
	rj.opts.Sorted = false
	rj.opts.Callback = func(fqn string, de fs.DirEntry) error {
		if !de.IsDir() {
			visits++
		}
		return rj.visitObj(fqn, de)
	}

	stopRange := rj.walkBck(meta.NewBck(bck.Name, apc.AIS, cmn.NsGlobal))

	if visits != numObjs {
		t.Errorf("walk truncated: visited %d out of %d objects", visits, numObjs)
	}
	if stopRange {
		t.Error("walkBck stopped bmd.Range - remaining buckets would not be rebalanced")
	}
	if cnt := rargs.xreb.ErrCnt(); cnt != numObjs {
		t.Errorf("recorded %d errors, expected one per object (%d)", cnt, numObjs)
	}
	if cnt := rargs.stats.errRead.Load(); cnt != numObjs {
		t.Errorf("err-read counter: got %d, expected %d", cnt, numObjs)
	}
	if rargs.xreb.IsAborted() {
		t.Error("unexpected abort on a per-object failure")
	}
}

// an object a client removes mid-walk is a benign race (not a rebalance error)
func TestJoggerWalkSilentOnObjNought(t *testing.T) {
	const numObjs = 3

	mi, bck := tstInitMpath(t)
	for range numObjs {
		tstCorruptedObj(t, bck, "vanishing-"+cos.GenTie()+".bin")
	}

	var (
		visits int
		rargs  = tstRargs(t)
		rj     = &rebJogger{rargs: rargs, ver: rargs.smap.Version, wg: &sync.WaitGroup{}}
	)
	// keep in sync with rebJogger.jog
	rj.opts.Mi = mi
	rj.opts.CTs = []string{fs.ObjCT}
	rj.opts.Sorted = false
	rj.opts.Callback = func(fqn string, de fs.DirEntry) error {
		if !de.IsDir() {
			visits++
			if err := os.Remove(fqn); err != nil {
				t.Fatal(err)
			}
		}
		return rj.visitObj(fqn, de)
	}

	if stopRange := rj.walkBck(meta.NewBck(bck.Name, apc.AIS, cmn.NsGlobal)); stopRange {
		t.Error("walkBck stopped bmd.Range")
	}
	if visits != numObjs {
		t.Errorf("walk truncated: visited %d out of %d objects", visits, numObjs)
	}
	if cnt := rargs.xreb.ErrCnt(); cnt != 0 {
		t.Errorf("benign delete-vs-walk race recorded as %d rebalance error(s)", cnt)
	}
	if cnt := rargs.stats.errRead.Load(); cnt != 0 {
		t.Errorf("err-read counter: got %d, expected 0", cnt)
	}
}

//
// helpers
//

func tstInitMpath(t *testing.T) (*fs.Mountpath, cmn.Bck) {
	t.Helper()
	cos.InitShortID(0)

	mpath := filepath.Join(t.TempDir(), "mpath")
	if err := cos.CreateDir(mpath); err != nil {
		t.Fatal(err)
	}

	fs.NewTestMFS(mock.NewIOS())
	mi, err := fs.AddTestMpath(mpath, "daeID")
	if err != nil {
		t.Fatal(err)
	}

	bck := cmn.Bck{Name: tstBck, Provider: apc.AIS, Ns: cmn.NsGlobal}
	props := &cmn.Bprops{
		Cksum:  cmn.CksumConf{Type: cos.ChecksumNone},
		Access: apc.AccessAll,
		BID:    0xb1c2d3e4,
	}
	bmd := mock.NewBaseBownerMock(meta.NewBck(bck.Name, apc.AIS, cmn.NsGlobal, props))
	core.T = mock.NewTarget(bmd)

	return mi, bck
}

// object whose persisted lmeta size != on-disk size (corruption or tampering)
func tstCorruptedObj(t *testing.T, bck cmn.Bck, objName string) {
	t.Helper()
	const size = 1024

	lom := &core.LOM{ObjName: objName}
	if err := lom.InitCmnBck(&bck); err != nil {
		t.Fatal(err)
	}
	fh, err := cos.CreateFile(lom.FQN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.Write(make([]byte, size)); err != nil {
		t.Fatal(err)
	}
	cos.Close(fh)

	lom.SetSize(size)
	lom.SetAtimeUnix(time.Now().UnixNano())
	lom.Lock(true)
	err = lom.Persist()
	lom.Unlock(true)
	if err != nil {
		t.Fatal(err)
	}
	lom.Uncache() // force lom.Load to go to disk

	if err := os.Truncate(lom.FQN, size+1); err != nil {
		t.Fatal(err)
	}
}

func tstRargs(t *testing.T) *rargs {
	t.Helper()

	smap := &meta.Smap{
		Version: 1,
		Tmap: meta.NodeMap{
			tstPeer: &meta.Snode{DaeID: tstPeer, DaeType: apc.Target, IDDigest: 0xfeedface},
		},
	}
	xreb := &xs.Rebalance{}
	xreb.InitBase(context.Background(), cos.GenUUID(), apc.ActRebalance, nil)

	return &rargs{
		m:      &Reb{filterGFN: prob.NewDefaultFilter(), stages: newNodeStages()},
		smap:   smap,
		config: cmn.GCO.Get(),
		xreb:   xreb,
		logHdr: "test",
	}
}
