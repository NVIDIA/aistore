// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"errors"
	"os"
	"sync"
	ratomic "sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
)

const (
	tstCksum    = "0123456789abcdef" // the local object
	tstCksumAlt = "fedcba9876543210" // different content
	tstCksumNew = "2233445566778899" // local overwrite during the unlocked window
)

// the HRW peer decides: identical copy => remove; different content => keep
func TestCleanupVerifyRemove(t *testing.T) {
	tests := []struct {
		name      string
		peerCksum string
		remove    bool
	}{
		{"identical", tstCksum, true},
		{"diverged", tstCksumAlt, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mi, bck := tstInitMpath(t)
			fqn := tstPutObj(t, bck, "obj.bin", tstObjSpec{cksum: tstCksum})

			clnArgs := tstClnArgs(t)
			tstHeadT2T(t, func(*core.LOM, *meta.Snode, ...string) (*cmn.ObjectPropsV2, error) {
				return tstPeerProps(tc.peerCksum), nil
			})

			tstWalkCleanup(t, clnArgs, mi, bck)

			_, statErr := os.Stat(fqn)
			if tc.remove {
				if cnt := clnArgs.stats.removeMisplaced.Load(); cnt != 1 || !os.IsNotExist(statErr) {
					t.Errorf("expecting removal: removed=%d on-disk=%v (%s)", cnt, statErr, tstClnStats(clnArgs))
				}
			} else if cnt := clnArgs.stats.keepDiverged.Load(); cnt != 1 || statErr != nil {
				t.Errorf("expecting keep: kept-diverged=%d on-disk=%v (%s)", cnt, statErr, tstClnStats(clnArgs))
			}
		})
	}
}

// overwrite from inside the peer-HEAD callback
func TestCleanupKeepsChangedWhileUnlocked(t *testing.T) {
	const objName = "changed.bin"

	mi, bck := tstInitMpath(t)
	fqn := tstPutObj(t, bck, objName, tstObjSpec{cksum: tstCksum})

	var rewriteErr ratomic.Pointer[error]
	clnArgs := tstClnArgs(t)
	tstHeadT2T(t, func(*core.LOM, *meta.Snode, ...string) (*cmn.ObjectPropsV2, error) {
		if err := tstRewriteCksum(bck, objName, tstCksumNew); err != nil {
			rewriteErr.Store(&err)
		}
		return tstPeerProps(tstCksum), nil
	})

	tstWalkCleanup(t, clnArgs, mi, bck)

	if errp := rewriteErr.Load(); errp != nil {
		t.Fatal(*errp)
	}
	if cnt := clnArgs.stats.skipChanged.Load(); cnt != 1 {
		t.Errorf("skip-changed %d objects, expecting 1 (%s)", cnt, tstClnStats(clnArgs))
	}
	if _, err := os.Stat(fqn); err != nil {
		t.Errorf("removed an object that changed while unlocked: %v", err)
	}
}

func TestSameObj(t *testing.T) {
	attrs := func(size int64, ver, cksum string) *cmn.ObjAttrs {
		oa := &cmn.ObjAttrs{Size: size}
		oa.SetVersion(ver)
		if cksum != "" {
			oa.SetCksum(cos.ChecksumOneXxh, cksum)
		}
		return oa
	}
	tests := []struct {
		name string
		a, b *cmn.ObjAttrs
		same bool
	}{
		{"identical", attrs(1024, "1", tstCksum), attrs(1024, "1", tstCksum), true},
		{"atime-only", &cmn.ObjAttrs{Size: 1024, Atime: 1}, &cmn.ObjAttrs{Size: 1024, Atime: 2}, true},
		{"size", attrs(1024, "1", tstCksum), attrs(2048, "1", tstCksum), false},
		{"version", attrs(1024, "1", tstCksum), attrs(1024, "2", tstCksum), false},
		{"checksum", attrs(1024, "1", tstCksum), attrs(1024, "1", tstCksumAlt), false},
		{"no-checksum", attrs(1024, "1", ""), attrs(1024, "1", tstCksum), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameObj(tc.a, tc.b); got != tc.same {
				t.Errorf("sameObj(%s, %s) = %v, expecting %v", tc.a, tc.b, got, tc.same)
			}
		})
	}
}

//
// helpers
//

func tstClnArgs(t *testing.T) *clnArgs {
	t.Helper()
	return &clnArgs{rargs: *tstRargs(t)}
}

type tstTarget struct {
	*mock.TargetMock
	head func(*core.LOM, *meta.Snode, ...string) (*cmn.ObjectPropsV2, error)
}

// interface guard
var _ core.Target = (*tstTarget)(nil)

func (t *tstTarget) HeadObjT2T(lom *core.LOM, tsi *meta.Snode, reqProps ...string) (*cmn.ObjectPropsV2, error) {
	return t.head(lom, tsi, reqProps...)
}

func tstHeadT2T(t *testing.T, f func(*core.LOM, *meta.Snode, ...string) (*cmn.ObjectPropsV2, error)) {
	t.Helper()
	tm, ok := core.T.(*mock.TargetMock)
	if !ok {
		t.Fatalf("expecting *mock.TargetMock, got %T", core.T)
	}
	core.T = &tstTarget{TargetMock: tm, head: f}
	t.Cleanup(func() { core.T = tm })
}

func tstPeerProps(cksumVal string) *cmn.ObjectPropsV2 {
	op := &cmn.ObjectPropsV2{}
	op.Size = tstObjSize
	op.SetCksum(cos.ChecksumOneXxh, cksumVal)
	return op
}

// run the cleanup jogger over a single mountpath
func tstWalkCleanup(t *testing.T, clnArgs *clnArgs, mi *fs.Mountpath, bck cmn.Bck) {
	t.Helper()

	rargs := &clnArgs.rargs
	cl := &clnJogger{
		rebJogger: rebJogger{rargs: rargs, ver: rargs.smap.Version, wg: &sync.WaitGroup{}},
		clnArgs:   clnArgs,
	}
	cl.opts.Mi = mi
	cl.opts.CTs = []string{fs.ObjCT}
	cl.opts.Sorted = false
	cl.opts.Callback = cl.visitObj

	if stopRange := cl.walkBck(meta.NewBck(bck.Name, apc.AIS, cmn.NsGlobal)); stopRange {
		t.Fatal("walkBck stopped bmd.Range")
	}
	if v := clnArgs.stats.visits.Load(); v != 1 {
		t.Fatalf("visited %d objects, expecting 1", v)
	}
}

func tstClnStats(clnArgs *clnArgs) string {
	var sb cos.SB
	sb.Init(128)
	clnArgs.ctlMsg(&sb)
	return sb.String()
}

func tstRewriteCksum(bck cmn.Bck, objName, cksumVal string) error {
	lom := &core.LOM{ObjName: objName}
	if err := lom.InitCmnBck(&bck); err != nil {
		return err
	}
	if !lom.TryLock(true) {
		return errors.New("cleanup still holds the object lock across the peer HEAD")
	}
	defer lom.Unlock(true)

	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		return err
	}
	lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, cksumVal))
	return lom.Persist()
}
