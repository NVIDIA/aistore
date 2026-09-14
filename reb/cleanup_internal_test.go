// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"os"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
)

const tstCksum = "0123456789abcdef"

// the HRW peer decides: identical copy => remove; different content => keep
func TestCleanupVerifyRemove(t *testing.T) {
	tests := []struct {
		name      string
		peerCksum string
		remove    bool
	}{
		{"identical", tstCksum, true},
		{"diverged", "fedcba9876543210", false},
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
