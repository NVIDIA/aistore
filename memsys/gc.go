// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/sys"
)

func (r *MMSA) freeMemToOS(mingc int64, p int, forces ...bool) {
	var (
		togc  = r.toGC.Load()
		force bool
	)
	if len(forces) > 0 {
		force = forces[0]
	}
	if p >= PressureExtreme {
		force = true
	}
	if !force && p <= PressureLow {
		togc := r.toGC.Load()
		if togc < mingc {
			return // too little to bother
		}
	}

	var (
		load     = sys.MaxLoad()
		highLoad = sys.HighLoadWM()
	)
	if !force {
		// too busy and not too "pressured"
		switch {
		case load >= float64(highLoad) && p <= PressureHigh:
			return
		case load >= max(float64(highLoad>>1), 1.0) && p <= PressureModerate:
			return
		}
	}

	//
	// NOTE: calling an expensive serialized goroutine
	//
	if started := oom.FreeToOS(force); !started {
		return
	}
	nlog.Warningln(r.String(), "free mem to OS [", r.pressure2S(p), force, cos.ToSizeIEC(togc, 1), load, "]")
	r.toGC.Store(0)
}
