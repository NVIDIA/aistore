// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"io"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"

	jsoniter "github.com/json-iterator/go"
)

// proxy:
// - stream apc.MossReq.In reading only the fields needed to authorize access to bucket(s)
// - return distinct referenced buckets (apc.AceGET required)
// - when colocation is requested, return the (optimally selected) DT

// local context to parse apc.MossReq
type mossScanCtx struct {
	dfltBck *meta.Bck
	smap    *smapX
	nat     int
	coloc   apc.ColocLevel

	// output
	bcks []*meta.Bck

	// state during parsing
	seen   cos.StrSet
	scores map[string]int // nil while all entries HRW to single target
	single string
	dtid   string
	score  int
	num    int
	err    error
}

func mossScan(iter *jsoniter.Iterator, dfltBck *meta.Bck, smap *smapX, nat int, coloc apc.ColocLevel) (bcks []*meta.Bck, dt *meta.Snode, _ error) {
	ctx := &mossScanCtx{
		dfltBck: dfltBck,
		smap:    smap,
		nat:     nat,
		coloc:   coloc,
		seen:    make(cos.StrSet, 4),
	}
	iter.ReadObjectCB(ctx.onObject)

	if ctx.err != nil {
		return nil, nil, ctx.err
	}
	if iter.Error != nil && iter.Error != io.EOF {
		return nil, nil, iter.Error
	}
	if ctx.num == 0 {
		return nil, nil, errors.New(apc.Moss + ": empty input")
	}
	if coloc > apc.ColocNone {
		return ctx.bcks, smap.GetTarget(ctx.dtid) /*DT*/, nil
	}
	return ctx.bcks, nil, nil
}

/////////////////
// mossScanCtx //
/////////////////

// jsoniter callback: top-level object - looking for "in" field
func (ctx *mossScanCtx) onObject(iter *jsoniter.Iterator, key string) bool {
	if key != "in" {
		iter.Skip()
		return true
	}
	return iter.ReadArrayCB(ctx.onEntry)
}

// jsoniter callback: single entry in the "in" array
func (ctx *mossScanCtx) onEntry(iter *jsoniter.Iterator) bool {
	var in apc.MossIn

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "objname":
			in.ObjName = iter.ReadString()
		case "bucket":
			in.Bucket = iter.ReadString()
		case "provider":
			in.Provider = iter.ReadString()
		case "uname":
			in.Uname = iter.ReadString()
		default:
			iter.Skip()
		}
		return true
	})

	ctx.num++

	bck, err := _mossInBck(&in, ctx.dfltBck)
	if err != nil {
		ctx.err = err
		return false
	}

	// (a) distinct referenced buckets - for authorization
	uname := bck.MakeUname("")
	if s := cos.UnsafeS(uname); !ctx.seen.Contains(s) {
		ctx.seen.Set(s)
		//
		// TODO: consider sort.Search (and copy/append) to return deterministically ordered
		//
		ctx.bcks = append(ctx.bcks, bck)
	}

	// (b) DT selection - when colocation is on
	if ctx.coloc >= apc.ColocOne {
		ctx.selectDT(&in, bck)
		if ctx.err != nil {
			return false
		}
	}

	return true
}

// resolve a MOSS request item to its bucket: the per-item bucket if specified,
// otherwise the request's default (path) bucket.
func _mossInBck(in *apc.MossIn, dfltBck *meta.Bck) (*meta.Bck, error) {
	bck, err := meta.BckFromUBP(in.Uname, in.Bucket, in.Provider)
	if err != nil {
		return nil, err
	}
	if bck == nil {
		bck = dfltBck
	}
	if bck == nil {
		return nil, fmt.Errorf("missing bucket specification for object %q", in.ObjName)
	}
	return bck, nil
}

// update DT selection state for a single entry
func (ctx *mossScanCtx) selectDT(in *apc.MossIn, bck *meta.Bck) {
	tsi, err := ctx.smap.HrwName2T(bck.MakeUname(in.ObjName))
	if err != nil {
		ctx.err = err
		return
	}

	tid := tsi.ID()
	switch {
	case ctx.single == "":
		ctx.single, ctx.dtid, ctx.score = tid, tid, 1
	case ctx.single == tid && ctx.scores == nil:
		ctx.score++
	default:
		if ctx.scores == nil {
			ctx.scores = make(map[string]int, ctx.nat)
			ctx.scores[ctx.single] = ctx.score
		}
		ctx.scores[tid]++
		if ctx.score < ctx.scores[tid] {
			ctx.score, ctx.dtid = ctx.scores[tid], tid
		}
	}
}
