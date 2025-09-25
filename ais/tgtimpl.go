// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

func (*target) DataClient() *http.Client { return g.client.data }

func (*target) GetAllRunning(inout *core.AllRunningInOut, periodic bool) {
	xreg.GetAllRunning(inout, periodic)
}

func (t *target) Health(si *meta.Snode, timeout time.Duration, query url.Values) ([]byte, int, error) {
	return t.reqHealth(si, timeout, query, t.owner.smap.get(), false /*retry*/)
}

func (t *target) PutObject(lom *core.LOM, params *core.PutParams) error {
	debug.Assert(params.WorkTag != "" && !params.Atime.IsZero())
	workFQN := lom.GenFQN(fs.WorkCT, params.WorkTag)

	poi := allocPOI()
	{
		poi.t = t
		poi.lom = lom
		poi.config = cmn.GCO.Get()
		poi.r = params.Reader
		poi.workFQN = workFQN
		poi.atime = params.Atime.UnixNano()
		poi.xctn = params.Xact
		poi.size = params.Size
		poi.owt = params.OWT
		poi.skipEC = params.SkipEC
		poi.coldGET = params.ColdGET
	}
	if poi.owt != cmn.OwtPut {
		poi.cksumToUse = params.Cksum
	}
	_, err := poi.putObject()
	freePOI(poi)
	debug.Func(func() {
		if err == nil {
			size := lom.Lsize(true)
			// (NOTE: check callers that give us a zero)
			debug.Assertf(params.OWT == cmn.OwtTransform || params.Size <= 0 || params.Size == size, "%s: %d vs %d", lom, params.Size, size)
		}
	})
	return err
}

func (t *target) FinalizeObj(lom *core.LOM, workFQN string, xctn core.Xact, owt cmn.OWT) (ecode int, err error) {
	if err = cos.Stat(workFQN); err != nil {
		return
	}
	poi := allocPOI()
	{
		poi.t = t
		poi.atime = time.Now().UnixNano()
		poi.lom = lom
		poi.workFQN = workFQN
		poi.owt = owt
		poi.xctn = xctn
	}
	ecode, err = poi.finalize()
	freePOI(poi)
	return
}

func (t *target) EvictObject(lom *core.LOM) (ecode int, err error) {
	ecode, err = t.DeleteObject(lom, true /*evict*/)
	return
}

func (t *target) HeadObjT2T(lom *core.LOM, si *meta.Snode) bool {
	return t.headt2t(lom, si, t.owner.smap.get())
}

// CopyObject:
// - either creates a full replica of the source object (the `lom` argument)
// - or transforms the object
//
// In both cases, the result is placed at the `params`-defined destination
// in accordance with the configured destination bucket policies.
//
// Destination object _may_ have a different name and _may_ be located in a different bucket.
//
// Scenarios include (but are not limited to):
//   - if both src and dst LOMs are from local buckets the copying then takes place between AIS targets
//     (of this same cluster);
//   - if the src is located in a remote bucket, we always first make sure it is also present in
//     the AIS cluster (by performing a cold GET if need be).
//   - if the dst is cloud, we perform a regular PUT logic thus also making sure that the new
//     replica gets created in the cloud bucket of _this_ AIS cluster.
func (t *target) CopyObject(lom *core.LOM, dm *bundle.DM, params *xs.CoiParams) xs.CoiRes {
	coi := (*coi)(params)
	return coi.do(t, dm, lom)
}

func (t *target) GetCold(ctx context.Context, lom *core.LOM, xkind string, owt cmn.OWT) (ecode int, err error) {
	// lock
	switch owt {
	case cmn.OwtGetTryLock: // e.g., downloader
		if !lom.TryLock(true) {
			if cmn.Rom.FastV(4, cos.SmoduleAIS) {
				nlog.Warningln(t.String(), lom.String(), owt.String(), "is busy")
			}
			return 0, cmn.ErrSkip // TODO: must be cmn.ErrBusy
		}
	case cmn.OwtGetLock: // regular usage
		lom.Lock(true)
	default:
		// other cold-get use cases include:
		// - cmn.OwtGet, goi.getCold
		// - cmn.OwtGetPrefetchLock, xs/prefetch
		debug.Assert(false, owt.String())
		return http.StatusInternalServerError, errors.New("cold-get: invalid " + owt.String())
	}

	// cold GET
	var (
		started = mono.NanoTime()
		bp      = t.Backend(lom.Bck())
	)
	if ecode, err = bp.GetObj(ctx, lom, owt, nil /*origReq*/); err != nil {
		lom.Unlock(true)
		lom.UncacheDel()
		if cmn.IsErrFailedTo(err) {
			nlog.Warningln(err)
		} else {
			nlog.Warningln("failed to GET remote", lom.Cname(), "[", err, ecode, "]")
		}
		return ecode, err
	}

	// unlock and stats
	lom.Unlock(true)
	lat := mono.SinceNano(started)
	t.rgetstats(bp, lom.Bck().Cname(""), xkind, lom.Lsize(), lat)

	return 0, nil
}

func (t *target) rgetstats(backend core.Backend, cname, xkind string, size, lat int64) {
	vlabs := map[string]string{
		stats.VlabBucket: cname,
		stats.VlabXkind:  xkind,
	}
	t.statsT.IncWith(backend.MetricName(stats.GetCount), vlabs)
	t.statsT.AddWith(
		cos.NamedVal64{Name: backend.MetricName(stats.GetLatencyTotal), Value: lat, VarLabs: vlabs},
		cos.NamedVal64{Name: backend.MetricName(stats.GetSize), Value: size, VarLabs: vlabs},
	)
}

func (t *target) GetColdBlob(params *core.BlobParams, oa *cmn.ObjAttrs) (xctn core.Xact, err error) {
	debug.Assert(params.Lom != nil)
	debug.Assert(params.Msg != nil)
	_, xctn, err = t.blobdl(params, oa)
	return xctn, err
}

func (t *target) HeadCold(lom *core.LOM, origReq *http.Request) (oa *cmn.ObjAttrs, ecode int, err error) {
	var (
		bp    = t.Backend(lom.Bck())
		now   = mono.NanoTime()
		vlabs = map[string]string{stats.VlabBucket: lom.Bck().Cname("")}
	)
	oa, ecode, err = bp.HeadObj(context.Background(), lom, origReq)
	if err != nil {
		t.statsT.IncWith(stats.ErrHeadCount, vlabs)
	} else {
		t.statsT.IncWith(bp.MetricName(stats.HeadCount), vlabs)
		t.statsT.AddWith(
			cos.NamedVal64{Name: bp.MetricName(stats.HeadLatencyTotal), Value: mono.SinceNano(now), VarLabs: vlabs},
		)
	}
	return oa, ecode, err
}

func (t *target) GetFromNeighbor(params *core.GfnParams) (*http.Response, error) {
	var (
		lom      = params.Lom
		query    = lom.Bck().NewQuery()
		archived bool
	)
	query.Set(apc.QparamIsGFNRequest, "true")
	if params.ArchPath != "" {
		// (compare w/ t.getObject)
		debug.Assertf(!strings.HasPrefix(params.ArchPath, lom.ObjName),
			"expecting archpath _in_ archive, got (%q, %q)", params.ArchPath, lom.ObjName)
		query.Set(apc.QparamArchpath, params.ArchPath)
		archived = true
	}

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodGet
		reqArgs.Base = params.Tsi.URL(cmn.NetIntraData)
		reqArgs.Header = http.Header{
			apc.HdrCallerID:   []string{t.SID()},
			apc.HdrCallerName: []string{t.String()},
		}
		reqArgs.Path = apc.URLPathObjects.Join(lom.Bck().Name, lom.ObjName)
		reqArgs.Query = query
	}

	if params.Config == nil {
		params.Config = cmn.GCO.Get()
	}
	req, _, cancel, err := reqArgs.ReqWith(sendFileTimeout(params.Config, params.Size, archived))
	if err != nil {
		cmn.FreeHra(reqArgs)
		return nil, err
	}
	defer cancel()

	resp, err := g.client.data.Do(req)
	cmn.FreeHra(reqArgs)
	cmn.HreqFree(req)

	return resp, err
}
