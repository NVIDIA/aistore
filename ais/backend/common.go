// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

const numBackendMetricks = 12

type base struct {
	metrics  cos.StrKVs // this backend's metric names (below)
	provider string
}

func (b *base) init(snode *meta.Snode, tr stats.Tracker, startingUp bool) {
	var (
		prefix = b.provider
		regExt = true
	)
	if prefix == apc.AIS {
		prefix = apc.RemAIS
	}

	if !startingUp {
		// re-initializing or enabling at runtime
		all := tr.GetMetricNames()
		if _, ok := all[prefix+"."+stats.GetCount]; ok {
			nlog.Infoln(prefix, "backend metrics already reg-ed")
			regExt = false
		}
	}

	labels := cos.StrKVs{"backend": prefix}
	b.metrics = make(map[string]string, numBackendMetricks)

	// NOTE semantics:
	// - counts absolutely all remote GETs
	// - including those performed by tcb/tco jobs to copy or transform remote source
	// - see also: stats/common
	b.metrics[stats.GetCount] = prefix + "." + stats.GetCount
	b.metrics[stats.GetLatencyTotal] = prefix + "." + stats.GetLatencyTotal
	b.metrics[stats.GetSize] = prefix + "." + stats.GetSize

	if regExt {
		tr.RegExtMetric(snode,
			b.metrics[stats.GetCount],
			stats.KindCounter,
			&stats.Extra{
				Help:    "GET: total number of executed remote requests",
				StrName: "remote_get_count",
				Labels:  labels,
				VarLabs: stats.BckXlabs,
			},
		)
		tr.RegExtMetric(snode,
			b.metrics[stats.GetLatencyTotal],
			stats.KindTotal,
			&stats.Extra{
				Help:    "GET: total cumulative time (in nanoseconds) to execute remote requests and store, copy, or transform objects",
				StrName: "remote_get_ns_total",
				Labels:  labels,
				VarLabs: stats.BckXlabs,
			},
		)
		tr.RegExtMetric(snode,
			b.metrics[stats.GetSize],
			stats.KindSize,
			&stats.Extra{
				Help:    "GET: total cumulative size (in bytes) of all remote GET transactions",
				StrName: "remote_get_bytes_total",
				Labels:  labels,
				VarLabs: stats.BckXlabs,
			},
		)
	}

	// PUT
	b.metrics[stats.PutCount] = prefix + "." + stats.PutCount
	b.metrics[stats.PutLatencyTotal] = prefix + "." + stats.PutLatencyTotal
	b.metrics[stats.PutE2ELatencyTotal] = prefix + "." + stats.PutE2ELatencyTotal
	b.metrics[stats.PutSize] = prefix + "." + stats.PutSize

	if regExt {
		tr.RegExtMetric(snode,
			b.metrics[stats.PutCount],
			stats.KindCounter,
			&stats.Extra{
				Help:    "PUT: total number of executed remote requests to a given backend",
				StrName: "remote_put_count",
				Labels:  labels,
				VarLabs: stats.BckXlabs,
			},
		)
		tr.RegExtMetric(snode,
			b.metrics[stats.PutLatencyTotal],
			stats.KindTotal,
			&stats.Extra{
				Help:    "PUT: total cumulative time (nanoseconds) to execute remote requests",
				StrName: "remote_put_ns_total",
				Labels:  labels,
				VarLabs: stats.BckXlabs,
			},
		)
		tr.RegExtMetric(snode,
			b.metrics[stats.PutE2ELatencyTotal],
			stats.KindTotal,
			&stats.Extra{
				StrName: "remote_e2e_put_ns_total",
				Help: "PUT: total end-to-end time (nanoseconds) for servicing remote requests; " +
					"includes the time to receive PUT payload, store it in-cluster, execute the remote PUT, and finalize new in-cluster object",
				Labels:  labels,
				VarLabs: stats.BckXlabs,
			},
		)
		tr.RegExtMetric(snode,
			b.metrics[stats.PutSize],
			stats.KindSize,
			&stats.Extra{
				Help:    "PUT: total cumulative size (bytes) of all PUTs to a given remote backend",
				StrName: "remote_e2e_put_bytes_total",
				Labels:  labels,
				VarLabs: stats.BckXlabs,
			},
		)
	}

	// HEAD
	b.metrics[stats.HeadCount] = prefix + "." + stats.HeadCount
	b.metrics[stats.HeadLatencyTotal] = prefix + "." + stats.HeadLatencyTotal

	if regExt {
		tr.RegExtMetric(snode,
			b.metrics[stats.HeadCount],
			stats.KindCounter,
			&stats.Extra{
				Help:    "HEAD: total number of executed remote requests to a given backend",
				StrName: "remote_head_count",
				Labels:  labels,
				VarLabs: stats.BckVlabs,
			},
		)
		tr.RegExtMetric(snode,
			b.metrics[stats.HeadLatencyTotal],
			stats.KindTotal,
			&stats.Extra{
				Help:    "HEAD: total cumulative time (nanoseconds) to execute remote requests",
				StrName: "remote_head_ns_total",
				Labels:  labels,
				VarLabs: stats.BckVlabs,
			},
		)
	}

	// version changed out-of-band
	b.metrics[stats.VerChangeCount] = prefix + "." + stats.VerChangeCount
	b.metrics[stats.VerChangeSize] = prefix + "." + stats.VerChangeSize

	if regExt {
		tr.RegExtMetric(snode,
			b.metrics[stats.VerChangeCount],
			stats.KindCounter,
			&stats.Extra{
				Help:    "number of out-of-band updates (by a 3rd party performing remote PUTs outside this cluster)",
				StrName: "remote_ver_change_count",
				Labels:  labels,
				VarLabs: stats.BckVlabs,
			},
		)
		tr.RegExtMetric(snode,
			b.metrics[stats.VerChangeSize],
			stats.KindSize,
			&stats.Extra{
				Help:    "total cumulative size of objects that were updated out-of-band",
				StrName: "remote_ver_change_bytes_total",
				Labels:  labels,
				VarLabs: stats.BckVlabs,
			},
		)
	}
}

func (b *base) Provider() string { return b.provider }

func (b *base) MetricName(name string) string {
	out, ok := b.metrics[name]
	debug.Assert(ok && out != "", name)
	return out
}

func (b *base) CreateBucket(_ *meta.Bck) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrUnsupp("create", b.provider+" bucket")
}

func newErrInventory(provider string) error {
	return cmn.NewErrUnsupp("list "+provider+" backend objects via", "bucket inventory")
}

func (b *base) GetBucketInv(*meta.Bck, *core.LsoS3InvCtx) (int, error) {
	return 0, newErrInventory(b.provider)
}

func (b *base) ListObjectsInv(*meta.Bck, *apc.LsoMsg, *cmn.LsoRes, *core.LsoS3InvCtx) error {
	return newErrInventory(b.provider)
}

//
// multipart upload - default "not implemented" methods
//

func (b *base) StartMpt(*core.LOM, *http.Request) (string, int, error) {
	return "", http.StatusNotImplemented, cmn.NewErrUnsupp("multipart upload start", b.provider)
}

func (b *base) PutMptPart(*core.LOM, cos.ReadOpenCloser, *http.Request, string, int64, int32) (string, int, error) {
	return "", http.StatusNotImplemented, cmn.NewErrUnsupp("multipart upload part", b.provider)
}

func (b *base) CompleteMpt(*core.LOM, *http.Request, string, []byte, apc.MptCompletedParts) (string, string, int, error) {
	return "", "", http.StatusNotImplemented, cmn.NewErrUnsupp("multipart upload complete", b.provider)
}

func (b *base) AbortMpt(*core.LOM, *http.Request, string) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrUnsupp("multipart upload abort", b.provider)
}

//
// common helpers and misc
//

func fmtLsoTime(t time.Time) string { return t.Format(time.RFC3339) }
func fmtHdrTime(t time.Time) string { return t.Format(http.TimeFormat) }

func calcPageSize(pageSize, maxPageSize int64) int64 {
	debug.Assert(pageSize >= 0, pageSize)
	if pageSize == 0 {
		return maxPageSize
	}
	return min(pageSize, maxPageSize)
}

func allocPutParams(res core.GetReaderResult, owt cmn.OWT) *core.PutParams {
	params := core.AllocPutParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = res.R
		params.OWT = owt
		params.Cksum = res.ExpCksum
		params.Size = res.Size
		params.Atime = time.Now()
		params.SkipBackend = true
	}
	return params
}
