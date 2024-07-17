// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

type base struct {
	provider string
	metrics  map[string]string // this backend's metric names (below)
}

func (b *base) init(snode *meta.Snode, tstats stats.Tracker) {
	prefix := b.provider
	if prefix == apc.AIS {
		prefix = apc.RemAIS
	}
	b.metrics = make(map[string]string, 8)
	b.metrics[stats.GetCount] = prefix + "." + stats.GetCount
	b.metrics[stats.GetLatencyTotal] = prefix + "." + stats.GetLatencyTotal
	b.metrics[stats.GetE2ELatencyTotal] = prefix + "." + stats.GetE2ELatencyTotal
	b.metrics[stats.GetSize] = prefix + "." + stats.GetSize

	tstats.RegExtMetric(snode, b.metrics[stats.GetCount], stats.KindCounter)
	tstats.RegExtMetric(snode, b.metrics[stats.GetLatencyTotal], stats.KindTotal)
	tstats.RegExtMetric(snode, b.metrics[stats.GetE2ELatencyTotal], stats.KindTotal)
	tstats.RegExtMetric(snode, b.metrics[stats.GetSize], stats.KindSize)

	b.metrics[stats.PutCount] = prefix + "." + stats.PutCount
	b.metrics[stats.PutLatencyTotal] = prefix + "." + stats.PutLatencyTotal
	b.metrics[stats.PutE2ELatencyTotal] = prefix + "." + stats.PutE2ELatencyTotal
	b.metrics[stats.PutSize] = prefix + "." + stats.PutSize

	tstats.RegExtMetric(snode, b.metrics[stats.PutCount], stats.KindCounter)
	tstats.RegExtMetric(snode, b.metrics[stats.PutLatencyTotal], stats.KindTotal)
	tstats.RegExtMetric(snode, b.metrics[stats.PutE2ELatencyTotal], stats.KindTotal)
	tstats.RegExtMetric(snode, b.metrics[stats.PutSize], stats.KindSize)

	b.metrics[stats.VerChangeCount] = prefix + "." + stats.VerChangeCount
	b.metrics[stats.VerChangeSize] = prefix + "." + stats.VerChangeSize

	tstats.RegExtMetric(snode, b.metrics[stats.VerChangeCount], stats.KindCounter)
	tstats.RegExtMetric(snode, b.metrics[stats.VerChangeSize], stats.KindSize)
}

func (b *base) Provider() string              { return b.provider }
func (b *base) MetricName(name string) string { return b.metrics[name] }

func (b *base) CreateBucket(_ *meta.Bck) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrUnsupp("create", b.provider+" bucket")
}

func newErrInventory(provider string) error {
	return cmn.NewErrUnsupp("list "+provider+" backend objects via", "bucket inventory")
}

func (b *base) GetBucketInv(*meta.Bck, *core.LsoInvCtx) (int, error) {
	return 0, newErrInventory(b.provider)
}

func (b *base) ListObjectsInv(*meta.Bck, *apc.LsoMsg, *cmn.LsoRes, *core.LsoInvCtx) error {
	return newErrInventory(b.provider)
}

//
// common helpers and misc
//

func fmtTime(t time.Time) string { return t.Format(time.RFC3339) }

func calcPageSize(pageSize, maxPageSize int64) int64 {
	debug.Assert(pageSize >= 0, pageSize)
	if pageSize == 0 {
		return maxPageSize
	}
	return min(pageSize, maxPageSize)
}

//nolint:deadcode,unused // used by dummy backends
func newErrInitBackend(provider string) error { return &cmn.ErrInitBackend{Provider: provider} }

func allocPutParams(res core.GetReaderResult, owt cmn.OWT) *core.PutParams {
	params := core.AllocPutParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = res.R
		params.OWT = owt
		params.Cksum = res.ExpCksum
		params.Size = res.Size
		params.Atime = time.Now()
		params.ColdGET = true
	}
	return params
}
