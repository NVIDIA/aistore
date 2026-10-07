// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

// remote GET read deadline: protection from backends that stop (or nearly stop) sending
// (ingress counterpart of the GET write deadline - see ais/tgtobj.go initWdl;
// terminology: see cmn.XferRenewSize)
//
// - initial deadline - one window (timeout.send_file_time) - is set before the request is issued:
//   covers connection, response headers, and SDK retries (the latter honor ctx)
// - thereafter, renewed after each renewal size (cmn.XferRenewSize bytes) received
//   => minimum transfer rate 64KiB/s (same as the write deadline; see cmn.XferMinRate)
// - expired: cancel the request context w/ cmn.ErrRemoteGetTimeout cause;
//   net/http aborts the in-flight body read (closing the body would not - it waits for the read)
// - only this deadline's own cause substitutes the returned error; parent cancellation
//   (e.g., blob-download chunk timeout, xaction abort) passes through unchanged
// - substituted: 504 status (see also ais/target.go), counted once per request
//   ("err.<backend>.get.timeout.n" - see stats.GetTimeoutCount)
// - timer-based, not conn-based: connections are pooled and owned by the transport

type rdl struct {
	r         io.ReadCloser
	ctx       context.Context
	cancel    context.CancelCauseFunc
	timer     *time.Timer
	b         *base     // stats (optional in unit tests)
	bck       *meta.Bck // bucket label
	tout      time.Duration
	renewSize int64 // renewal size (cmn.XferRenewSize)
	n         int64 // bytes past the last renewal boundary (overshoot carries over; see Read)
	counted   bool
}

// returns nil (and the original context) when disabled
func newRdl(ctx context.Context, b *base, bck *meta.Bck) (*rdl, context.Context) {
	if cmn.Rom.Features().IsSet(feat.DisableGetDeadline) {
		return nil, ctx
	}
	tout := cmn.Rom.SendFile()
	if tout <= 0 {
		return nil, ctx
	}
	d := &rdl{b: b, bck: bck, tout: tout, renewSize: cmn.XferRenewSize(tout)}
	d.ctx, d.cancel = context.WithCancelCause(ctx)
	d.timer = time.AfterFunc(tout, d.expire)
	return d, d.ctx
}

func (d *rdl) expire() { d.cancel(cmn.NewErrRemoteGetTimeout(d.tout, d.renewSize)) }

func (d *rdl) expired() (err *cmn.ErrRemoteGetTimeout) {
	err, _ = errors.AsType[*cmn.ErrRemoteGetTimeout](context.Cause(d.ctx))
	return err
}

// this deadline's own expiration (and nothing else) substitutes the error - see Read and fini
func (d *rdl) timeout() *cmn.ErrRemoteGetTimeout {
	e := d.expired()

	if e == nil || d.counted {
		return e
	}

	d.counted = true
	if d.b != nil && d.b.tstats != nil {
		vlabs := map[string]string{stats.VlabBucket: d.bck.Cname("")}
		d.b.tstats.IncWith(d.b.MetricName(stats.GetTimeoutCount), vlabs)
	}
	return e
}

// deferred by GetObjReader:
// - error: release timer and context
// - success: wrap the body
func (d *rdl) fini(res *core.GetReaderResult) {
	if d == nil {
		return
	}
	if res.Err == nil && res.R == nil {
		// unlikely (but never wrap a nil reader)
		res.Err = errors.New("remote GET: no error and no reader")
		debug.AssertNoErr(res.Err)
	}

	if res.Err == nil {
		d.r = res.R
		res.R = d
		return
	}

	d.timer.Stop()
	d.cancel(nil) // (the first cause wins - see expired)
	if res.R != nil {
		res.R.Close()
		res.R = nil
	}
	if e := d.timeout(); e != nil {
		res.Err = e
		res.ErrCode = http.StatusGatewayTimeout
	}
}

func (d *rdl) Read(p []byte) (n int, err error) {
	n, err = d.r.Read(p)
	if d.n += int64(n); d.n >= d.renewSize {
		d.n %= d.renewSize // keep the bytes past the boundary: renewal k at cumulative k*renewSize
		if d.ctx.Err() == nil {
			d.timer.Reset(d.tout) // (not after expiry or cancellation: nothing left to protect)
		}
	}
	if err != nil && err != io.EOF {
		if e := d.timeout(); e != nil {
			err = e
		}
	}
	return n, err
}

// cancel first: some readers wait for in-flight requests on Close (e.g., OCI MPD children)
func (d *rdl) Close() error {
	d.timer.Stop()
	d.cancel(nil)
	return d.r.Close()
}
