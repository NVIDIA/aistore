// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
)

// remote GET read deadline: protection from backends that stop sending
// (ingress counterpart of the GET write deadline - see ais/tgtobj.go initWdl)
//
// - initial deadline (timeout.send_file_time) is set before the request is issued:
//   covers connection, response headers, and SDK retries (the latter honor ctx)
// - thereafter, renewed once per cmn.XferChunkSize bytes received
//   => implied minimum rate 64KiB/s (same as GET write deadline; see cmn.XferMinRate)
// - expired: cancel the request context w/ errReadTimeout cause;
//   net/http aborts the in-flight body read (closing the body would not - it waits for the read)
// - only this deadline's own cause substitutes the returned error; parent cancellation
//   (e.g., blob-download chunk timeout, xaction abort) passes through unchanged
// - intentionally not net.Error/Timeout(): must not be mistaken for a (benign) client timeout
// - timer-based, not conn-based: connections are pooled and owned by the transport

type (
	rdl struct {
		r      io.ReadCloser
		ctx    context.Context
		cancel context.CancelCauseFunc
		timer  *time.Timer
		tout   time.Duration
		csize  int64 // renewal chunk
		n      int64 // bytes since last renewal
	}
	errReadTimeout struct {
		tout  time.Duration
		csize int64
	}
)

func (e *errReadTimeout) Error() string {
	return fmt.Sprintf("remote backend read timeout: less than %s in %v (timeout.send_file_time)",
		cos.IEC(e.csize, 1), e.tout)
}

// returns nil (and the original context) when disabled
func newRdl(ctx context.Context) (*rdl, context.Context) {
	if cmn.Rom.Features().IsSet(feat.DisableGetDeadline) {
		return nil, ctx
	}
	tout := cmn.Rom.SendFile()
	if tout <= 0 {
		return nil, ctx
	}
	d := &rdl{tout: tout, csize: cmn.XferChunkSize(tout)}
	d.ctx, d.cancel = context.WithCancelCause(ctx)
	d.timer = time.AfterFunc(tout, d.expire)
	return d, d.ctx
}

func (d *rdl) expire() { d.cancel(&errReadTimeout{tout: d.tout, csize: d.csize}) }

func (d *rdl) expired() (err *errReadTimeout) {
	err, _ = errors.AsType[*errReadTimeout](context.Cause(d.ctx))
	return err
}

// deferred by GetObjReader:
// - error: release timer and context
// - success: wrap the body
func (d *rdl) fini(res *core.GetReaderResult) {
	if d == nil {
		return
	}
	if res.Err != nil {
		d.timer.Stop()
		d.cancel(nil) // (the first cause wins - see expired)
		if res.R != nil {
			res.R.Close()
			res.R = nil
		}
		if e := d.expired(); e != nil {
			res.Err = fmt.Errorf("%w: %v", e, res.Err)
		}
		return
	}
	d.r = res.R
	res.R = d
}

func (d *rdl) Read(p []byte) (n int, err error) {
	n, err = d.r.Read(p)
	if d.n += int64(n); d.n >= d.csize {
		d.n = 0
		d.timer.Reset(d.tout)
	}
	if err != nil && err != io.EOF {
		if e := d.expired(); e != nil {
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
