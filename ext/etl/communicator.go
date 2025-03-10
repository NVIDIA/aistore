// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	CommStats interface {
		ObjCount() int64
		InBytes() int64
		OutBytes() int64
	}

	// Communicator is responsible for managing communications with local ETL container.
	// It listens to cluster membership changes and terminates ETL container, if need be.
	Communicator interface {
		meta.Slistener

		Name() string
		Xact() core.Xact
		PodName() string
		SvcName() string

		String() string

		// InlineTransform uses one of the two ETL container endpoints:
		//  - Method "PUT", Path "/"
		//  - Method "GET", Path "/bucket/object"
		InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, targs string) (int, error)

		// OfflineTransform is driven by `OfflineDP` to provide offline transformation, as it were
		// Implementations include:
		// - pushComm
		// - redirectComm
		// - revProxyComm
		// See also, and separately: on-the-fly transformation as part of a user (e.g. training model) GET request handling
		OfflineTransform(lom *core.LOM, timeout time.Duration) (cos.ReadCloseSizer, int, error)

		Stop()

		CommStats
	}

	baseComm struct {
		listener meta.Slistener
		boot     *etlBootstrapper
		pw       *podWatcher
	}
	pushComm struct {
		baseComm
		command []string
	}
	redirectComm struct {
		baseComm
	}
	revProxyComm struct {
		baseComm
		rp *httputil.ReverseProxy
	}

	// TODO: Generalize and move to `cos` package
	cbWriter struct {
		w       io.Writer
		writeCb func(int)
	}
)

// interface guard
var (
	_ Communicator = (*pushComm)(nil)
	_ Communicator = (*redirectComm)(nil)
	_ Communicator = (*revProxyComm)(nil)

	_ io.Writer = (*cbWriter)(nil)
)

//////////////
// baseComm //
//////////////

func newCommunicator(listener meta.Slistener, boot *etlBootstrapper, pw *podWatcher) Communicator {
	switch boot.msg.CommTypeX {
	case Hpush, HpushStdin:
		pc := &pushComm{}
		pc.listener, pc.boot, pc.pw = listener, boot, pw
		if boot.msg.CommTypeX == HpushStdin { // io://
			pc.command = boot.originalCommand
		}
		return pc
	case Hpull:
		rc := &redirectComm{}
		rc.listener, rc.boot, rc.pw = listener, boot, pw
		return rc
	case Hrev:
		rp := &revProxyComm{}
		rp.listener, rp.boot, rp.pw = listener, boot, pw

		transformerURL, err := url.Parse(boot.uri)
		debug.AssertNoErr(err)
		revProxy := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// Replacing the `req.URL` host with ETL container host
				req.URL.Scheme = transformerURL.Scheme
				req.URL.Host = transformerURL.Host
				req.URL.RawQuery = pruneQuery(req.URL.RawQuery)
				if _, ok := req.Header["User-Agent"]; !ok {
					// Explicitly disable `User-Agent` so it's not set to default value.
					req.Header.Set("User-Agent", "")
				}
			},
		}
		rp.rp = revProxy
		return rp
	}

	debug.Assert(false, "unknown comm-type '"+boot.msg.CommTypeX+"'")
	return nil
}

func (c *baseComm) Name() string    { return c.boot.originalPodName }
func (c *baseComm) PodName() string { return c.boot.pod.Name }
func (c *baseComm) SvcName() string { return c.boot.pod.Name /*same as pod name*/ }

func (c *baseComm) ListenSmapChanged() { c.listener.ListenSmapChanged() }

func (c *baseComm) String() string {
	return fmt.Sprintf("%s[%s]-%s", c.boot.originalPodName, c.boot.xctn.ID(), c.boot.msg.CommTypeX)
}

func (c *baseComm) Xact() core.Xact { return c.boot.xctn }
func (c *baseComm) ObjCount() int64 { return c.boot.xctn.Objs() }
func (c *baseComm) InBytes() int64  { return c.boot.xctn.InBytes() }
func (c *baseComm) OutBytes() int64 { return c.boot.xctn.OutBytes() }

func (c *baseComm) Stop() {
	c.boot.xctn.Finish()
	if c.pw != nil {
		c.pw.stop(false)
	}
}

func (c *baseComm) getWithTimeout(lom *core.LOM, url string, timeout time.Duration) (r cos.ReadCloseSizer, ecode int, err error) {
	if err := c.boot.xctn.AbortErr(); err != nil {
		return nil, 0, err
	}

	var (
		req    *http.Request
		resp   *http.Response
		cancel func()
	)
	if timeout != 0 {
		var ctx context.Context
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	} else {
		req, err = http.NewRequest(http.MethodGet, url, http.NoBody)
	}
	if err == nil {
		resp, err = core.T.DataClient().Do(req) //nolint:bodyclose // Closed by the caller.
	}
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if resp != nil {
			ecode = resp.StatusCode
		}
		return nil, ecode, err
	}

	// if the transformed object's size is unknown, fall back to the original source size.
	size := resp.ContentLength
	if size == cos.ContentLengthUnknown {
		size = lom.Lsize()
	}

	return cos.NewReaderWithArgs(cos.ReaderArgs{
		R:    resp.Body,
		Size: size,
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
		},
	}), 0, nil
}

//////////////
// pushComm: implements (Hpush | HpushStdin)
//////////////

func (pc *pushComm) doRequest(lom *core.LOM, timeout time.Duration, targs string) (r cos.ReadCloseSizer, ecode int, err error) {
	if err := lom.InitBck(lom.Bucket()); err != nil {
		return nil, 0, err
	}

	lom.Lock(false)
	r, ecode, err = pc.do(lom, timeout, targs)
	lom.Unlock(false)

	if err != nil && cos.IsNotExist(err, ecode) && lom.Bucket().IsRemote() {
		ecode, err = core.T.GetCold(context.Background(), lom, pc.boot.xctn.Kind(), cmn.OwtGetLock)
		if err != nil {
			return nil, ecode, err
		}
		lom.Lock(false)
		r, ecode, err = pc.do(lom, timeout, targs)
		lom.Unlock(false)
	}
	return r, ecode, err
}

func (pc *pushComm) do(lom *core.LOM, timeout time.Duration, targs string) (_ cos.ReadCloseSizer, ecode int, err error) {
	var (
		body   io.ReadCloser
		cancel func()
		req    *http.Request
		resp   *http.Response
		u      string
	)
	if e := pc.boot.xctn.AbortErr(); e != nil {
		return nil, 0, e
	}
	if e := lom.Load(false /*cache it*/, true /*locked*/); e != nil {
		return nil, 0, e
	}

	switch pc.boot.msg.ArgTypeX {
	case ArgTypeDefault, ArgTypeURL:
		// [TODO] to remove the following assert (and the corresponding limitation):
		// - container must be ready to receive complete bucket name including namespace
		// - see `bck.AddToQuery` and api/bucket.go for numerous examples
		debug.Assert(lom.Bck().Ns.IsGlobal(), lom.Bck().Cname(""), " - bucket with namespace")
		u = pc.boot.uri + "/" + lom.Bck().Name + "/" + lom.ObjName

		fh, e := cos.NewFileHandle(lom.FQN)
		if e != nil {
			return nil, 0, e
		}
		body = fh
	case ArgTypeFQN:
		body = http.NoBody
		u = cos.JoinPath(pc.boot.uri, url.PathEscape(lom.FQN)) // compare w/ rc.redirectURL()
	default:
		e := pc.boot.msg.errInvalidArg()
		debug.AssertNoErr(e) // is validated at construction time
		nlog.Errorln(e)
	}

	if targs != "" {
		q := url.Values{apc.QparamETLTransformArgs: []string{targs}}
		u = cos.JoinQuery(u, q)
	}

	if timeout != 0 {
		var ctx context.Context
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, u, body)
	} else {
		req, err = http.NewRequest(http.MethodPut, u, body)
	}
	if err != nil {
		cos.Close(body)
		goto finish
	}

	if len(pc.command) != 0 {
		// HpushStdin case
		q := req.URL.Query()
		q["command"] = []string{"bash", "-c", strings.Join(pc.command, " ")}
		req.URL.RawQuery = q.Encode()
	}
	req.ContentLength = lom.Lsize()
	req.Header.Set(cos.HdrContentType, cos.ContentBinary)

	// do
	resp, err = core.T.DataClient().Do(req) //nolint:bodyclose // Closed by the caller.

finish:
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if resp != nil {
			ecode = resp.StatusCode
		}
		return nil, ecode, err
	}

	// if the transformed object's size is unknown, fall back to the original source size.
	size := resp.ContentLength
	if size == cos.ContentLengthUnknown {
		size = lom.Lsize()
	}

	rargs := cos.ReaderArgs{
		R:    resp.Body,
		Size: size,
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
		},
	}
	return cos.NewReaderWithArgs(rargs), 0, nil
}

func (pc *pushComm) InlineTransform(w http.ResponseWriter, _ *http.Request, lom *core.LOM, targs string) (int, error) {
	r, ecode, err := pc.doRequest(lom, 0 /*timeout*/, targs)
	if err != nil {
		return ecode, err
	}
	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpush, lom.Cname(), err)
	}

	bufsz := r.Size()
	if bufsz < 0 {
		bufsz = memsys.DefaultBufSize // TODO: track an average
	}
	buf, slab := core.T.PageMM().AllocSize(bufsz)
	_, err = io.CopyBuffer(w, r, buf)

	slab.Free(buf)
	r.Close()
	return 0, err
}

func (pc *pushComm) OfflineTransform(lom *core.LOM, timeout time.Duration) (cos.ReadCloseSizer, int, error) {
	clone := *lom
	r, ecode, err := pc.doRequest(&clone, timeout, "")
	if err == nil && cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpush, clone.Cname(), err)
	}
	return r, ecode, err
}

//////////////////
// redirectComm: implements Hpull
//////////////////

func (rc *redirectComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, targs string) (int, error) {
	if err := rc.boot.xctn.AbortErr(); err != nil {
		return 0, err
	}
	err := lomLoad(lom)
	if err != nil {
		return 0, err
	}

	u := rc.redirectURL(lom)
	if targs != "" {
		q := url.Values{apc.QparamETLTransformArgs: []string{targs}}
		u = cos.JoinQuery(u, q)
	}
	http.Redirect(w, r, u, http.StatusTemporaryRedirect)

	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpull, lom.Cname())
	}
	return 0, nil
}

func (rc *redirectComm) redirectURL(lom *core.LOM) (u string) {
	switch rc.boot.msg.ArgTypeX {
	case ArgTypeDefault, ArgTypeURL:
		u = cos.JoinPath(rc.boot.uri, transformerPath(lom))
	case ArgTypeFQN:
		u = cos.JoinPath(rc.boot.uri, url.PathEscape(lom.FQN))
	default:
		err := rc.boot.msg.errInvalidArg()
		debug.AssertNoErr(err)
		nlog.Errorln(err)
	}
	return u
}

func (rc *redirectComm) OfflineTransform(lom *core.LOM, timeout time.Duration) (cos.ReadCloseSizer, int, error) {
	clone := *lom
	errV := lomLoad(&clone)
	if errV != nil {
		return nil, 0, errV
	}

	etlURL := rc.redirectURL(&clone)
	r, ecode, err := rc.getWithTimeout(&clone, etlURL, timeout)

	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpull, clone.Cname(), err, ecode)
	}
	return r, ecode, err
}

//////////////////
// revProxyComm: implements Hrev
//////////////////

func (rp *revProxyComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, _ string) (int, error) {
	err := lomLoad(lom)
	if err != nil {
		return 0, err
	}
	path := transformerPath(lom)

	r.URL.Path, _ = url.PathUnescape(path) // `Path` must be unescaped otherwise it will be escaped again.
	r.URL.RawPath = path                   // `RawPath` should be escaped version of `Path`.
	rp.rp.ServeHTTP(w, r)

	return 0, nil
}

func (rp *revProxyComm) OfflineTransform(lom *core.LOM, timeout time.Duration) (cos.ReadCloseSizer, int, error) {
	clone := *lom
	errV := lomLoad(&clone)
	if errV != nil {
		return nil, 0, errV
	}
	etlURL := cos.JoinPath(rp.boot.uri, transformerPath(&clone))
	r, ecode, err := rp.getWithTimeout(&clone, etlURL, timeout)

	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hrev, clone.Cname(), err, ecode)
	}
	return r, ecode, err
}

//////////////
// cbWriter //
//////////////

func (cw *cbWriter) Write(b []byte) (n int, err error) {
	n, err = cw.w.Write(b)
	cw.writeCb(n)
	return
}

//
// utils
//

// prune query (received from AIS proxy) prior to reverse-proxying the request to/from container -
// not removing apc.QparamETLName, for instance, would cause infinite loop.
func pruneQuery(rawQuery string) string {
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		nlog.Errorf("failed to parse raw query %q, err: %v", rawQuery, err)
		return ""
	}
	for _, filtered := range []string{apc.QparamETLName, apc.QparamProxyID, apc.QparamUnixTime} {
		vals.Del(filtered)
	}
	return vals.Encode()
}

// TODO -- FIXME: unify the way we encode bucket/object:
// - url.PathEscape(uname) - see below - versus
// - Bck().Name + "/" + lom.ObjName - see pushComm above - versus
// - bck.AddToQuery() elsewhere
func transformerPath(lom *core.LOM) string {
	return "/" + url.PathEscape(lom.Uname())
}

func lomLoad(lom *core.LOM) (err error) {
	if err = lom.Load(true /*cacheIt*/, false /*locked*/); err != nil {
		if cos.IsNotExist(err, 0) && lom.Bucket().IsRemote() {
			err = nil // NOTE: size == 0
		}
	}
	return err
}
