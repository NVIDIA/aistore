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
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
)

type (
	CommStats interface {
		ObjCount() int64
		InBytes() int64
		OutBytes() int64
	}

	// Communicator is responsible for managing communications with local ETL pod.
	// It listens to cluster membership changes and terminates ETL pod, if need be.
	Communicator interface {
		meta.Slistener

		ETLName() string
		PodName() string
		SvcName() string
		getInitMsg() InitMsg

		String() string

		SetupConnection() error
		Stop()
		Restart(boot *etlBootstrapper)
		GetPodWatcher() *podWatcher
		GetSecret() string

		Xact() *XactETL // underlying `apc.ActETLInline` xaction (see xact/xs/etl.go)
		CommStats       // only stats for `apc.ActETLInline` inline transform

		// InlineTransform uses one of the two ETL container endpoints:
		//  - Method "PUT", Path "/"
		//  - Method "GET", Path "/bucket/object"
		InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, latestVer bool, targs string) (int, error)
	}

	// httpCommunicator manages stateless communication to ETL pod through HTTP requests
	httpCommunicator interface {
		Communicator

		// OfflineTransform is an instance of `core.GetROC` function, which is driven by `TCB` and `TCO` to provide offline transformation
		// Implementations include:
		// - pushComm
		// - redirectComm
		// - revProxyComm
		// See also, and separately: on-the-fly transformation as part of a user (e.g. training model) GET request handling
		OfflineTransform(lom *core.LOM, latestVer, sync bool, gargs *core.GetROCArgs) core.ReadResp
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

	// The http standard library automatically closes the request body on error,
	// which also releases the associated LOM lock (see `deferROC.Close()` in `core/ldp.go`).
	// The getBody function restores the body reader with the LOM properly locked again for retries.
	getBodyFunc func() core.ReadResp
	retryer     struct {
		client  *http.Client
		reqArgs *cmn.HreqArgs
		ctx     context.Context
		resp    *http.Response
		getBody getBodyFunc
	}
)

// interface guard
var (
	_ httpCommunicator = (*pushComm)(nil)
	_ httpCommunicator = (*redirectComm)(nil)
)

//////////////
// baseComm //
//////////////

func newCommunicator(listener meta.Slistener, boot *etlBootstrapper, pw *podWatcher) Communicator {
	switch boot.msg.CommType() {
	case Hpush, HpushStdin:
		pc := &pushComm{}
		pc.listener, pc.boot, pc.pw = listener, boot, pw
		if boot.msg.CommType() == HpushStdin { // io://
			pc.command = boot.originalCommand
		}
		return pc
	case Hpull:
		rc := &redirectComm{}
		rc.listener, rc.boot, rc.pw = listener, boot, pw
		return rc
	case WebSocket:
		ws := &webSocketComm{offlineSessions: make(map[string]Session, 4)}
		ws.commCtx, ws.commCtxCancel = context.WithCancel(context.Background())
		ws.listener, ws.boot, ws.pw = listener, boot, pw
		return ws
	}

	debug.Assert(false, "unknown comm-type '"+boot.msg.CommType()+"'")
	return nil
}

func (c *baseComm) ETLName() string { return c.boot.msg.Name() }
func (c *baseComm) PodName() string { return c.boot.pod.Name }
func (c *baseComm) SvcName() string { return c.boot.pod.Name /*same as pod name*/ }

func (c *baseComm) getInitMsg() InitMsg { return c.boot.msg }

func (c *baseComm) ListenSmapChanged() { c.listener.ListenSmapChanged() }

func (c *baseComm) String() string {
	return fmt.Sprintf("%s[%s]-%s", c.boot.originalPodName, c.boot.xctn.ID(), c.boot.msg.CommType())
}

func (c *baseComm) Xact() *XactETL  { return c.boot.xctn }
func (c *baseComm) ObjCount() int64 { return c.boot.xctn.Objs() }
func (c *baseComm) InBytes() int64  { return c.boot.xctn.InBytes() }
func (c *baseComm) OutBytes() int64 { return c.boot.xctn.OutBytes() }

func (c *baseComm) GetPodWatcher() *podWatcher { return c.pw }
func (c *baseComm) GetSecret() string          { return c.boot.secret }
func (c *baseComm) SetupConnection() error     { return c.boot.setupConnection("http://") }

func (c *baseComm) Restart(updBoot *etlBootstrapper) {
	c.boot.uri = updBoot.uri
	if updBoot.secret != "" {
		c.boot.secret = updBoot.secret
	}
	c.pw.boot = updBoot
}

func (c *baseComm) Stop() {
	// Note: xctn might have already been aborted and finished by pod watcher
	if !c.boot.xctn.Finished() && !c.boot.xctn.IsAborted() {
		c.boot.xctn.Finish()
	}
	if c.pw != nil {
		c.pw.stop(true)
	}
}

func handleRespEcode(ecode int, oah cos.OAH, r cos.ReadOpenCloser, err error) core.ReadResp {
	if err != nil {
		return core.ReadResp{R: r, OAH: oah, Err: err, Ecode: ecode}
	}
	switch ecode {
	case http.StatusOK, http.StatusAccepted, 0:
		return core.ReadResp{R: r, OAH: oah, Ecode: http.StatusOK}
	case http.StatusNoContent:
		// already delivered to destination target, no additional action needed
		if r != nil {
			cos.Close(r)
		}
		return core.ReadResp{R: nil, OAH: oah, Err: cmn.ErrSkip, Ecode: http.StatusNoContent}
	default:
		nlog.Errorln("unexpected ecode from etl:", ecode, oah, err)
	}
	return core.ReadResp{R: r, OAH: oah, Err: err, Ecode: ecode}
}

func doWithTimeout(reqArgs *cmn.HreqArgs, getBody getBodyFunc, timeout time.Duration, started int64) (r cos.ReadCloseSizer, ecode int, err error) {
	if timeout == 0 {
		timeout = DefaultObjTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rtyr := &retryer{client: core.T.DataClient(), reqArgs: reqArgs, ctx: ctx, getBody: getBody}
	ecode, err = cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call:      rtyr.call,
		SoftErr:   10,
		Verbosity: cmn.RetryLogVerbose,
		Sleep:     max(cmn.Rom.MaxKeepalive(), time.Second*5),
	})
	if err != nil {
		cancel()
		return nil, ecode, err
	}

	return cos.NewReaderWithArgs(cos.ReaderArgs{
		R:    rtyr.resp.Body,
		Size: rtyr.resp.ContentLength,
		DeferCb: func() {
			cancel()
			st := core.T.StatsUpdater()
			st.Inc(stats.ETLOfflineCount)
			st.Add(stats.ETLOfflineLatencyTotal, int64(mono.Since(started)))
		},
	}), rtyr.resp.StatusCode, nil
}

//////////////
// pushComm: implements (Hpush | HpushStdin)
//////////////

func (pc *pushComm) doRequest(lom *core.LOM, targs string, latestVer, sync bool, gargs *core.GetROCArgs) core.ReadResp {
	if err := lom.InitBck(lom.Bucket()); err != nil {
		return core.ReadResp{Err: err}
	}
	var (
		path    string
		getBody getBodyFunc

		started = mono.NanoTime()
		oah     = &cos.SimpleOAH{Atime: time.Now().UnixNano()}
		query   = make(url.Values, 2)
	)

	switch pc.boot.msg.ArgType() {
	case ArgTypeDefault, ArgTypeURL:
		// [TODO] to remove the following assert (and the corresponding limitation):
		// - container must be ready to receive complete bucket name including namespace
		// - see `bck.AddToQuery` and api/bucket.go for numerous examples
		debug.Assert(lom.Bck().Ns.IsGlobal(), lom.Bck().Cname(""), " - bucket with namespace")
		path = lom.Bck().Name + "/" + lom.ObjName
		getBody = func() core.ReadResp { return lom.GetROC(latestVer, sync) }
	case ArgTypeFQN:
		if ecode, err := lomLoad(lom, pc.boot.xctn.Kind()); err != nil {
			return core.ReadResp{Err: err, Ecode: ecode}
		}

		// body = http.NoBody
		path = url.PathEscape(lom.FQN)
	default:
		e := fmt.Errorf("%s: unexpected argument type %q", pc.boot.msg, pc.boot.msg.ArgType())
		debug.AssertNoErr(e) // is validated at construction time
		nlog.Errorln(e)
	}

	if len(pc.command) != 0 { // HpushStdin case
		query = url.Values{"command": []string{"bash", "-c", strings.Join(pc.command, " ")}}
	}

	if targs != "" {
		query.Set(apc.QparamETLTransformArgs, targs)
	}

	reqArgs := &cmn.HreqArgs{
		Method: http.MethodPut,
		Base:   pc.boot.uri,
		Path:   path,
		Header: http.Header{},
		Query:  query,
	}

	if pc.boot.msg.IsDirectPut() && gargs != nil && !gargs.Local {
		reqArgs.Header.Add(apc.HdrNodeURL, gargs.Daddr)
	}

	// note: `Content-Length` header is set during `retryer.call()` below
	_, objTimeout := pc.boot.msg.Timeouts()
	r, ecode, err := doWithTimeout(reqArgs, getBody, objTimeout.D(), started)
	if err != nil {
		return core.ReadResp{Err: err, Ecode: ecode}
	}
	oah.Size = r.Size()
	return core.ReadResp{R: cos.NopOpener(r), OAH: oah, Err: err, Ecode: ecode}
}

func (pc *pushComm) InlineTransform(w http.ResponseWriter, _ *http.Request, lom *core.LOM, latestVer bool, targs string) (int, error) {
	resp := pc.doRequest(lom, targs, latestVer, false, nil)
	if resp.Err != nil {
		return resp.Ecode, resp.Err
	}
	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpush, lom.Cname(), resp.Err)
	}

	bufsz := resp.OAH.Lsize()
	if bufsz < 0 {
		bufsz = memsys.DefaultBufSize // TODO: track an average
	}
	buf, slab := core.T.PageMM().AllocSize(bufsz)
	_, err := io.CopyBuffer(w, resp.R, buf)

	slab.Free(buf)
	resp.R.Close()
	return 0, err
}

func (pc *pushComm) OfflineTransform(lom *core.LOM, latestVer, sync bool, gargs *core.GetROCArgs) core.ReadResp {
	resp := pc.doRequest(lom, "", latestVer, sync, gargs)
	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpush, lom.Cname(), resp.Err, resp.Ecode)
	}
	return handleRespEcode(resp.Ecode, resp.OAH, resp.R, resp.Err)
}

//////////////////
// redirectComm: implements Hpull
//////////////////

func (rc *redirectComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, latestVer bool, targs string) (int, error) {
	if err := rc.boot.xctn.AbortErr(); err != nil {
		return 0, err
	}
	_, err := lomLoad(lom, rc.boot.xctn.Kind())
	if err != nil {
		return 0, err
	}

	path, query := rc.redirectArgs(lom, latestVer)
	if targs != "" {
		query.Set(apc.QparamETLTransformArgs, targs)
	}
	url := cos.JoinQuery(cos.JoinPath(rc.boot.uri, path), query)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)

	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpull, lom.Cname())
	}
	return 0, nil
}

// TODO: support `sync` option as well
func (rc *redirectComm) redirectArgs(lom *core.LOM, latestVer bool) (path string, query url.Values) {
	query = make(url.Values)
	switch rc.boot.msg.ArgType() {
	case ArgTypeDefault, ArgTypeURL:
		path = url.PathEscape(lom.Uname())
	case ArgTypeFQN:
		path = url.PathEscape(lom.FQN)
	default:
		err := fmt.Errorf("%s: unexpected argument type %q", rc.boot.msg, rc.boot.msg.ArgType())
		debug.AssertNoErr(err)
		nlog.Errorln(err)
	}

	if latestVer {
		query.Set(apc.QparamLatestVer, "true")
	}
	return path, query
}

func (rc *redirectComm) OfflineTransform(lom *core.LOM, latestVer, _ bool, gargs *core.GetROCArgs) core.ReadResp {
	var (
		started = mono.NanoTime()
		clone   = *lom
	)
	_, err := lomLoad(&clone, rc.boot.xctn.Kind())
	if err != nil {
		return core.ReadResp{Err: err}
	}
	path, query := rc.redirectArgs(&clone, latestVer)

	reqArgs := &cmn.HreqArgs{
		Method: http.MethodGet,
		Base:   rc.boot.uri,
		Path:   path,
		BodyR:  http.NoBody,
		Header: http.Header{},
		Query:  query,
	}

	if rc.boot.msg.IsDirectPut() && gargs != nil && !gargs.Local {
		reqArgs.Header.Add(apc.HdrNodeURL, gargs.Daddr)
	}

	_, objTimeout := rc.boot.msg.Timeouts()
	r, ecode, err := doWithTimeout(reqArgs, nil, objTimeout.D(), started)
	if err != nil {
		return core.ReadResp{Err: err, Ecode: ecode}
	}

	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpull, clone.Cname(), err, ecode)
	}
	clone.SetSize(r.Size())
	return handleRespEcode(ecode, &clone, cos.NopOpener(r), err)
}

//
// utils
//

func lomLoad(lom *core.LOM, xKind string) (ecode int, err error) {
	if err = lom.Load(true /*cacheIt*/, false /*locked*/); err != nil {
		if cos.IsNotExist(err, 0) && lom.Bucket().IsRemote() {
			return core.T.GetCold(context.Background(), lom, xKind, cmn.OwtGetLock)
		}
	}
	return http.StatusAccepted, err
}

/////////////
// retryer //
/////////////

func (rtyr *retryer) call() (status int, err error) {
	debug.Assert(rtyr.reqArgs != nil && rtyr.client != nil)
	var (
		body io.ReadCloser = http.NoBody
		size int64
	)

	// If a fresh body is needed (e.g., for retries), fetch it via getBody
	if rtyr.getBody != nil {
		src := rtyr.getBody()
		if src.Err != nil {
			return src.Ecode, src.Err
		}
		body = src.R
		size = src.OAH.Lsize()
	}

	req, err := http.NewRequestWithContext(rtyr.ctx, rtyr.reqArgs.Method, rtyr.reqArgs.URL(), body)
	if err != nil {
		return 0, err
	}

	req.Header = rtyr.reqArgs.Header
	req.ContentLength = size

	rtyr.resp, err = rtyr.client.Do(req) //nolint:bodyclose // closed by a caller
	if rtyr.resp != nil {
		status = rtyr.resp.StatusCode
	}
	return status, err
}
