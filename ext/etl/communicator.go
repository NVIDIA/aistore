// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	CommStats interface {
		ObjCount() int64
		InBytes() int64
		OutBytes() int64
	}

	// Communicator is responsible for managing communications with local ETL pod.
	Communicator interface {
		ETLName() string
		getInitMsg() InitMsg

		String() string

		setupConnection(schema, podAddr string) (ecode int, err error)
		setupXaction(xid string) error
		stop() error
		GetSecret() string
		Xact() *XactETL // underlying `apc.ActETLInline` xaction (see xact/xs/etl.go)
		CommStats       // only stats for `apc.ActETLInline` inline transform

		// InlineTransform uses one of the two ETL container endpoints:
		//  - Method "PUT", Path "/"
		//  - Method "GET", Path "/bucket/object"
		//  - Returns:
		//    - size: the size of transformed object
		//    - ecode: error code
		//    - err: error encountered during transformation
		InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, args *InlineTransArgs) (size int64, ecode int, err error)

		// ProcessDownloadJob extracts objects from job and routes them to ETL pod
		ProcessDownloadJob(ctx *ETLObjDownloadCtx) (cos.ReadCloseSizer, int, error)
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
		OfflineTransform(lom *core.LOM, latestVer, sync bool, args *core.ETLArgs) core.ReadResp
	}

	InlineTransArgs struct {
		TransformArgs string
		Pipeline      apc.ETLPipeline
		LatestVer     bool
		// TODO: add sync option
	}

	baseComm struct {
		msg     InitMsg
		config  *cmn.Config
		xctn    *XactETL
		secret  string
		podAddr string
		podURI  string
		stopped atomic.Bool
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

func newCommunicator(msg InitMsg, secret string, config *cmn.Config) (Communicator, error) {
	switch msg.CommType() {
	case Hpush, HpushStdin:
		pc := &pushComm{}
		pc.msg, pc.secret, pc.config = msg, secret, config
		return pc, nil
	case Hpull:
		rc := &redirectComm{}
		rc.msg, rc.secret, rc.config = msg, secret, config
		return rc, nil
	case WebSocket:
		ws := &webSocketComm{sessions: make(map[string]Session, 4)}
		ws.msg, ws.secret, ws.config = msg, secret, config
		ws.commCtx, ws.commCtxCancel = context.WithCancel(context.Background())
		return ws, nil
	}

	debug.Assert(false, "unknown comm-type '"+msg.CommType()+"'")
	return nil, fmt.Errorf("unknown comm-type %s", msg.CommType())
}

func (c *baseComm) ETLName() string     { return c.msg.Name() }
func (c *baseComm) getInitMsg() InitMsg { return c.msg }
func (c *baseComm) String() string      { return fmt.Sprintf("[%s]-%s", c.xctn.ID(), c.msg.CommType()) }
func (c *baseComm) Xact() *XactETL      { return c.xctn }
func (c *baseComm) ObjCount() int64     { return c.xctn.Objs() }
func (c *baseComm) InBytes() int64      { return c.xctn.InBytes() }
func (c *baseComm) OutBytes() int64     { return c.xctn.OutBytes() }
func (c *baseComm) GetSecret() string   { return c.secret }

func (c *baseComm) setupXaction(xid string) error {
	rns := xreg.RenewETL(c.msg, xid)
	if rns.Err != nil {
		return rns.Err
	}
	xctn := rns.Entry.Get()
	c.xctn = xctn.(*XactETL)
	debug.Assertf(c.xctn.ID() == xid, "%s vs %s", c.xctn.ID(), xid)
	return nil
}

func (c *baseComm) setupConnection(schema, podAddr string) (ecode int, err error) {
	// the pod must be reachable via its tcp addr
	c.podAddr = podAddr
	if ecode, err = c.dial(); err != nil {
		if cmn.Rom.V(4, cos.ModETL) {
			nlog.Warningf("%s: failed to dial %s", c.msg.Cname(), c.podAddr)
		}
		return ecode, err
	}

	c.podURI = schema + c.podAddr
	if cmn.Rom.V(4, cos.ModETL) {
		nlog.Infof("%s: setup connection to %s", c.msg.Cname(), c.podURI)
	}
	return 0, nil
}

func (c *baseComm) dial() (int, error) {
	var (
		action = "dial POD at " + c.podAddr
		args   = &cmn.RetryArgs{
			Call:      c.call,
			SoftErr:   10,
			HardErr:   2,
			Sleep:     3 * time.Second,
			Verbosity: cmn.RetryLogOff,
			Action:    action,
		}
	)
	ecode, err := args.Do()
	if err != nil {
		return ecode, fmt.Errorf("failed to wait for ETL Service/Pod at %q to respond: %v", c.podAddr, err)
	}
	return 0, nil
}

func (c *baseComm) call() (int, error) {
	ctx := context.Background()
	dialer := &net.Dialer{Timeout: cmn.Rom.MaxKeepalive()}
	netFam := cmn.AddrToNetworkFamily(c.podAddr)
	conn, err := dialer.DialContext(ctx, netFam, c.podAddr)
	if err != nil {
		return 0, err
	}
	cos.Close(conn)
	return 0, nil
}

func (c *baseComm) stop() error {
	if !c.stopped.CAS(false, true) {
		return nil // already stopped. do nothing
	}

	// Note: xctn might have already been aborted and finished by pod watcher
	if !c.xctn.IsDone() && !c.xctn.IsAborted() {
		c.xctn.Finish()
	}

	return nil
}

// ProcessDownloadJob routes download job to ETL pod
func (pc *pushComm) ProcessDownloadJob(ctx *ETLObjDownloadCtx) (cos.ReadCloseSizer, int, error) {
	if ctx.ObjName == "" || ctx.Link == "" {
		return nil, http.StatusBadRequest, errors.New("missing objName or link in ETL job context")
	}

	query := make(url.Values, 3)
	query.Set(apc.QparamOrigURL, ctx.Link)
	query.Set(apc.QparamObjTo, ctx.ObjName)
	if ctx.ETLArgs != "" {
		query.Set(apc.QparamETLTransformArgs, ctx.ETLArgs)
	}

	reqArgs := cmn.AllocHra()
	defer cmn.FreeHra(reqArgs)
	{
		reqArgs.Method = http.MethodPost
		reqArgs.Base = pc.podURI
		reqArgs.Path = apc.ETLDownload
		reqArgs.Query = query
	}

	_, objTimeout := pc.msg.Timeouts()
	resp, ecode, err := doWithTimeout(reqArgs, nil, objTimeout.D())

	if err != nil {
		return nil, ecode, fmt.Errorf("failed to send object to ETL pod: %v", err)
	}
	if ecode >= http.StatusBadRequest {
		if resp != nil {
			resp.Close()
		}
		return nil, ecode, fmt.Errorf("ETL pod returned error status: %d", ecode)
	}
	if resp == nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("no response from ETL pod for %s", ctx.ObjName)
	}

	return resp, http.StatusOK, nil
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
		if ecode >= 400 {
			if cmn.Rom.V(5, cos.ModETL) {
				nlog.Warningln("unexpected ecode from etl:", ecode, oah, err)
			}
			debug.Assert(r != nil)
			// error from ETL, retrieve the error message from the response body
			e, err := cos.ReadAll(r)
			if err != nil {
				err = fmt.Errorf("failed to read error message from ETL response: %v", err)
			} else {
				err = fmt.Errorf("ETL error: %s", e)
			}
			return core.ReadResp{R: r, OAH: oah, Err: err, Ecode: ecode}
		}
	}
	return core.ReadResp{R: r, OAH: oah, Err: err, Ecode: ecode}
}

func doWithTimeout(reqArgs *cmn.HreqArgs, getBody getBodyFunc, timeout time.Duration) (r cos.ReadCloseSizer, ecode int, err error) {
	if timeout == 0 {
		timeout = DefaultObjTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	rtyr := &retryer{client: core.T.DataClient(), reqArgs: reqArgs, ctx: ctx, getBody: getBody}
	args := &cmn.RetryArgs{
		Call:      rtyr.call,
		SoftErr:   10,
		Verbosity: cmn.RetryLogVerbose,
		Sleep:     max(cmn.Rom.MaxKeepalive(), time.Second*5),
	}
	ecode, err = args.Do()
	if err != nil {
		cancel()
		return nil, ecode, err
	}

	return &cos.ReaderWithArgs{
		R:       rtyr.resp.Body,
		Rsize:   rtyr.resp.ContentLength,
		DeferCb: cancel,
	}, rtyr.resp.StatusCode, nil
}

//////////////
// pushComm: implements (Hpush | HpushStdin)
//////////////

func (pc *pushComm) doRequest(lom *core.LOM, args *core.ETLArgs, latestVer, sync bool) core.ReadResp {
	if err := lom.InitBck(lom.Bck()); err != nil {
		return core.ReadResp{Err: err}
	}
	var (
		getBody getBodyFunc

		path  = lom.Bck().Name + "/" + lom.ObjName
		oah   = &cos.SimpleOAH{Atime: time.Now().UnixNano()}
		query = make(url.Values, 3)
	)

	switch {
	case latestVer, sync, cmn.Rom.Features().IsSet(feat.DontAllowPassingFQNtoETL): // TODO -- FIXME: consider chunked case
		// [TODO] to remove the following assert (and the corresponding limitation):
		// - container must be ready to receive complete bucket name including namespace
		// - see `bck.AddToQuery` and api/bucket.go for numerous examples
		debug.Assert(lom.Bck().Ns.IsGlobal(), lom.Bck().Cname(""), " - bucket with namespace")
		getBody = func() core.ReadResp { return lom.GetROC(latestVer, sync) }
	default:
		// default to FQN
		if ecode, err := lomLoad(lom, pc.xctn.Kind()); err != nil {
			return core.ReadResp{Err: err, Ecode: ecode}
		}
		query.Set(apc.QparamETLFQN, url.PathEscape(lom.FQN))
	}

	if len(pc.command) != 0 { // HpushStdin case
		query = url.Values{"command": []string{"bash", "-c", strings.Join(pc.command, " ")}}
	}

	reqArgs := &cmn.HreqArgs{
		Method: http.MethodPut,
		Base:   pc.podURI,
		Path:   path,
		Header: http.Header{},
		Query:  query,
	}

	if args != nil {
		if args.TransformArgs != "" {
			query.Set(apc.QparamETLTransformArgs, args.TransformArgs)
		}
		if len(args.Pipeline) > 0 {
			reqArgs.Header.Add(apc.HdrNodeURL, args.Pipeline.Pack())
		}
	}

	// note: `Content-Length` header is set during `retryer.call()` below
	_, objTimeout := pc.msg.Timeouts()
	r, ecode, err := doWithTimeout(reqArgs, getBody, objTimeout.D())
	if err != nil {
		return core.ReadResp{Err: err, Ecode: ecode}
	}

	if cmn.Rom.V(5, cos.ModETL) {
		nlog.Infoln(Hpush, lom.Cname(), args.Pipeline.String(), err, ecode)
	}

	oah.Size = r.Size()
	return core.ReadResp{R: cos.NopOpener(r), OAH: oah, Err: err, Ecode: ecode}
}

func (pc *pushComm) InlineTransform(w http.ResponseWriter, _ *http.Request, lom *core.LOM, args *InlineTransArgs) (size int64, ecode int, err error) {
	resp := pc.doRequest(lom, &core.ETLArgs{TransformArgs: args.TransformArgs, Pipeline: args.Pipeline}, args.LatestVer, false /* sync */) // TODO: support sync
	if resp.Err != nil {
		return 0, resp.Ecode, resp.Err
	}
	if cmn.Rom.V(5, cos.ModETL) {
		nlog.Infoln(Hpush, lom.Cname(), resp.Err)
	}

	bufsz := resp.OAH.Lsize()
	if bufsz < 0 {
		bufsz = memsys.DefaultBufSize // TODO: track an average
	}
	buf, slab := core.T.PageMM().AllocSize(bufsz)
	w.WriteHeader(resp.Ecode)
	n, err := cos.CopyBuffer(w, resp.R, buf)

	slab.Free(buf)
	resp.R.Close()
	return n, 0, err
}

func (pc *pushComm) OfflineTransform(lom *core.LOM, latestVer, sync bool, args *core.ETLArgs) core.ReadResp {
	resp := pc.doRequest(lom, args, latestVer, sync)
	if cmn.Rom.V(5, cos.ModETL) {
		nlog.Infoln(Hpush, lom.Cname(), resp.Err, resp.Ecode)
	}
	return handleRespEcode(resp.Ecode, resp.OAH, resp.R, resp.Err)
}

//////////////////
// redirectComm: implements Hpull
//////////////////

// NOTE: pipeline not implemented for hpull, since HTTP redirect doesn't preserve headers. Cannot use header to pass pipeline.
func (rc *redirectComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, args *InlineTransArgs) (size int64, ecode int, err error) {
	if args.Pipeline != nil {
		return 0, http.StatusBadRequest, errors.New("inline transform pipeline not supported for " + rc.msg.CommType())
	}
	if err := rc.xctn.AbortErr(); err != nil {
		return 0, 0, err
	}
	ecode, err = lomLoad(lom, rc.xctn.Kind())
	if err != nil {
		return 0, ecode, err
	}

	path, query := rc.redirectArgs(lom, args.LatestVer)
	if args.TransformArgs != "" {
		query.Set(apc.QparamETLTransformArgs, args.TransformArgs)
	}
	url := cos.JoinQuery(cos.JoinPath(rc.podURI, path), query)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)

	if cmn.Rom.V(5, cos.ModETL) {
		nlog.Infoln(Hpull, lom.Cname())
	}
	return cos.ContentLengthUnknown, 0, nil // TODO: stats inline transform size for hpull
}

// TODO: support `sync` option as well
func (*redirectComm) redirectArgs(lom *core.LOM, latestVer bool) (string, url.Values) {
	query := make(url.Values, 4)
	if !cmn.Rom.Features().IsSet(feat.DontAllowPassingFQNtoETL) {
		// TODO -- FIXME: consider chunked case
		query.Set(apc.QparamETLFQN, url.PathEscape(lom.FQN))
	}
	if latestVer {
		query.Set(apc.QparamLatestVer, "true")
	}
	return url.PathEscape(lom.Uname()), query
}

func (rc *redirectComm) OfflineTransform(lom *core.LOM, latestVer, _ bool, args *core.ETLArgs) core.ReadResp {
	clone := *lom
	ecode, err := lomLoad(&clone, rc.xctn.Kind())
	if err != nil {
		return core.ReadResp{Err: err, Ecode: ecode}
	}
	path, query := rc.redirectArgs(&clone, latestVer)

	if args != nil && args.TransformArgs != "" {
		query.Set(apc.QparamETLTransformArgs, args.TransformArgs)
	}

	reqArgs := &cmn.HreqArgs{
		Method: http.MethodGet,
		Base:   rc.podURI,
		Path:   path,
		BodyR:  http.NoBody,
		Header: http.Header{},
		Query:  query,
	}

	if args != nil && len(args.Pipeline) > 0 {
		reqArgs.Header.Add(apc.HdrNodeURL, args.Pipeline.Pack())
	}

	_, objTimeout := rc.msg.Timeouts()
	r, ecode, err := doWithTimeout(reqArgs, nil, objTimeout.D())
	if err != nil {
		return core.ReadResp{Err: err, Ecode: ecode}
	}

	if cmn.Rom.V(5, cos.ModETL) {
		nlog.Infoln(Hpull, clone.Cname(), args.Pipeline.String(), err, ecode)
	}
	clone.SetSize(r.Size())
	return handleRespEcode(ecode, &clone, cos.NopOpener(r), err)
}

func (*redirectComm) ProcessDownloadJob(_ *ETLObjDownloadCtx) (cos.ReadCloseSizer, int, error) {
	return nil, http.StatusNotImplemented, errors.New("ETL downloads not supported for hpull communication type")
}

//
// utils
//

func lomLoad(lom *core.LOM, xKind string) (ecode int, err error) {
	if err = lom.Load(false /*cacheIt*/, false /*locked*/); err != nil {
		if cos.IsNotExist(err) && lom.Bucket().IsRemote() {
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
