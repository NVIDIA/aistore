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
	"strconv"
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

	// communicatorCommon is responsible for managing communications with local ETL pod.
	// It listens to cluster membership changes and terminates ETL pod, if need be.
	communicatorCommon interface {
		meta.Slistener

		ETLName() string
		PodName() string
		SvcName() string

		String() string

		SetupConnection() error
		Stop()
		Restart(boot *etlBootstrapper)
		GetPodWatcher() *podWatcher
		GetSecret() string

		Xact() core.Xact // underlying `apc.ActETLInline` xaction (see xact/xs/etl.go)
		CommStats        // only stats for `apc.ActETLInline` inline transform
	}

	// Communicator manages stateless communication to ETL pod through HTTP requests
	Communicator interface {
		communicatorCommon

		// InlineTransform uses one of the two ETL container endpoints:
		//  - Method "PUT", Path "/"
		//  - Method "GET", Path "/bucket/object"
		InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, targs string) (int, error)

		// OfflineTransform is an instance of `core.GetROC` function, which is driven by `TCB` and `TCO` to provide offline transformation
		// Implementations include:
		// - pushComm
		// - redirectComm
		// - revProxyComm
		// See also, and separately: on-the-fly transformation as part of a user (e.g. training model) GET request handling
		OfflineTransform(lom *core.LOM, latestVer, sync bool, daddr string) core.ReadResp
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
)

// interface guard
var (
	_ Communicator = (*pushComm)(nil)
	_ Communicator = (*redirectComm)(nil)
	_ Communicator = (*revProxyComm)(nil)
)

//////////////
// baseComm //
//////////////

func newCommunicator(listener meta.Slistener, boot *etlBootstrapper, pw *podWatcher) communicatorCommon {
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
	case WebSocket:
		ws := &webSocketComm{sessions: make(map[string]Session, 4)}
		ws.listener, ws.boot, ws.pw = listener, boot, pw
		return ws
	}

	debug.Assert(false, "unknown comm-type '"+boot.msg.CommTypeX+"'")
	return nil
}

func (c *baseComm) ETLName() string { return c.boot.msg.EtlName }
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

func (c *baseComm) GetPodWatcher() *podWatcher { return c.pw }
func (c *baseComm) GetSecret() string          { return c.boot.secret }
func (c *baseComm) SetupConnection() error     { return c.boot.setupConnection("http://") }

func (c *baseComm) Restart(updBoot *etlBootstrapper) {
	c.boot.uri = updBoot.uri
	if updBoot.secret != "" {
		c.boot.secret = updBoot.secret
	}
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
	case http.StatusOK:
	case http.StatusAccepted:
	case 0:
		return core.ReadResp{R: r, OAH: oah, Ecode: http.StatusOK}
	case http.StatusNoContent:
		// already delivered to destination target, no additional action needed
		if r != nil {
			cos.Close(r)
		}
		return core.ReadResp{R: nil, OAH: oah, Ecode: http.StatusNoContent}
	default:
		nlog.Errorln("unexpected ecode from etl:", ecode, oah, err)
	}
	return core.ReadResp{R: r, OAH: oah, Err: err, Ecode: http.StatusNoContent}
}

func doWithTimeout(reqArgs *cmn.HreqArgs, srcSize int64, timeout time.Duration, started int64) (r cos.ReadCloseSizer, ecode int, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)
	if timeout == 0 {
		timeout = DefaultReqTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req, err = http.NewRequestWithContext(ctx, reqArgs.Method, reqArgs.URL(), reqArgs.BodyR)
	if err != nil {
		cancel()
		return nil, 0, err
	}

	req.ContentLength = srcSize
	resp, err = core.T.DataClient().Do(req) //nolint:bodyclose // Closed by the caller.
	if err != nil {
		cancel()
		if resp != nil {
			ecode = resp.StatusCode
		}
		return nil, ecode, err
	}

	// if the transformed object's size is unknown, fall back to the original source size.
	dstSize := resp.ContentLength
	if dstSize == cos.ContentLengthUnknown {
		dstSize = srcSize
	}

	return cos.NewReaderWithArgs(cos.ReaderArgs{
		R:    resp.Body,
		Size: dstSize,
		DeferCb: func() {
			cancel()
			st := core.T.StatsUpdater()
			st.Inc(stats.ETLOfflineCount)
			st.Add(stats.ETLOfflineLatencyTotal, int64(mono.Since(started)))
		},
	}), resp.StatusCode, nil
}

//////////////
// pushComm: implements (Hpush | HpushStdin)
//////////////

func (pc *pushComm) doRequest(lom *core.LOM, timeout time.Duration, targs string, latestVer, sync bool, daddr string) core.ReadResp {
	if err := lom.InitBck(lom.Bucket()); err != nil {
		return core.ReadResp{Err: err}
	}
	var (
		path    string
		srcSize int64
		body    io.ReadCloser
		oah     cos.OAH = lom

		started = mono.NanoTime()
		query   = make(url.Values, 2)
	)

	switch pc.boot.msg.ArgTypeX {
	case ArgTypeDefault, ArgTypeURL:
		// [TODO] to remove the following assert (and the corresponding limitation):
		// - container must be ready to receive complete bucket name including namespace
		// - see `bck.AddToQuery` and api/bucket.go for numerous examples
		debug.Assert(lom.Bck().Ns.IsGlobal(), lom.Bck().Cname(""), " - bucket with namespace")
		path = lom.Bck().Name + "/" + lom.ObjName
		srcResp := lom.GetROC(latestVer, sync)
		if srcResp.Err != nil {
			return srcResp
		}

		body = srcResp.R
		srcSize = srcResp.OAH.Lsize()
		oah = srcResp.OAH
	case ArgTypeFQN:
		if ecode, err := lomLoad(lom, pc.boot.xctn.Kind()); err != nil {
			return core.ReadResp{Err: err, Ecode: ecode}
		}

		body = http.NoBody
		path = url.PathEscape(lom.FQN)
	default:
		e := pc.boot.msg.errInvalidArg()
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
		BodyR:  body,
		Header: http.Header{
			cos.HdrContentLength: []string{strconv.Itoa(int(srcSize))},
			apc.HdrNodeURL:       []string{daddr},
		},
		Query: query,
	}

	r, ecode, err := doWithTimeout(reqArgs, srcSize, timeout, started)
	return core.ReadResp{R: cos.NopOpener(r), OAH: oah, Err: err, Ecode: ecode}
}

func (pc *pushComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, targs string) (int, error) {
	latestVer := r.URL.Query().Get(apc.QparamLatestVer)
	resp := pc.doRequest(lom, pc.boot.msg.Timeout.D(), targs, cos.IsParseBool(latestVer), false, "")
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

func (pc *pushComm) OfflineTransform(lom *core.LOM, latestVer, sync bool, daddr string) core.ReadResp {
	resp := pc.doRequest(lom, pc.boot.msg.Timeout.D(), "", latestVer, sync, daddr)
	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpush, lom.Cname(), resp.Err, resp.Ecode)
	}
	return handleRespEcode(resp.Ecode, resp.OAH, resp.R, resp.Err)
}

//////////////////
// redirectComm: implements Hpull
//////////////////

func (rc *redirectComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, targs string) (int, error) {
	if err := rc.boot.xctn.AbortErr(); err != nil {
		return 0, err
	}
	_, err := lomLoad(lom, rc.boot.xctn.Kind())
	if err != nil {
		return 0, err
	}

	latestVer := r.URL.Query().Get(apc.QparamLatestVer)
	path, query := rc.redirectArgs(lom, cos.IsParseBool(latestVer))
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
	switch rc.boot.msg.ArgTypeX {
	case ArgTypeDefault, ArgTypeURL:
		path = transformerPath(lom)
	case ArgTypeFQN:
		path = url.PathEscape(lom.FQN)
	default:
		err := rc.boot.msg.errInvalidArg()
		debug.AssertNoErr(err)
		nlog.Errorln(err)
	}

	if latestVer {
		query.Set(apc.QparamLatestVer, "true")
	}
	return path, query
}

func (rc *redirectComm) OfflineTransform(lom *core.LOM, latestVer, _ bool, daddr string) core.ReadResp {
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
		Header: http.Header{
			cos.HdrContentLength: []string{strconv.Itoa(int(clone.Lsize()))},
			apc.HdrNodeURL:       []string{daddr},
		},
		Query: query,
	}

	r, ecode, err := doWithTimeout(reqArgs, clone.Lsize(), rc.boot.msg.Timeout.D(), started)

	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpull, clone.Cname(), err, ecode)
	}
	clone.SetSize(r.Size())
	return handleRespEcode(ecode, &clone, cos.NopOpener(r), err)
}

//////////////////
// revProxyComm: implements Hrev
//////////////////

func (rp *revProxyComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, _ string) (int, error) {
	_, err := lomLoad(lom, rp.boot.xctn.Kind())
	if err != nil {
		return 0, err
	}
	path := transformerPath(lom)

	r.URL.Path, _ = url.PathUnescape(path) // `Path` must be unescaped otherwise it will be escaped again.
	r.URL.RawPath = path                   // `RawPath` should be escaped version of `Path`.
	rp.rp.ServeHTTP(w, r)

	return 0, nil
}

func (rp *revProxyComm) OfflineTransform(lom *core.LOM, _, _ bool, daddr string) core.ReadResp {
	var (
		started = mono.NanoTime()
		clone   = *lom
	)
	_, err := lomLoad(&clone, rp.boot.xctn.Kind())
	if err != nil {
		return core.ReadResp{Err: err}
	}

	reqArgs := &cmn.HreqArgs{
		Method: http.MethodGet,
		Base:   rp.boot.uri,
		Path:   transformerPath(&clone),
		BodyR:  http.NoBody,
		Header: http.Header{
			cos.HdrContentLength: []string{strconv.Itoa(int(clone.Lsize()))},
			apc.HdrNodeURL:       []string{daddr},
		},
	}

	r, ecode, err := doWithTimeout(reqArgs, clone.Lsize(), rp.boot.msg.Timeout.D(), started)

	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hrev, clone.Cname(), err, ecode)
	}

	return handleRespEcode(ecode, &clone, cos.NopOpener(r), err)
}

//
// utils
//

// getComm retrieves the communicator from registry by etl name
// Returns an error if not found or not in the Running stage.
func getComm(etlName string) (communicatorCommon, error) {
	comm, stage := mgr.getByName(etlName)
	if comm == nil {
		return nil, cos.NewErrNotFound(core.T, etlName)
	}

	if stage != Running {
		return comm, cos.NewErrNotFound(core.T, etlName+" not in Running stage")
	}
	return comm, nil
}

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

func lomLoad(lom *core.LOM, xKind string) (ecode int, err error) {
	if err = lom.Load(true /*cacheIt*/, false /*locked*/); err != nil {
		if cos.IsNotExist(err, 0) && lom.Bucket().IsRemote() {
			return core.T.GetCold(context.Background(), lom, xKind, cmn.OwtGetLock)
		}
	}
	return http.StatusAccepted, err
}
