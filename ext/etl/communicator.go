// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		Xact() cluster.Xact
		PodName() string
		SvcName() string

		String() string

		// InlineTransform uses one of the two ETL container endpoints:
		//  - Method "PUT", Path "/"
		//  - Method "GET", Path "/bucket/object"
		InlineTransform(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string) error

		// OfflineTransform interface implementations realize offline ETL.
		// OfflineTransform is driven by `OfflineDP` - not to confuse
		// with GET requests from users (such as training models and apps)
		// to perform on-the-fly transformation.
		OfflineTransform(bck *meta.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error)
		Stop()

		CommStats
	}

	baseComm struct {
		listener meta.Slistener
		boot     *etlBootstrapper
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

func newCommunicator(listener meta.Slistener, boot *etlBootstrapper) Communicator {
	switch boot.msg.CommTypeX {
	case Hpush, HpushStdin:
		pc := &pushComm{}
		pc.listener, pc.boot = listener, boot
		if boot.msg.CommTypeX == HpushStdin { // io://
			pc.command = boot.originalCommand
		}
		return pc
	case Hpull:
		rc := &redirectComm{}
		rc.listener, rc.boot = listener, boot
		return rc
	case Hrev:
		rp := &revProxyComm{}
		rp.listener, rp.boot = listener, boot

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

func (c *baseComm) Xact() cluster.Xact { return c.boot.xctn }
func (c *baseComm) ObjCount() int64    { return c.boot.xctn.Objs() }
func (c *baseComm) InBytes() int64     { return c.boot.xctn.InBytes() }
func (c *baseComm) OutBytes() int64    { return c.boot.xctn.OutBytes() }

func (c *baseComm) Stop() { c.boot.xctn.Finish() }

func (c *baseComm) getWithTimeout(url string, size int64, timeout time.Duration) (r cos.ReadCloseSizer, err error) {
	if err := c.boot.xctn.AbortErr(); err != nil {
		return nil, err
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
		resp, err = c.boot.t.DataClient().Do(req) //nolint:bodyclose // Closed by the caller.
	}
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, err
	}

	return cos.NewReaderWithArgs(cos.ReaderArgs{
		R:      resp.Body,
		Size:   resp.ContentLength,
		ReadCb: func(n int, err error) { c.boot.xctn.InObjsAdd(0, int64(n)) },
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
			c.boot.xctn.InObjsAdd(1, 0)
			c.boot.xctn.OutObjsAdd(1, size) // see also: `coi.objsAdd`
		},
	}), nil
}

//////////////
// pushComm: implements (Hpush | HpushStdin)
//////////////

func (pc *pushComm) doRequest(bck *meta.Bck, lom *cluster.LOM, timeout time.Duration) (r cos.ReadCloseSizer, err error) {
	if err := lom.InitBck(bck.Bucket()); err != nil {
		return nil, err
	}

	lom.Lock(false)
	r, err = pc.do(lom, timeout)
	lom.Unlock(false)

	if err != nil && cmn.IsObjNotExist(err) && bck.IsRemote() {
		_, err = pc.boot.t.GetCold(context.Background(), lom, cmn.OwtGetLock)
		if err != nil {
			return nil, err
		}
		lom.Lock(false)
		r, err = pc.do(lom, timeout)
		lom.Unlock(false)
	}
	return
}

func (pc *pushComm) do(lom *cluster.LOM, timeout time.Duration) (cos.ReadCloseSizer, error) {
	if err := pc.boot.xctn.AbortErr(); err != nil {
		return nil, err
	}
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil, err
	}
	size := lom.SizeBytes()

	// `fh` is closed by Do(req).
	fh, err := cos.NewFileHandle(lom.FQN)
	if err != nil {
		return nil, err
	}

	var (
		cancel func()
		req    *http.Request
		resp   *http.Response
		url    = pc.boot.uri + "/" + lom.Bck().Name + "/" + lom.ObjName
	)

	debug.Assert(lom.Bck().Ns.IsGlobal(), lom.Bck().Ns.String()) // the url (above) simplifies out bucket's namespace

	// TODO -- FIXME: switch(ArgType)

	if timeout != 0 {
		var ctx context.Context
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, url, fh)
	} else {
		req, err = http.NewRequest(http.MethodPut, url, fh)
	}
	if err != nil {
		cos.Close(fh)
		goto finish
	}
	if len(pc.command) != 0 {
		q := req.URL.Query()
		q["command"] = []string{"bash", "-c", strings.Join(pc.command, " ")}
		req.URL.RawQuery = q.Encode()
	}
	req.ContentLength = size
	req.Header.Set(cos.HdrContentType, cos.ContentBinary)
	resp, err = pc.boot.t.DataClient().Do(req) //nolint:bodyclose // Closed by the caller.
finish:
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, err
	}

	return cos.NewReaderWithArgs(cos.ReaderArgs{
		R:      resp.Body,
		Size:   resp.ContentLength,
		ReadCb: func(n int, err error) { pc.boot.xctn.InObjsAdd(0, int64(n)) },
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
			pc.boot.xctn.InObjsAdd(1, 0)
			pc.boot.xctn.OutObjsAdd(1, size) // see also: `coi.objsAdd`
		},
	}), nil
}

func (pc *pushComm) InlineTransform(w http.ResponseWriter, _ *http.Request, bck *meta.Bck, objName string) error {
	lom := cluster.AllocLOM(objName)
	r, err := pc.doRequest(bck, lom, 0 /*timeout*/)
	cluster.FreeLOM(lom)
	if err != nil {
		return err
	}
	if pc.boot.config.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpush, lom.Cname(), err)
	}

	size := r.Size()
	if size < 0 {
		size = memsys.DefaultBufSize // TODO: track the average
	}
	buf, slab := pc.boot.t.PageMM().AllocSize(size)
	_, err = io.CopyBuffer(w, r, buf)

	slab.Free(buf)
	r.Close()
	return err
}

func (pc *pushComm) OfflineTransform(bck *meta.Bck, objName string, timeout time.Duration) (r cos.ReadCloseSizer, err error) {
	lom := cluster.AllocLOM(objName)
	r, err = pc.doRequest(bck, lom, timeout)
	cluster.FreeLOM(lom)
	if err == nil && pc.boot.config.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpush, lom.Cname(), err)
	}
	return
}

//////////////////
// redirectComm: implements Hpull
//////////////////

func (rc *redirectComm) InlineTransform(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string) error {
	if err := rc.boot.xctn.AbortErr(); err != nil {
		return err
	}

	size, err := lomLoad(bck, objName)
	if err != nil {
		return err
	}
	rc.boot.xctn.OutObjsAdd(1, size)

	// is there a way to determine `rc.stats.outBytes`?
	redirectURL := cos.JoinPath(rc.boot.uri, transformerPath(bck, objName))
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	if rc.boot.config.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpull, bck.Cname(objName))
	}
	return nil
}

func (rc *redirectComm) OfflineTransform(bck *meta.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	size, errV := lomLoad(bck, objName)
	if errV != nil {
		return nil, errV
	}
	etlURL := cos.JoinPath(rc.boot.uri, transformerPath(bck, objName))
	r, err := rc.getWithTimeout(etlURL, size, timeout)
	if rc.boot.config.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hpull, bck.Cname(objName), err)
	}
	return r, err
}

//////////////////
// revProxyComm: implements Hrev
//////////////////

func (rp *revProxyComm) InlineTransform(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string) error {
	size, err := lomLoad(bck, objName)
	if err != nil {
		return err
	}
	rp.boot.xctn.OutObjsAdd(1, size)

	// is there a way to determine `rc.stats.outBytes`?
	path := transformerPath(bck, objName)
	r.URL.Path, _ = url.PathUnescape(path) // `Path` must be unescaped otherwise it will be escaped again.
	r.URL.RawPath = path                   // `RawPath` should be escaped version of `Path`.
	rp.rp.ServeHTTP(w, r)
	return nil
}

func (rp *revProxyComm) OfflineTransform(bck *meta.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	size, errV := lomLoad(bck, objName)
	if errV != nil {
		return nil, errV
	}
	etlURL := cos.JoinPath(rp.boot.uri, transformerPath(bck, objName))
	r, err := rp.getWithTimeout(etlURL, size, timeout)
	if rp.boot.config.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(Hrev, bck.Cname(objName), err)
	}
	return r, err
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

// TODO: Consider encoding bucket and object name without the necessity to escape.
func transformerPath(bck *meta.Bck, objName string) string {
	return "/" + url.PathEscape(bck.MakeUname(objName))
}

func lomLoad(bck *meta.Bck, objName string) (int64, error) {
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		return 0, err
	}
	if err := lom.Load(true /*cacheIt*/, false /*locked*/); err != nil {
		if cmn.IsObjNotExist(err) && bck.IsRemote() {
			return 0, nil
		}
		return 0, err
	}
	return lom.SizeBytes(), nil
}
