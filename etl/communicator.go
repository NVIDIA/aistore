// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
		cluster.Slistener

		Name() string
		PodName() string
		SvcName() string

		// OnlineTransform uses one of the two ETL container endpoints:
		//  - Method "PUT", Path "/"
		//  - Method "GET", Path "/bucket/object"
		OnlineTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error

		// OfflineTransform interface implementations realize offline ETL.
		// OfflineTransform is driven by `OfflineDataProvider` - not to confuse
		// with GET requests from users (such as training models and apps)
		// to perform on-the-fly transformation.
		OfflineTransform(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error)

		Stop()

		CommStats
	}

	commArgs struct {
		listener    cluster.Slistener
		bootstraper *etlBootstraper
	}

	baseComm struct {
		cluster.Slistener
		t cluster.Target

		name    string
		podName string

		xact cluster.Xact
	}

	pushComm struct {
		baseComm
		mem     *memsys.MMSA
		uri     string
		command []string
	}
	redirectComm struct {
		baseComm
		uri string
	}
	revProxyComm struct {
		baseComm
		rp  *httputil.ReverseProxy
		uri string
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

func makeCommunicator(args commArgs) Communicator {
	baseComm := baseComm{
		Slistener: args.listener,
		t:         args.bootstraper.t,
		name:      args.bootstraper.originalPodName,
		podName:   args.bootstraper.pod.Name,
		xact:      args.bootstraper.xact,
	}

	switch args.bootstraper.msg.CommType {
	case PushCommType:
		return &pushComm{
			baseComm: baseComm,
			mem:      args.bootstraper.t.PageMM(),
			uri:      args.bootstraper.uri,
		}
	case RedirectCommType:
		return &redirectComm{baseComm: baseComm, uri: args.bootstraper.uri}
	case RevProxyCommType:
		transformerURL, err := url.Parse(args.bootstraper.uri)
		cos.AssertNoErr(err)
		rp := &httputil.ReverseProxy{
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
		return &revProxyComm{baseComm: baseComm, rp: rp, uri: args.bootstraper.uri}
	case IOCommType:
		return &pushComm{
			baseComm: baseComm,
			mem:      args.bootstraper.t.PageMM(),
			uri:      args.bootstraper.uri,
			command:  args.bootstraper.originalCommand,
		}
	default:
		cos.AssertMsg(false, args.bootstraper.msg.CommType)
	}
	return nil
}

func (c baseComm) Name() string    { return c.name }
func (c baseComm) PodName() string { return c.podName }
func (c baseComm) SvcName() string { return c.podName /*pod name is same as service name*/ }

func (c baseComm) ObjCount() int64 { return c.xact.Objs() }
func (c baseComm) InBytes() int64  { return c.xact.InBytes() }
func (c baseComm) OutBytes() int64 { return c.xact.OutBytes() }

func (c *baseComm) Stop() {
	c.xact.Finish(nil)
}

//////////////
// pushComm //
//////////////

func (pc *pushComm) doRequest(bck *cluster.Bck, objName string, timeout time.Duration) (r cos.ReadCloseSizer, err error) {
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)

	if err := lom.Init(bck.Bck); err != nil {
		return nil, err
	}

	r, err = pc.tryDoRequest(lom, timeout)
	if err != nil && cmn.IsObjNotExist(err) && bck.IsRemote() {
		_, err = pc.t.GetCold(context.Background(), lom, cluster.PrefetchWait)
		if err != nil {
			return nil, err
		}
		r, err = pc.tryDoRequest(lom, timeout)
	}
	return
}

func (pc *pushComm) tryDoRequest(lom *cluster.LOM, timeout time.Duration) (cos.ReadCloseSizer, error) {
	if pc.xact.Aborted() {
		return nil, cmn.NewErrAborted(pc.xact.Name(), "try-request", nil)
	}

	lom.Lock(false)
	defer lom.Unlock(false)

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
		req    *http.Request
		resp   *http.Response
		cancel func()
	)
	if timeout != 0 {
		var ctx context.Context
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, pc.uri, fh)
	} else {
		req, err = http.NewRequest(http.MethodPut, pc.uri, fh)
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
	req.Header.Set(cmn.HdrContentType, cmn.ContentBinary)
	resp, err = pc.t.DataClient().Do(req) // nolint:bodyclose // Closed by the caller.
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
		ReadCb: func(i int, err error) { pc.xact.OutObjsAdd(1, int64(i)) },
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
			pc.xact.InObjsAdd(1, size)
		},
	}), nil
}

func (pc *pushComm) OnlineTransform(w http.ResponseWriter, _ *http.Request, bck *cluster.Bck, objName string) error {
	var (
		size   int64
		r, err = pc.doRequest(bck, objName, 0 /*timeout*/)
	)
	if err != nil {
		return err
	}
	defer r.Close()
	if size = r.Size(); size < 0 {
		size = memsys.DefaultBufSize // TODO: track the average
	}
	buf, slab := pc.mem.AllocSize(size)
	_, err = io.CopyBuffer(w, r, buf)
	slab.Free(buf)
	return err
}

func (pc *pushComm) OfflineTransform(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	return pc.doRequest(bck, objName, timeout)
}

//////////////////
// redirectComm //
//////////////////

func (rc *redirectComm) OnlineTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	if rc.xact.Aborted() {
		return cmn.NewErrAborted(rc.xact.Name(), "try-request", nil)
	}

	size, err := determineSize(bck, objName)
	if err != nil {
		return err
	}
	rc.xact.InObjsAdd(1, size)

	// TODO: Is there way to determine `rc.stats.outBytes`?
	redirectURL := cos.JoinPath(rc.uri, transformerPath(bck, objName))
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return nil
}

func (rc *redirectComm) OfflineTransform(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	size, err := determineSize(bck, objName)
	if err != nil {
		return nil, err
	}

	etlURL := cos.JoinPath(rc.uri, transformerPath(bck, objName))
	return rc.getWithTimeout(etlURL, size, timeout)
}

//////////////////
// revProxyComm //
//////////////////

func (pc *revProxyComm) OnlineTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	size, err := determineSize(bck, objName)
	if err != nil {
		return err
	}
	pc.xact.InObjsAdd(1, size)

	// TODO: Is there way to determine `rc.stats.outBytes`?
	path := transformerPath(bck, objName)
	r.URL.Path, _ = url.PathUnescape(path) // `Path` must be unescaped otherwise it will be escaped again.
	r.URL.RawPath = path                   // `RawPath` should be escaped version of `Path`.
	pc.rp.ServeHTTP(w, r)
	return nil
}

func (pc *revProxyComm) OfflineTransform(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	size, err := determineSize(bck, objName)
	if err != nil {
		return nil, err
	}

	etlURL := cos.JoinPath(pc.uri, transformerPath(bck, objName))
	return pc.getWithTimeout(etlURL, size, timeout)
}

//////////////
// cbWriter //
//////////////

func (cw *cbWriter) Write(b []byte) (n int, err error) {
	n, err = cw.w.Write(b)
	cw.writeCb(n)
	return
}

///////////
// utils //
///////////

// prune query (received from AIS proxy) prior to reverse-proxying the request to/from container -
// not removing cmn.URLParamUUID, for instance, would cause infinite loop.
func pruneQuery(rawQuery string) string {
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		glog.Errorf("failed to parse raw query %q, err: %v", rawQuery, err)
		return ""
	}
	for _, filtered := range []string{cmn.URLParamUUID, cmn.URLParamProxyID, cmn.URLParamUnixTime} {
		vals.Del(filtered)
	}
	return vals.Encode()
}

// TODO: Consider encoding bucket and object name without the necessity to escape.
func transformerPath(bck *cluster.Bck, objName string) string {
	return "/" + url.PathEscape(bck.MakeUname(objName))
}

func (c *baseComm) getWithTimeout(url string, size int64, timeout time.Duration) (r cos.ReadCloseSizer, err error) {
	if c.xact.Aborted() {
		return nil, cmn.NewErrAborted(c.xact.Name(), "try-request", nil)
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
	if err != nil {
		goto finish
	}
	resp, err = c.t.DataClient().Do(req) // nolint:bodyclose // Closed by the caller.
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
		ReadCb: func(i int, err error) { c.xact.OutObjsAdd(1, int64(i)) },
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
			c.xact.InObjsAdd(1, size)
		},
	}), nil
}

func determineSize(bck *cluster.Bck, objName string) (int64, error) {
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(bck.Bck); err != nil {
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
