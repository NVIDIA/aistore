// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	corev1 "k8s.io/api/core/v1"
)

type (
	CommStats interface {
		ObjCount() int64
		InBytes() int64
		OutBytes() int64
	}

	// Communicator is responsible for managing communications with local ETL container.
	// Do() gets executed as part of (each) GET bucket/object by the user.
	// Communicator listens to cluster membership changes and terminates ETL container,
	// if need be.
	Communicator interface {
		cluster.Slistener

		Name() string
		PodName() string
		SvcName() string

		// Do() uses one of the two ETL container endpoints:
		// - Method "PUT", Path "/"
		// - Method "GET", Path "/bucket/object"
		Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error

		// Get() interface implementations realize offline ETL.
		// Get() is driven by `OfflineDataProvider` - not to confuse with
		// GET requests from users (such as training models and apps)
		// to perform on-the-fly transformation.
		Get(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error)

		CommStats
	}

	commArgs struct {
		listener       cluster.Slistener
		t              cluster.Target
		pod            *corev1.Pod
		name           string
		commType       string
		transformerURL string
	}

	commStats struct {
		objCount atomic.Int64
		inBytes  atomic.Int64
		outBytes atomic.Int64
	}

	baseComm struct {
		cluster.Slistener
		t cluster.Target

		name    string
		podName string

		transformerURL string

		stats *commStats
	}

	pushComm struct {
		baseComm
		mem *memsys.MMSA
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

func makeCommunicator(args commArgs) Communicator {
	baseComm := baseComm{
		Slistener:      args.listener,
		t:              args.t,
		name:           args.name,
		podName:        args.pod.GetName(),
		transformerURL: args.transformerURL,

		stats: &commStats{},
	}

	switch args.commType {
	case PushCommType:
		return &pushComm{baseComm: baseComm, mem: args.t.MMSA()}
	case RedirectCommType:
		return &redirectComm{baseComm: baseComm}
	case RevProxyCommType:
		transURL, err := url.Parse(baseComm.transformerURL)
		cos.AssertNoErr(err)
		rp := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// Replacing the `req.URL` host with ETL container host
				req.URL.Scheme = transURL.Scheme
				req.URL.Host = transURL.Host
				req.URL.RawQuery = pruneQuery(req.URL.RawQuery)
				if _, ok := req.Header["User-Agent"]; !ok {
					// Explicitly disable `User-Agent` so it's not set to default value.
					req.Header.Set("User-Agent", "")
				}
			},
		}
		return &revProxyComm{baseComm: baseComm, rp: rp}
	default:
		cos.AssertMsg(false, args.commType)
	}
	return nil
}

func (c baseComm) Name() string    { return c.name }
func (c baseComm) PodName() string { return c.podName }
func (c baseComm) SvcName() string { return c.podName /*pod name is same as service name*/ }

func (c baseComm) ObjCount() int64 { return c.stats.objCount.Load() }
func (c baseComm) InBytes() int64  { return c.stats.inBytes.Load() }
func (c baseComm) OutBytes() int64 { return c.stats.outBytes.Load() }

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
	lom.Lock(false)
	defer lom.Unlock(false)

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil, err
	}

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
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, pc.transformerURL, fh)
	} else {
		req, err = http.NewRequest(http.MethodPut, pc.transformerURL, fh)
	}
	if err != nil {
		cos.Close(fh)
		goto finish
	}

	req.ContentLength = lom.SizeBytes()
	req.Header.Set(cmn.HdrContentType, cmn.ContentBinary)
	resp, err = pc.t.DataClient().Do(req) // nolint:bodyclose // Closed by the caller.
finish:
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, err
	}

	pc.stats.inBytes.Add(lom.SizeBytes())
	return cos.NewReaderWithArgs(cos.ReaderArgs{
		R:      resp.Body,
		Size:   resp.ContentLength,
		ReadCb: func(i int, err error) { pc.stats.outBytes.Add(int64(i)) },
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
			pc.stats.objCount.Inc()
		},
	}), nil
}

func (pc *pushComm) Do(w http.ResponseWriter, _ *http.Request, bck *cluster.Bck, objName string) error {
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
	buf, slab := pc.mem.Alloc(size)
	_, err = io.CopyBuffer(w, r, buf)
	slab.Free(buf)
	return err
}

func (pc *pushComm) Get(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	return pc.doRequest(bck, objName, timeout)
}

//////////////////
// redirectComm //
//////////////////

func (rc *redirectComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	size, err := determineSize(bck, objName)
	if err != nil {
		return err
	}
	rc.stats.inBytes.Add(size)

	// TODO: Is there way to determine `rc.stats.outBytes`?
	redirectURL := cos.JoinPath(rc.transformerURL, transformerPath(bck, objName))
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return nil
}

func (rc *redirectComm) Get(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	size, err := determineSize(bck, objName)
	if err != nil {
		return nil, err
	}
	rc.stats.inBytes.Add(size)

	etlURL := cos.JoinPath(rc.transformerURL, transformerPath(bck, objName))
	return rc.getWithTimeout(etlURL, timeout)
}

//////////////////
// revProxyComm //
//////////////////

func (pc *revProxyComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	size, err := determineSize(bck, objName)
	if err != nil {
		return err
	}
	pc.stats.inBytes.Add(size)

	// TODO: Is there way to determine `rc.stats.outBytes`?
	path := transformerPath(bck, objName)
	r.URL.Path, _ = url.PathUnescape(path) // `Path` must be unescaped otherwise it will be escaped again.
	r.URL.RawPath = path                   // `RawPath` should be escaped version of `Path`.
	pc.rp.ServeHTTP(w, r)
	return nil
}

func (pc *revProxyComm) Get(bck *cluster.Bck, objName string, timeout time.Duration) (cos.ReadCloseSizer, error) {
	size, err := determineSize(bck, objName)
	if err != nil {
		return nil, err
	}
	pc.stats.inBytes.Add(size)

	etlURL := cos.JoinPath(pc.transformerURL, transformerPath(bck, objName))
	return pc.getWithTimeout(etlURL, timeout)
}

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

func (c *baseComm) getWithTimeout(url string, timeout time.Duration) (r cos.ReadCloseSizer, err error) {
	var (
		req    *http.Request
		resp   *http.Response
		cancel func()
	)
	if timeout != 0 {
		var ctx context.Context
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	} else {
		req, err = http.NewRequest(http.MethodGet, url, nil)
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
		ReadCb: func(i int, err error) { c.stats.outBytes.Add(int64(i)) },
		DeferCb: func() {
			if cancel != nil {
				cancel()
			}
			c.stats.objCount.Inc()
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
