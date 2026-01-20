//go:build sharding

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"net/http"
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

//
// common code used by proxy _and_ target, both
//

var bcastClient *http.Client

func bcast(method, path string, urlParams url.Values, body []byte, smap *meta.Smap, ignore ...*meta.Snode) []response {
	var (
		idx       int
		responses = make([]response, smap.CountActiveTs())
		wg        = &sync.WaitGroup{}
	)
outer:
	for tid, tsi := range smap.Tmap {
		if smap.InMaintOrDecomm(tid) {
			continue
		}
		for _, ignoreNode := range ignore {
			if ignoreNode.Eq(tsi) {
				continue outer
			}
		}
		reqArgs := cmn.HreqArgs{
			Method: method,
			Base:   tsi.URL(cmn.NetIntraControl),
			Path:   path,
			Query:  urlParams,
			Body:   body,
		}
		wg.Add(1)
		go func(si *meta.Snode, args *cmn.HreqArgs, i int) {
			responses[i] = call(args)
			responses[i].si = si
			wg.Done()
		}(tsi, &reqArgs, idx)
		idx++
	}
	wg.Wait()

	return responses[:idx]
}

func call(reqArgs *cmn.HreqArgs) response {
	req, err := reqArgs.Req()
	if err != nil {
		return response{err: err, statusCode: http.StatusInternalServerError}
	}

	resp, err := bcastClient.Do(req) //nolint:bodyclose // Closed inside `cos.Close`.

	cmn.HreqFree(req)
	if err != nil {
		return response{err: err, statusCode: http.StatusInternalServerError}
	}
	out, err := cos.ReadAll(resp.Body)
	cos.Close(resp.Body)
	return response{res: out, err: err, statusCode: resp.StatusCode}
}
