// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
	for _, node := range smap.Tmap {
		if smap.InMaintOrDecomm(node) {
			continue
		}
		for _, ignoreNode := range ignore {
			if ignoreNode.Equals(node) {
				continue outer
			}
		}
		reqArgs := cmn.HreqArgs{
			Method: method,
			Base:   node.URL(cmn.NetIntraControl),
			Path:   path,
			Query:  urlParams,
			Body:   body,
		}
		wg.Add(1)
		go func(si *meta.Snode, args *cmn.HreqArgs, i int) {
			responses[i] = call(args)
			responses[i].si = si
			wg.Done()
		}(node, &reqArgs, idx)
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
	if err != nil {
		return response{err: err, statusCode: http.StatusInternalServerError}
	}
	out, err := io.ReadAll(resp.Body)
	cos.Close(resp.Body)
	return response{res: out, err: err, statusCode: resp.StatusCode}
}
