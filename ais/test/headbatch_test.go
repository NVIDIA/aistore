// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

const hdbNumObjs = 8 // per bucket

// one T2T request with objects from two buckets
func TestHeadBatchT2T(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	var (
		bp   = tools.BaseAPIParams(proxyURL)
		smap = tools.GetClusterMap(t, proxyURL)
		bcks = []cmn.Bck{
			{Name: "hdb-" + trand.String(6), Provider: apc.AIS},
			{Name: "hdb-" + trand.String(6), Provider: apc.AIS},
		}
		req  cmn.HdbReq
		want []apc.HdbStatus
	)
	config, err := api.GetClusterConfig(bp)
	tassert.CheckFatal(t, err)
	if config.Auth.IntraRequestAuthConfigured() {
		t.Skip("cannot sign intra-cluster requests")
	}
	tsi, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	// objects that tsi owns, in both buckets (names generated for tsi: no dependency on HRW distribution)
	for i := range bcks {
		bck := &bcks[i]
		for _, name := range hdbPutObjs(t, bp, bck, smap, tsi) {
			props := apc.JoinProps(apc.GetPropsChecksum, apc.GetPropsAtime, apc.GetPropsVersion, apc.GetPropsCustom)
			op, err := api.HeadObjectV2(bp, *bck, name, props, api.HeadArgs{})
			tassert.CheckFatal(t, err)
			req.In = append(req.In, cmn.HdbIn{ObjAttrs: op.ObjAttrs, Name: name, Bidx: req.AddBck(bck)})
			want = append(want, apc.HdbSame)
		}
	}

	diverged := req.In[0]
	diverged.Size++
	nobck := cmn.Bck{Name: "hdb-none-" + trand.String(6), Provider: apc.AIS}
	req.In = append(req.In,
		diverged,
		cmn.HdbIn{Name: "hdb-missing", Bidx: 0},
		cmn.HdbIn{Name: "hdb-any", Bidx: req.AddBck(&nobck)},
	)
	want = append(want, apc.HdbDiverged, apc.HdbMissing, apc.HdbFailed)
	tassert.CheckFatal(t, req.Validate())

	status, body := hdbPost(t, tsi, smap.Primary, req.NewPack())
	tassert.Fatalf(t, status == http.StatusOK, "head-batch to %s: status %d (%s)", tsi.StringEx(), status, body)
	var resp apc.HdbResp
	tassert.CheckFatal(t, resp.Unpack(cos.NewUnpacker(body)))
	tassert.CheckFatal(t, resp.Validate(len(req.In)))
	for i := range want {
		in := &req.In[i]
		tassert.Errorf(t, resp.Status[i] == want[i], "%s: %s, expecting %s (%s)",
			req.Bcks[in.Bidx].Cname(in.Name), resp.Status[i], want[i], resp.Cause(i))
	}

	status, body = hdbPost(t, tsi, nil /*sender*/, req.NewPack())
	tassert.Errorf(t, status == http.StatusBadRequest && bytes.Contains(body, []byte("not an intra-control request")),
		"expecting intra-control rejection, got status %d (%s)", status, body)
}

// put hdbNumObjs objects that HRW places on tsi
func hdbPutObjs(t *testing.T, bp api.BaseParams, bck *cmn.Bck, smap *meta.Smap, tsi *meta.Snode) []string {
	tools.CreateBucket(t, proxyURL, *bck, nil, true /*cleanup*/)
	p, err := api.HeadBucket(bp, *bck, false /*don't add*/)
	tassert.CheckFatal(t, err)
	names := make([]string, 0, hdbNumObjs)
	for i := range hdbNumObjs {
		name := tools.GenerateObjectNameOnTarget(fmt.Sprintf("hdb-%d-", i), *bck, smap, tsi)
		tassert.CheckFatal(t, tools.PutObjRR(bp, *bck, name, cos.KiB, p.Cksum.Type))
		names = append(names, name)
	}
	return names
}

// POST /v1/objects to the intra-control endpoint of tsi
func hdbPost(t *testing.T, tsi, sender *meta.Snode, body []byte) (int, []byte) {
	url := tsi.URL(cmn.NetIntraControl) + apc.URLPathObjects.S
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	tassert.CheckFatal(t, err)
	req.Header.Set(cos.HdrContentType, cos.ContentBinary)
	if sender != nil {
		req.Header.Set(apc.HdrSenderID, sender.ID())
		req.Header.Set(apc.HdrSenderName, sender.StringEx())
	}
	resp, err := tools.BaseAPIParams(url).Client.Do(req)
	tassert.CheckFatal(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	tassert.CheckFatal(t, err)
	return resp.StatusCode, out
}
