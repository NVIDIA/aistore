// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"

	jsoniter "github.com/json-iterator/go"
)

//
// proxy --------------------------------------------
//

func (p *proxy) bgetNBI(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, am actMsgRaw, dpq *dpq) {
	if !qbck.IsBucket() {
		p.writeErrf(w, r, "bad %s request: %q is not a bucket", am.msg.Action, qbck.String())
		return
	}
	// bucket
	bck := meta.CloneBck((*cmn.Bck)(qbck))
	bckArgs := allocBctx()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.msg = am.msg
		bckArgs.perms = apc.AceBckHEAD
		bckArgs.bck = bck
		bckArgs.dpq = dpq
		bckArgs.createAIS = false
	}
	bck, err := bckArgs.initAndTry()
	freeBctx(bckArgs)
	if err != nil {
		return
	}

	// bcast
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   r.URL.Path,
		Body:   am.body,
		Header: http.Header{cos.HdrContentType: []string{cos.ContentJSON}},
	}
	args.req.Query = bck.AddToQuery(nil)
	results := p.bcastGroup(args)
	freeBcArgs(args)

	// merge results
	merged := make(apc.NBIInfoMap)
	for _, res := range results {
		if res.err != nil {
			err := res.toErr()
			freeBcastRes(results)
			p.writeErr(w, r, err)
			return
		}

		var infos apc.NBIInfoMap
		if err := jsoniter.Unmarshal(res.bytes, &infos); err != nil {
			freeBcastRes(results)
			p.writeErr(w, r, err)
			return
		}
		for k, info := range infos {
			existing, ok := merged[k]
			if !ok {
				merged[k] = info
				continue
			}
			existing.Size += info.Size

			if info.Started != 0 && (existing.Started == 0 || info.Started < existing.Started) {
				existing.Started = info.Started
			}
			if info.Finished > existing.Finished {
				existing.Finished = info.Finished
			}
			debug.Assertf(existing.Prefix == info.Prefix, "nbi %q prefix mismatch: %q != %q", info.Name, existing.Prefix, info.Prefix)
		}
	}
	freeBcastRes(results)

	p.writeJSON(w, r, merged, am.msg.Action)
}

// currently, create/destroy inventory requires admin perm
func (p *proxy) destroyNBI(w http.ResponseWriter, r *http.Request, bck *meta.Bck, am actMsgRaw) {
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: r.Method, Path: r.URL.Path, Body: am.body}
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err := res.toErr()
		freeBcastRes(results)
		p.writeErr(w, r, err)
		return
	}
	nlog.Infoln(am.msg.Action, "for bucket", bck.Cname(""), "[", am.msg.Name, "]")
	freeBcastRes(results)
}
