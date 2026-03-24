// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"maps"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"

	jsoniter "github.com/json-iterator/go"
)

//
// proxy --------------------------------------------
//

func (p *proxy) bgetNBI(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *apc.ActMsg, dpq *dpq) {
	// bucket
	var bck *meta.Bck
	if qbck.IsBucket() {
		bck = meta.CloneBck((*cmn.Bck)(qbck))
		var err error
		bckArgs := allocBctx()
		{
			bckArgs.p = p
			bckArgs.w = w
			bckArgs.r = r
			bckArgs.msg = msg
			bckArgs.perms = apc.AceBckHEAD
			bckArgs.bck = bck
			bckArgs.dpq = dpq
			bckArgs.createAIS = false
		}
		bck, err = bckArgs.initAndTry()
		freeBctx(bckArgs)
		if err != nil {
			return
		}
	} else {
		bck = (*meta.Bck)(qbck) // keep as is for subsequent bmd.Range()
	}

	// bcast
	args := allocBcArgs()
	amsg := p.newAmsg(msg, nil /*bmd*/)
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   r.URL.Path,
		Body:   cos.MustMarshal(amsg),
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
			existing.Ntotal += info.Ntotal
			existing.Chunks += info.Chunks

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

	p.writeJSON(w, r, merged, msg.Action)
}

// currently, create/destroy inventory requires admin perm
func (p *proxy) destroyNBI(w http.ResponseWriter, r *http.Request, bck *meta.Bck, am actMsgRaw) {
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: r.Method, Path: r.URL.Path, Body: am.body}
	args.req.Query = bck.AddToQuery(nil)
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

//
// target ------------------------------------------------------
//

func (t *target) bgetNBI(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *actMsgExt, dpq *dpq) {
	// 1. specific bucket
	if qbck.IsBucket() {
		bck, err := t._resolveQbck(w, r, qbck, dpq.dontAddRemote)
		if err != nil {
			return
		}
		info, err := fs.CollectNBI(bck.Bucket())
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		if msg.Name == "" {
			t.writeJSON(w, r, info, msg.Action)
			return
		}

		// filter by inv-name
		for k, one := range info {
			if one.Name == msg.Name {
				info = apc.NBIInfoMap{k: one}
				t.writeJSON(w, r, info, msg.Action) // found
				return
			}
		}

		info = make(apc.NBIInfoMap) // not found
		t.writeJSON(w, r, info, msg.Action)
		return
	}

	// 2. all buckets matching qbck filter
	var (
		bmd      = t.owner.bmd.get()
		merged   apc.NBIInfoMap
		provider *string
		ns       *cmn.Ns
	)
	if qbck.Provider != "" {
		provider = &qbck.Provider
	}
	if !qbck.Ns.IsGlobal() {
		ns = &qbck.Ns
	}
	bmd.Range(provider, ns, func(bck *meta.Bck) bool {
		info, err := fs.CollectNBI(bck.Bucket())
		if err != nil || len(info) == 0 {
			return false
		}
		if merged == nil {
			merged = make(apc.NBIInfoMap)
		}
		if msg.Name == "" {
			maps.Copy(merged, info)
			return false
		}
		// filter by inv-name
		for k, one := range info {
			if one.Name == msg.Name {
				merged[k] = one
				break
			}
		}
		return false
	})
	if merged == nil {
		merged = make(apc.NBIInfoMap)
	}
	t.writeJSON(w, r, merged, msg.Action)
}

func (t *target) destroyNBI(w http.ResponseWriter, r *http.Request, bck *meta.Bck, msg *actMsgExt) {
	nlp := newBckNLP(bck)
	if !nlp.TryLock(cmn.Rom.MaxKeepalive()) {
		t.writeErr(w, r, cmn.NewErrBusy("bucket", bck.Cname("")))
		return
	}
	err := fs.DestroyNBI(msg.Action, bck.Bucket(), msg.Name /*invName*/)
	nlp.Unlock()
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	nlog.Infoln(msg.Action, "for bucket", bck.Cname(""), "[", msg.Name, "]")
}
