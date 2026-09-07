// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
)

// List-objects call flow:
//
// Native: bgetObjects => initAndTry => listObjects => lsOwner
// S3:     listObjectsS3 => lsOwner
//
// lsOwner: assign/validate UUID => HRW-select proxy
//   self:  execute page directly
//   peer:  forwardLSO => reverseRequest => owner.bckCtrlHandler => execute page
//
// Execute page:
//   Native: lsNativePage => lsPage
//   S3:     lsS3Page => lsPageS3 => lsPage
//
// Owner formats the response; forwarding proxy relays it unchanged.
// Subsequent pages retain UUID and repeat the same owner selection.
//
// Special case:
// - when remote bucket is not in the BMD - forwardCP as usual
//   (an extra hop is a MUST, but only once)

type (
	// list-objects flow control
	lsofcRes struct {
		tsi            *meta.Snode // designated target for R-flow; nil for A-flow
		listRemote     bool        // R-flow vs A-flow
		wantOnlyRemote bool        // when listRemote: do not populate with AIS metadata
	}

	// (forwardLSO machinery)
	lsoReq struct {
		Bck     cmn.Bck     `json:"bck"`
		LsoMsg  *apc.LsoMsg `json:"lso"`
		Props   *cmn.Bprops `json:"props,omitempty"`
		S3Token *string     `json:"s3_token,omitempty"` // nil for native; original S3 token otherwise
		New     bool        `json:"new,omitempty"`      // first request, even though UUID is now assigned
	}
)

// one page => msgpack rsp
func (p *proxy) listObjects(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lsmsg *apc.LsoMsg) {
	// LsVerChanged a.k.a. '--check-versions' limitations
	if lsmsg.IsFlagSet(apc.LsDiff) {
		if err := _checkVerChanged(bck, lsmsg); err != nil {
			p.statsT.IncBck(stats.ErrListCount, bck.Bucket())
			p.writeErr(w, r, err)
			return
		}
	}

	// default props & flags => user-provided message
	lsmsg.NormalizeNameSizeDflt()

	if lsmsg.IsFlagSet(apc.LsArchDir) {
		lsmsg.SetFlag(apc.LsCached)
	}

	smap := p.owner.smap.get()
	psi, newls, err := p.lsOwner(lsmsg, smap)
	if err != nil {
		p.statsT.IncBck(stats.ErrListCount, bck.Bucket())
		p.writeErr(w, r, err)
		return
	}
	if psi != nil {
		p.forwardLSO(w, r, bck, lsmsg, psi, smap, newls, nil)
		return
	}
	p.lsNativePage(w, r, bck, lsmsg, smap, newls)
}

// native API: execute p.lsPage locally or on behalf of a peer
func (p *proxy) lsNativePage(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lsmsg *apc.LsoMsg,
	smap *smapX, newls bool) {
	beg := mono.NanoTime()
	lst, err := p.lsPage(bck, lsmsg, r.Header, smap, newls)
	if err != nil {
		p.statsT.IncBck(stats.ErrListCount, bck.Bucket())
		p.writeErr(w, r, err)
		return
	}

	vlabs := map[string]string{stats.VlabBucket: bck.Cname("")}
	p.statsT.IncWith(stats.ListCount, vlabs)
	p.statsT.AddWith(
		cos.NamedVal64{Name: stats.ListLatency, Value: mono.SinceNano(beg), VarLabs: vlabs},
	)

	var ok bool
	if cos.AcceptsMsgPack(r.Header) {
		ok = p.writeMsgPack(w, lst, lsotag)
	} else {
		ok = p.writeJS(w, r, lst, lsotag)
	}
	if !ok && cmn.Rom.V(4, cos.ModAIS) {
		nlog.Errorln("failed to transmit list-objects page (TCP RST?)")
	}

	// GC
	clear(lst.Entries)
	lst.Entries = nil
}

func _checkVerChanged(bck *meta.Bck, lsmsg *apc.LsoMsg) error {
	const a = "cannot perform remote versions check (or diff vs remote bucket)"
	if !bck.HasVersioningMD() {
		return errors.New(a + ": bucket " + bck.Cname("") + " does not provide remote versioning info")
	}
	if lsmsg.IsFlagSet(apc.LsNameOnly) || lsmsg.IsFlagSet(apc.LsNameSize) || !lsmsg.WantProp(apc.GetPropsCustom) {
		return fmt.Errorf(a+" without listing %q (object property)", apc.GetPropsCustom)
	}
	if lsmsg.IsFlagSet(apc.LsNotCached) {
		return errors.New(a + " when apc.LsNotCached (CLI '--not-cached') is set")
	}
	if lsmsg.IsFlagSet(apc.LsCached) {
		return errors.New(a + " when apc.LsCached (CLI '--cached') is set")
	}
	return nil
}

// next page: common execution code (native and s3 API, both) - post-routing
func (p *proxy) lsPage(bck *meta.Bck, lsmsg *apc.LsoMsg, hdr http.Header,
	smap *smapX, newls bool) (*cmn.LsoRes, error) {
	var lst *cmn.LsoRes
	fc, err := p._lsofc(bck, lsmsg, smap)
	if err != nil {
		return nil, err
	}

	if fc.listRemote {
		// R-flow
		if lsmsg.StartAfter != "" {
			// TODO: remote AIS first, then Cloud
			return nil, fmt.Errorf("%s option --start_after (%s) not yet supported for remote buckets (%s)",
				lsotag, lsmsg.StartAfter, bck)
		}
		// verbose log
		if cmn.Rom.V(4, cos.ModAIS) {
			var s string
			if lsmsg.ContinuationToken != "" {
				s = " cont=" + lsmsg.ContinuationToken
			}
			if lsmsg.SID != "" {
				s += " via " + fc.tsi.StringEx()
			}
			nlog.Infoln(apc.ActList, "[", lsmsg.UUID, "]", bck.Cname(""), s)
		}

		config := cmn.GCO.Get()
		lst, err = p.lsObjsR(bck, lsmsg, hdr, smap, fc.tsi, config, fc.wantOnlyRemote, newls)

		// TODO `status == http.StatusGone`: at this point we know that this
		// remote bucket exists and is offline. We should somehow try to list
		// cached objects. This isn't easy as we basically need to start a new
		// xaction and return a new `UUID`.
	} else {
		// A-flow
		lst, err = p.lsObjsA(bck, lsmsg, hdr, smap)
	}

	return lst, err
}

// new request: assign x-lso UUID; otherwise use existing
// map to `psi` owner
// return nil owner for local execution
func (p *proxy) lsOwner(lsmsg *apc.LsoMsg, smap *smapX) (psi *meta.Snode, newls bool, err error) {
	if lsmsg.UUID == "" {
		lsmsg.UUID = cos.GenUUID()
		newls = true
	} else if !cos.IsValidUUID(lsmsg.UUID) {
		return nil, false, fmt.Errorf("%s: invalid UUID %q", apc.BadLsoRequest, lsmsg.UUID)
	}
	if smap == nil || !smap.isValid() {
		return nil, newls, &cmn.ErrHTTP{Status: http.StatusServiceUnavailable, Message: "cannot route list-objects: invalid Smap"}
	}
	psi, err = smap.HrwProxyTask(lsmsg.UUID)
	if err == nil && psi.ID() == p.SID() {
		psi = nil
	}
	return psi, newls, err
}

// forward x-lso next-page request to x-lso owner
func (p *proxy) forwardLSO(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lsmsg *apc.LsoMsg,
	psi *meta.Snode, smap *smapX, newls bool, token *string) {
	debug.AssertFunc(func() bool { return psi.ID() != p.SID() }, "reversing to self")

	msg := lsoReq{Bck: bck.Clone(), LsoMsg: lsmsg, New: newls, S3Token: token}
	if bck.Props.BID == 0 {
		msg.Props = bck.Props
	}
	body := cos.MustMarshal(&msg)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set(cos.HdrContentType, cos.ContentJSON)
	r.URL.Path = apc.URLPathBuckets.Join(bck.Name)
	r.URL.RawPath = ""
	r.URL.RawQuery = ""
	r.URL.ForceQuery = false
	p.setIntraHdrs(r, smap, true /*peer present*/)

	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infoln(p.String(), lsotag, "[", lsmsg.UUID, "] =>", psi.StringEx())
	}

	var errHdlr stdlibErrHdlr
	if token != nil {
		errHdlr = rpErrHandlerS3
	}
	p.reverseRequest(w, r, psi.ID(), psi.URL(cmn.NetIntraControl), errHdlr)
}

// Only forwardLSO enters here. The sending proxy has initialized the bucket and
// checked client access; do not re-enter public dispatch or route the page again.
func (p *proxy) bckCtrlHandler(w http.ResponseWriter, r *http.Request) {
	if !p.cluStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodGet {
		cmn.WriteErr405(w, r, http.MethodGet)
		return
	}
	if ecode, err := p.checkIntra(r, false /*only primary*/); err != nil {
		p.writeErr(w, r, err, ecode)
		return
	}

	smap := p.owner.smap.get()
	if smap == nil || smap.GetProxy(r.Header.Get(apc.HdrSenderID)) == nil {
		p.writeErrMsg(w, r, "list-objects: expected proxy sender", http.StatusForbidden)
		return
	}
	items, err := p.parseURL(w, r, apc.URLPathBuckets.L, 1, false)
	if err != nil {
		return
	}

	var msg lsoReq
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	if msg.LsoMsg == nil || !cos.IsValidUUID(msg.LsoMsg.UUID) || msg.LsoMsg.PageSize < 0 || msg.Bck.Name != items[0] {
		p.writeErrMsg(w, r, "invalid forwarded list-objects request", http.StatusBadRequest)
		return
	}

	// Preserve prepared prefixes and first-request state. Only unregistered remote
	// buckets carry properties; all registered buckets use the receiving proxy's BMD.
	bck := (*meta.Bck)(&msg.Bck)
	if err = bck.Validate(); err == nil {
		switch {
		case msg.Props == nil:
			err = bck.Init(p.owner.bmd)
		case !bck.IsRemote() || msg.Props.BID != 0 || !msg.LsoMsg.IsFlagSet(apc.LsDontAddRemote):
			err = errors.New("invalid properties in forwarded list-objects request")
		default:
			bck.Props = msg.Props
		}
	}
	if err != nil {
		p.statsT.IncBck(stats.ErrListCount, bck.Bucket())
		if msg.S3Token != nil {
			s3.WriteErr(w, r, s3.ErrInfo{Err: err})
		} else {
			p.writeErr(w, r, err)
		}
		return
	}

	if msg.S3Token != nil {
		p.lsS3Page(w, r, bck, msg.LsoMsg, smap, msg.New, *msg.S3Token)
	} else {
		p.lsNativePage(w, r, bck, msg.LsoMsg, smap, msg.New)
	}
}

// list-objects: flow control helper
// - decide: R-flow or A-flow
// - designate target for R-flow, etc.
func (p *proxy) _lsofc(bck *meta.Bck, lsmsg *apc.LsoMsg, smap *smapX) (lsofcRes, error) {
	switch {
	case lsmsg.IsFlagSet(apc.LsNBI):
		var fc lsofcRes
		if err := lsmsg.ValidateNBI(); err != nil {
			e := fmt.Errorf("%s: the request to list via native bucket inventory has invalid or unsupported flags: %v", bck.Cname(""), err)
			return fc, e
		}
		// listing native bucket inventory is always A-flow:
		// - each target enumerates its local inventory chunks
		// - proxy merges and paginates the result
		return fc, nil

	case !bck.IsRemote() || lsmsg.IsFlagSet(apc.LsCached):
		var fc lsofcRes
		if lsmsg.IsFlagSet(apc.LsNotCached) {
			return fc, fmt.Errorf("%s is not a remote bucket - cannot list 'not cached' objects", bck.Cname(""))
		}
		return fc, nil

	default:
		return p._lsofcRemote(bck, lsmsg, smap)
	}
}

func (p *proxy) _lsofcRemote(bck *meta.Bck, lsmsg *apc.LsoMsg, smap *smapX) (fc lsofcRes, err error) {
	debug.AssertFunc(func() bool { return bck.IsRemote() })
	debug.AssertFunc(func() bool { return !lsmsg.IsFlagSet(apc.LsCached) })

	fc.listRemote = true

	// remote bucket outside cluster (not in BMD) that hasn't been added ("on the fly") by the caller
	// (lsmsg flag below)
	if bck.Props.BID == 0 {
		debug.AssertFunc(func() bool { return lsmsg.IsFlagSet(apc.LsDontAddRemote) })
		fc.wantOnlyRemote = true
		if !lsmsg.WantOnlyRemoteProps() {
			err := fmt.Errorf("cannot list remote and not-in-cluster bucket %s for not-only-remote object properties: %q",
				bck.Cname(""), lsmsg.Props)
			return fc, err
		}
	} else {
		fc.wantOnlyRemote = lsmsg.WantOnlyRemoteProps()
	}

	// check previously designated target vs Smap
	if lsmsg.SID != "" {
		return p._lsofcSID(lsmsg, smap, fc.wantOnlyRemote)
	}

	// designate one target to carry-out backend.list-objects
	fc.tsi, err = smap.HrwTargetTask(lsmsg.UUID)
	if err == nil {
		lsmsg.SID = fc.tsi.ID()
	}
	return fc, err
}

func (p *proxy) _lsofcSID(lsmsg *apc.LsoMsg, smap *smapX, wantOnlyRemote bool) (fc lsofcRes, err error) {
	fc.listRemote = true
	fc.wantOnlyRemote = wantOnlyRemote
	fc.tsi = smap.GetTarget(lsmsg.SID)
	if fc.tsi == nil || fc.tsi.InMaintOrDecomm() {
		err = &errNodeNotFound{si: p.si, smap: smap, msg: lsotag + " failure:", id: lsmsg.SID}
		nlog.Errorln(err)
		if smap.CountActiveTs() == 1 {
			// (walk an extra mile)
			orig := err
			fc.tsi, err = smap.HrwTargetTask(lsmsg.UUID)
			if err == nil {
				nlog.Warningf("ignoring [%v] - utilizing the last (or the only) active target %s", orig, fc.tsi)
				lsmsg.SID = fc.tsi.ID()
			}
		}
	}
	return fc, err
}

// A-flow:
// - bcast list-objects to all targets;
// - combine, sort and return a merged and sorted result
func (p *proxy) lsObjsA(bck *meta.Bck, lsmsg *apc.LsoMsg, hdr http.Header, smap *smapX) (allEntries *cmn.LsoRes, err error) {
	var (
		actMsgExt *actMsgExt
		args      *bcastArgs
		results   sliceResults
		isNBI     = lsmsg.IsFlagSet(apc.LsNBI)
	)
	if lsmsg.PageSize == 0 && !isNBI {
		lsmsg.PageSize = apc.MaxPageSizeAIS
	}

	actMsgExt = p.newAmsgActVal(apc.ActList, &lsmsg)
	args = allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(bck.Name),
		Header: hdr,
		Query:  bck.NewQuery(),
		Body:   cos.MustMarshal(actMsgExt),
	}
	args.timeout = apc.LongTimeout
	args.smap = smap
	args.cresv = cresmGeneric[cmn.LsoRes]{}

	// Combine the results.
	results = p.bcastGroup(args)
	freeBcArgs(args)
	var (
		lists   = make([]*cmn.LsoRes, 0, len(results))
		hasMore bool
	)
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(apc.ActList, lsmsg.UUID)
			}
			err = res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		lst := res.v.(*cmn.LsoRes)
		hasMore = hasMore || lst.ContinuationToken != ""
		if len(lst.Entries) > 0 || (isNBI && lst.ContinuationToken != "") {
			lists = append(lists, lst)
		}
	}
	freeBcastRes(results)

	if isNBI {
		page := finLsoNBI(lists, lsmsg)
		return page, nil
	}

	page := concatLso(lists, lsmsg)
	finLsoA(page, lsmsg, hasMore)
	return page, nil
}

// R-flow:
//   - call designated target to list remote source;
//   - when wantOnlyRemote: just return the next page
//   - otherwise, use intra-cluster streams to share the latter
//     for subsequent local filtering and adding local metadata (`filterAddLmeta`)
func (p *proxy) lsObjsR(bck *meta.Bck, lsmsg *apc.LsoMsg, hdr http.Header, smap *smapX, dt *meta.Snode, config *cmn.Config,
	wantOnlyRemote, newls bool) (*cmn.LsoRes, error) {
	var (
		nat           int
		phasedStartup bool
	)
	if newls && !wantOnlyRemote {
		nat = smap.CountActiveTs()
		if nat > 1 {
			// R-flow startup: in multi-target clusters, initialize all non-DT
			// receive paths before calling the DT. The DT performs backend ListObjects
			// and starts distributing listed pages - receivers must be ready.
			phasedStartup = true
		}
	}

	var (
		results   sliceResults
		actMsgExt = p.newAmsgActVal(apc.ActList, &lsmsg)
		bargs     = allocBcArgs()
		timeout   = config.Client.ListObjTimeout.D()
	)
	bargs.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(bck.Name),
		Header: hdr,
		Query:  bck.NewQuery(),
		Body:   cos.MustMarshal(actMsgExt),
	}
	bargs.timeout = timeout
	bargs.smap = smap
	bargs.cresv = cresmGeneric[cmn.LsoRes]{}
	bargs.network = cmn.NetIntraControl // note: targets => proxy to merge pages - over control-net
	bargs.allAtOnce = true

	switch {
	case wantOnlyRemote:
		cargs := allocCargs()
		{
			cargs.si = dt
			cargs.req = bargs.req
			cargs.cresv = bargs.cresv
			cargs.timeout = timeout // config.Client default
		}
		// duplicate via query to have target ignoring an (early) failure to initialize bucket
		if lsmsg.IsFlagSet(apc.LsDontHeadRemote) {
			cargs.req.Query.Set(apc.QparamDontHeadRemote, "true")
		}
		if lsmsg.IsFlagSet(apc.LsDontAddRemote) {
			cargs.req.Query.Set(apc.QparamDontAddRemote, "true")
		}
		res := p.call(cargs, smap)
		freeCargs(cargs)
		results = make(sliceResults, 1)
		results[0] = res

	case phasedStartup:
		if !p.ClusterStarted() {
			i := http.StatusServiceUnavailable
			return nil, &cmn.ErrHTTP{Message: http.StatusText(i), Status: i}
		}
		cresv, path := bargs.cresv, bargs.req.Path
		bargs.cresv = nil
		bargs.req.Path += "/" + apc.Begin2PC
		bargs.timeout = cmn.Rom.MaxKeepalive()
		results = p.bcastGroup(bargs)
		for _, res := range results {
			if res.err == nil {
				continue
			}
			err := res.toErr()
			freeBcastRes(results)

			p._abrtR(bargs, path)

			freeBcArgs(bargs)
			return nil, err
		}
		freeBcastRes(results)

		smapCurr := p.owner.smap.Get()
		if err := smap.CheckSameTargets(smapCurr, lsotag+" R-flow"); err != nil {
			p._abrtR(bargs, path)
			freeBcArgs(bargs)
			return nil, err
		}

		bargs.cresv = cresv
		bargs.req.Path = path + "/" + apc.Commit2PC
		bargs.timeout = timeout // config.Client default
		fallthrough
	default:
		results = p.bcastGroup(bargs)
	}

	freeBcArgs(bargs)

	var (
		lists     = make([]*cmn.LsoRes, 0, len(results))
		nextToken string
	)
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(apc.ActList, lsmsg.UUID)
			}
			err := res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		lst := res.v.(*cmn.LsoRes)
		debug.Assert(nextToken == "" || nextToken == lst.ContinuationToken)
		nextToken = lst.ContinuationToken
		lists = append(lists, lst)
	}
	freeBcastRes(results)

	page := concatLso(lists, lsmsg)
	page.ContinuationToken = nextToken
	return page, nil
}

// (best-effort, async)
func (p *proxy) _abrtR(bargs *bcastArgs, path string) {
	bargs.noResults = true
	bargs.req.Path = path + "/" + apc.Abort2PC // mutates bargs w/ no reuse
	p.bcastGroup(bargs)                        // TODO: propagate cause (and cover prx/tgttxn aborts as well)
}

//
// list-objects helpers
//

func concatLso(lists []*cmn.LsoRes, lsmsg *apc.LsoMsg) (objs *cmn.LsoRes) {
	objs = &cmn.LsoRes{
		UUID: lsmsg.UUID,
	}
	if len(lists) == 0 {
		return objs
	}

	var entryCount int
	for _, l := range lists {
		objs.Flags |= l.Flags
		entryCount += len(l.Entries)
	}
	if entryCount == 0 {
		return objs
	}
	objs.Entries = make(cmn.LsoEntries, 0, entryCount)
	for _, l := range lists {
		objs.Entries = append(objs.Entries, l.Entries...)
		clear(l.Entries)
	}

	// For corner case: we have objects with replicas on page threshold
	// we have to sort taking status into account. Otherwise wrong
	// one(Status=moved) may get into the response
	//
	// For non-recursive mode: use lexicographical sort to keep continuation token semantics.
	// The "dirs-first" sort order breaks pagination because the token is lexicographical.
	// See related: _sortDirsFirst() in CLI
	if lsmsg.IsFlagSet(apc.LsNoRecursion) {
		cmn.SortLsoLex(objs.Entries)
	} else {
		cmn.SortLso(objs.Entries)
	}
	return objs
}

// Trim the merged page to the requested size and set the continuation token, if any.
func finLsoA(objs *cmn.LsoRes, lsmsg *apc.LsoMsg, hasMore bool) {
	maxSize := int(lsmsg.PageSize)
	// when recursion is disabled (apc.LsNoRecursion), the result _may_ include
	// duplicated names of virtual subdirectories - retain one extra to detect
	// the overflow
	if lsmsg.IsFlagSet(apc.LsNoRecursion) {
		objs.Entries = dedupLso(objs.Entries, maxSize+1)
	}
	switch l := len(objs.Entries); {
	case l > maxSize:
		// dropping the tail: truncated regardless of what the targets reported
		clear(objs.Entries[maxSize:])
		objs.Entries = objs.Entries[:maxSize]
		objs.ContinuationToken = objs.Entries[maxSize-1].Name
	case hasMore && l > 0:
		objs.ContinuationToken = objs.Entries[l-1].Name
	}
}

// - remove adjacent entries with the same Name (the input must already be sorted by Name)
// - stop after producing maxSize entries; maxSize <= 0 means unbounded
func dedupLso(entries cmn.LsoEntries, maxSize int) cmn.LsoEntries {
	var j int
	for _, en := range entries {
		if j > 0 && entries[j-1].Name == en.Name {
			continue
		}

		entries[j] = en
		j++
		if maxSize > 0 && j >= maxSize {
			break
		}
	}
	clear(entries[j:])
	return entries[:j]
}

// Merge at the speed of the "slowest" target lexicographically.
// Entries above `minToken` (see below) will be re-emitted by their owning target on the next call.
func finLsoNBI(lists []*cmn.LsoRes, lsmsg *apc.LsoMsg) *cmn.LsoRes {
	var (
		minToken string
		ncap     int
		page     = &cmn.LsoRes{UUID: lsmsg.UUID}
	)

	// 1. find min continuation token and count entries
	for _, l := range lists {
		page.Flags |= l.Flags
		n := len(l.Entries)
		ncap += n
		if l.ContinuationToken != "" {
			debug.AssertFunc(func() bool { return n == 0 || l.ContinuationToken == l.Entries[n-1].Name })
			if minToken == "" || l.ContinuationToken < minToken {
				minToken = l.ContinuationToken
			}
		}
	}
	page.ContinuationToken = minToken

	if ncap == 0 {
		return page
	}

	// 2. merge and sort
	entries := make(cmn.LsoEntries, 0, ncap)
	for _, l := range lists {
		entries = append(entries, l.Entries...)
	}
	if lsmsg.IsFlagSet(apc.LsNoRecursion) {
		cmn.SortLsoLex(entries)
		entries = dedupLso(entries, 0)
	} else {
		cmn.SortLso(entries)
	}

	// 3. truncate (> minToken)
	if minToken != "" {
		i := sort.Search(len(entries), func(i int) bool {
			return entries[i].Name > minToken
		})
		if i < len(entries) {
			clear(entries[i:])
			entries = entries[:i]
		}
	}

	page.Entries = entries
	return page
}

///////////
// lstc* - list remote (non-present, not-cached) objects and feed resulting pages to x-tco
///////////

type (
	lstca struct {
		a  map[string]*lstcx
		mu sync.Mutex
	}
	lstcx struct {
		hdr http.Header // arg
		p   *proxy
		// arg
		bckFrom *meta.Bck
		bckTo   *meta.Bck
		amsg    *apc.ActMsg // orig
		config  *cmn.Config
		smap    *smapX
		// work
		tsi     *meta.Snode
		xid     string // x-tco
		lsmsg   apc.LsoMsg
		altmsg  apc.ActMsg
		tcomsg  cmn.TCOMsg
		cnt     int
		stopped atomic.Bool
	}
)

func (a *lstca) add(c *lstcx) {
	a.mu.Lock()
	if a.a == nil {
		a.a = make(map[string]*lstcx, 4)
	}
	a.a[c.xid] = c
	a.mu.Unlock()
}

func (a *lstca) del(c *lstcx) {
	a.mu.Lock()
	delete(a.a, c.xid)
	a.mu.Unlock()
}

func (a *lstca) abort(xargs *xact.ArgsMsg) {
	switch {
	case xargs.ID != "":
		if !strings.HasPrefix(xargs.ID, xact.PrefixTcoID) {
			return
		}
		a.mu.Lock()
		if c, ok := a.a[xargs.ID]; ok {
			c.stopped.Store(true)
		}
		a.mu.Unlock()
		nlog.Infoln(xargs.ID, "aborted")
	case xargs.Kind == apc.ActCopyObjects || xargs.Kind == apc.ActETLObjects:
		var ids []string
		a.mu.Lock()
		for uuid, c := range a.a {
			c.stopped.Store(true)
			ids = append(ids, uuid)
		}
		clear(a.a)
		a.mu.Unlock()
		if len(ids) > 0 {
			nlog.Infoln(ids, "aborted")
		}
	}
}

func (c *lstcx) do() (string, error) {
	// 1. lsmsg
	c.lsmsg = apc.LsoMsg{
		UUID:     cos.GenUUID(),
		Prefix:   c.tcomsg.TCBMsg.Prefix,
		Props:    apc.GetPropsName,
		PageSize: 0, // i.e., backend.MaxPageSize()
	}
	c.lsmsg.SetFlag(apc.LsNameOnly | apc.LsNoDirs)
	if c.tcomsg.TCBMsg.NonRecurs {
		c.lsmsg.SetFlag(apc.LsNoRecursion)
	}
	c.smap = c.p.owner.smap.get()
	tsi, err := c.smap.HrwTargetTask(c.lsmsg.UUID)
	if err != nil {
		return "", err
	}
	c.tsi = tsi
	c.lsmsg.SID = tsi.ID()

	// 2. ls 1st page
	var lst *cmn.LsoRes
	lst, err = c.p.lsObjsR(c.bckFrom, &c.lsmsg, c.hdr, c.smap, tsi /*designated target*/, c.config, true /*wantOnlyRemote*/, true /*newls*/)
	if err != nil {
		return "", err
	}
	if len(lst.Entries) == 0 {
		//
		// TODO: return http.StatusNoContent to indicate exactly that (#6393)
		//
		nlog.Infoln(c.amsg.Action, c.bckFrom.Cname(""), " to ", c.bckTo.Cname("")+": lso counts zero - nothing to do")
		return c.lsmsg.UUID, nil
	}

	// 3. assign txn UUID here, and use it to communicate with x-tco directly across pages (ref050724)
	c.tcomsg.TxnUUID = cos.GenUUID()

	// 4. tcomsg
	c.tcomsg.ToBck = c.bckTo.Clone()
	lr, cnt := &c.tcomsg.ListRange, len(lst.Entries)
	lr.ObjNames = make([]string, 0, cnt)
	for _, en := range lst.Entries {
		if en.IsAnyFlagSet(apc.EntryIsDir) { // always skip virtual dirs
			continue
		}
		lr.ObjNames = append(lr.ObjNames, en.Name)
	}

	// 5. multi-obj action: transform/copy 1st page
	c.altmsg.Value = &c.tcomsg
	c.altmsg.Action = apc.ActCopyObjects
	if c.amsg.Action == apc.ActETLBck {
		c.altmsg.Action = apc.ActETLObjects
	}

	if c.xid, err = c.p.tcobjs(c.bckFrom, c.bckTo, &c.altmsg, &c.tcomsg); err != nil {
		return "", err
	}

	nlog.Infoln("'ls --all' to execute [" + c.amsg.Action + " -> " + c.altmsg.Action + "]")
	s := fmt.Sprintf("%s[%s] %s => %s", c.altmsg.Action, c.xid, c.bckFrom.String(), c.bckTo.String())

	// 6. more pages, if any
	if lst.ContinuationToken != "" {
		// Run
		nlog.Infoln("run", s, "...")
		c.lsmsg.ContinuationToken = lst.ContinuationToken
		go c.pages(s, cnt)
	} else {
		nlog.Infoln(s, "count", cnt)
	}
	return c.xid, nil
}

func (c *lstcx) pages(s string, cnt int) {
	c.cnt = cnt
	c.p.lstca.add(c)

	// pages 2, 3, ...
	var err error
	for !c.stopped.Load() && c.lsmsg.ContinuationToken != "" {
		if cnt, err = c._page(); err != nil {
			break
		}
		c.cnt += cnt
	}
	c.p.lstca.del(c)
	nlog.Infoln(s, "count", c.cnt, "stopped", c.stopped.Load(), "c-token", c.lsmsg.ContinuationToken, "err", err)
}

// next page
func (c *lstcx) _page() (int, error) {
	lst, err := c.p.lsObjsR(c.bckFrom, &c.lsmsg, c.hdr, c.smap, c.tsi, c.config, true /*wantOnlyRemote*/, false /*newls*/)
	if err != nil {
		return 0, err
	}
	c.lsmsg.ContinuationToken = lst.ContinuationToken
	if len(lst.Entries) == 0 {
		debug.Assert(lst.ContinuationToken == "")
		return 0, nil
	}

	lr := &c.tcomsg.ListRange
	clear(lr.ObjNames)
	lr.ObjNames = lr.ObjNames[:0]
	lr.ObjNames = cos.ResetSliceCap(lr.ObjNames, apc.MaxPageSizeAIS) // clip cap

	for _, en := range lst.Entries {
		if en.IsAnyFlagSet(apc.EntryIsDir) { // always skip virtual dirs
			continue
		}
		lr.ObjNames = append(lr.ObjNames, en.Name)
	}
	c.altmsg.Name = c.xid
	c.altmsg.Value = &c.tcomsg
	err = c.bcast()
	return len(lr.ObjNames), err
}

// calls t.httpxpost (TODO: slice of names is the only "delta" - optimize)
// POST /v1/xactions/{ProxyToContTCO}
// See also: xact/t2tctrl.go - another intra-cluster client

const (
	proxyToContTCO = "continueTCO" // see also: xact.T2TCtrl
)

func (c *lstcx) bcast() (err error) {
	body := cos.MustMarshal(c.altmsg)
	args := allocBcArgs()
	path := apc.URLPathXactions.Join(proxyToContTCO)
	{
		args.req = cmn.HreqArgs{Method: http.MethodPost, Path: path, Body: body}
		args.to = core.Targets
		args.timeout = cmn.Rom.MaxKeepalive()
	}
	if c.stopped.Load() {
		return
	}
	results := c.p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if err = res.err; err != nil {
			break
		}
	}
	freeBcastRes(results)
	return err
}
