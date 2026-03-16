// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
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
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
)

// one page => msgpack rsp
func (p *proxy) listObjects(w http.ResponseWriter, r *http.Request, bck *meta.Bck, amsg *apc.ActMsg, lsmsg *apc.LsoMsg) {
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

	if bck.IsHT() || lsmsg.IsFlagSet(apc.LsArchDir) {
		lsmsg.SetFlag(apc.LsCached)
	}

	// do page
	beg := mono.NanoTime()
	lst, err := p.lsPage(bck, amsg, lsmsg, r.Header, p.owner.smap.get())
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
	if strings.Contains(r.Header.Get(cos.HdrAccept), cos.ContentMsgPack) {
		ok = p.writeMsgPack(w, lst, lsotag)
	} else {
		ok = p.writeJS(w, r, lst, lsotag)
	}
	if !ok && cmn.Rom.V(4, cos.ModAIS) {
		nlog.Errorln("failed to transmit list-objects page (TCP RST?)")
	}

	// GC
	clear(lst.Entries)
	lst.Entries = lst.Entries[:0]
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

// one page; common code (native, s3 api)
func (p *proxy) lsPage(bck *meta.Bck, amsg *apc.ActMsg, lsmsg *apc.LsoMsg, hdr http.Header, smap *smapX) (*cmn.LsoRes, error) {
	var (
		nl             nl.Listener
		err            error
		tsi            *meta.Snode
		lst            *cmn.LsoRes
		newls          bool
		listRemote     bool
		wantOnlyRemote bool
	)
	if lsmsg.UUID == "" {
		lsmsg.UUID = cos.GenUUID()
		newls = true
	}
	tsi, listRemote, wantOnlyRemote, err = p._lsofc(bck, lsmsg, smap)
	if err != nil {
		return nil, err
	}
	if newls {
		if wantOnlyRemote {
			nl = xact.NewXactNL(lsmsg.UUID, apc.ActList, &smap.Smap, meta.NodeMap{tsi.ID(): tsi}, bck.Bucket())
		} else {
			// bcast
			nl = xact.NewXactNL(lsmsg.UUID, apc.ActList, &smap.Smap, nil, bck.Bucket())
		}
		// NOTE #2: TODO: currently, always primary - hrw redirect vs scenarios***
		nl.SetOwner(smap.Primary.ID())
		p.ic.registerEqual(regIC{nl: nl, smap: smap, msg: amsg})
	}

	if listRemote {
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
				s += " via " + tsi.StringEx()
			}
			nlog.Infoln(amsg.Action, "[", lsmsg.UUID, "]", bck.Cname(""), s)
		}

		config := cmn.GCO.Get()
		lst, err = p.lsObjsR(bck, lsmsg, hdr, smap, tsi, config, wantOnlyRemote)

		// TODO `status == http.StatusGone`: at this point we know that this
		// remote bucket exists and is offline. We should somehow try to list
		// cached objects. This isn't easy as we basically need to start a new
		// xaction and return a new `UUID`.
	} else {
		// A-flow
		lst, err = p.lsObjsA(bck, lsmsg, hdr)
	}

	return lst, err
}

// list-objects: flow control helper
// - decide: R-flow or A-flow
// - designate target for R-flow, etc.
func (p *proxy) _lsofc(bck *meta.Bck, lsmsg *apc.LsoMsg, smap *smapX) (_ *meta.Snode, listRemote, wantOnlyRemote bool, _ error) {
	switch {
	case lsmsg.IsFlagSet(apc.LsNBI):
		return p._lsofcNBI(bck, lsmsg)
	case !bck.IsRemote() || lsmsg.IsFlagSet(apc.LsCached):
		return p._lsofcInCluster(bck, lsmsg)
	default:
		return p._lsofcRemote(bck, lsmsg, smap)
	}
}

func (*proxy) _lsofcNBI(bck *meta.Bck, lsmsg *apc.LsoMsg) (_ *meta.Snode, listRemote, wantOnlyRemote bool, _ error) {
	if err := lsmsg.ValidateNBI(); err != nil {
		e := fmt.Errorf("%s: the request to list via native bucket inventory has invalid or unsupported flags: %v",
			bck.Cname(""), err)
		return nil, false, false, e
	}
	// NBI is always A-flow: each target enumerates its local inventory chunks,
	// proxy merges and paginates the result.
	return nil, false, false, nil
}

func (*proxy) _lsofcInCluster(bck *meta.Bck, lsmsg *apc.LsoMsg) (_ *meta.Snode, listRemote, wantOnlyRemote bool, _ error) {
	if lsmsg.IsFlagSet(apc.LsNotCached) {
		return nil, false, false, fmt.Errorf("%s is not a remote bucket - cannot list 'not cached' objects", bck.Cname(""))
	}
	return nil, false, false, nil
}

func (p *proxy) _lsofcRemote(bck *meta.Bck, lsmsg *apc.LsoMsg, smap *smapX) (_ *meta.Snode, listRemote, wantOnlyRemote bool, _ error) {
	debug.Assert(bck.IsRemote())
	debug.Assert(!lsmsg.IsFlagSet(apc.LsCached))

	// remote bucket outside cluster (not in BMD) that hasn't been added ("on the fly") by the caller
	// (lsmsg flag below)
	if bck.Props.BID == 0 {
		debug.Assert(lsmsg.IsFlagSet(apc.LsDontAddRemote))
		wantOnlyRemote = true
		if !lsmsg.WantOnlyRemoteProps() {
			err := fmt.Errorf("cannot list remote and not-in-cluster bucket %s for not-only-remote object properties: %q",
				bck.Cname(""), lsmsg.Props)
			return nil, true, true, err
		}
	} else {
		wantOnlyRemote = lsmsg.WantOnlyRemoteProps()
	}

	// check previously designated target vs Smap
	if lsmsg.SID != "" {
		return p._lsofcSID(lsmsg, smap, wantOnlyRemote)
	}

	// designate one target to carry-out backend.list-objects
	// (but note: when listing bucket inventory (`apc.HdrInventory`) target selection can change - see lsObjsR)
	tsi, err := smap.HrwTargetTask(lsmsg.UUID)
	if err == nil {
		lsmsg.SID = tsi.ID()
	}
	return tsi, true, wantOnlyRemote, err
}

func (p *proxy) _lsofcSID(lsmsg *apc.LsoMsg, smap *smapX, wantOnlyRemote bool) (_ *meta.Snode, listRemote, _ bool, _ error) {
	var (
		err error
		tsi = smap.GetTarget(lsmsg.SID)
	)
	if tsi == nil || tsi.InMaintOrDecomm() {
		err = &errNodeNotFound{p.si, smap, lsotag + " failure:", lsmsg.SID}
		nlog.Errorln(err)
		if smap.CountActiveTs() == 1 {
			// (walk an extra mile)
			orig := err
			tsi, err = smap.HrwTargetTask(lsmsg.UUID)
			if err == nil {
				nlog.Warningf("ignoring [%v] - utilizing the last (or the only) active target %s", orig, tsi)
				lsmsg.SID = tsi.ID()
			}
		}
	}
	return tsi, true, wantOnlyRemote, err
}

// A-flow:
// - bcast list-objects to all targets;
// - combine, sort and return a merged and sorted result
func (p *proxy) lsObjsA(bck *meta.Bck, lsmsg *apc.LsoMsg, hdr http.Header) (allEntries *cmn.LsoRes, err error) {
	var (
		actMsgExt *actMsgExt
		args      *bcastArgs
		results   sliceResults
		smap      = p.owner.smap.get()
	)
	if lsmsg.PageSize == 0 {
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
	lists := make([]*cmn.LsoRes, 0, len(results))
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
		if len(lst.Entries) > 0 || (lsmsg.IsFlagSet(apc.LsNBI) && lst.ContinuationToken != "") {
			lists = append(lists, lst)
		}
	}
	freeBcastRes(results)

	if lsmsg.IsFlagSet(apc.LsNBI) {
		page := finLsoNBI(lists, lsmsg)
		return page, nil
	}

	page := concatLso(lists, lsmsg)
	finLsoA(page, lsmsg)
	return page, nil
}

// R-flow:
//   - call designated target to list remote source;
//   - when wantOnlyRemote: just return the next page
//   - otherwise, use intra-cluster streams to share the latter
//     for subsequent local filtering and adding local metadata (`filterAddLmeta`)
func (p *proxy) lsObjsR(bck *meta.Bck, lsmsg *apc.LsoMsg, hdr http.Header, smap *smapX, tsi *meta.Snode, config *cmn.Config,
	wantOnlyRemote bool) (*cmn.LsoRes, error) {
	var (
		results   sliceResults
		actMsgExt = p.newAmsgActVal(apc.ActList, &lsmsg)
		args      = allocBcArgs()
		timeout   = config.Client.ListObjTimeout.D()
	)

	// Deprecated: remove by April-May 2026 (use NBI instead)
	if cos.IsParseBool(hdr.Get(apc.HdrInventory)) {
		if !bck.IsRemoteS3() {
			return nil, cmn.NewErrUnsupp("list (via bucket inventory) non-S3 bucket", bck.Cname(""))
		}
		if lsmsg.ContinuationToken == "" /*first page*/ {
			// override _lsofc selection (see above)
			var (
				err        error
				_, objName = s3.InvPrefObjname(bck.Bucket(), hdr.Get(apc.HdrInvName), hdr.Get(apc.HdrS3InvID))
			)
			tsi, err = smap.HrwName2T(bck.MakeUname(objName))
			if err != nil {
				return nil, err
			}
			lsmsg.SID = tsi.ID()

			timeout = config.Client.TimeoutLong.D()
		}
	}

	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(bck.Name),
		Header: hdr,
		Query:  bck.NewQuery(),
		Body:   cos.MustMarshal(actMsgExt),
	}
	if wantOnlyRemote {
		cargs := allocCargs()
		{
			cargs.si = tsi
			cargs.req = args.req
			cargs.timeout = timeout
			cargs.cresv = cresmGeneric[cmn.LsoRes]{}
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
	} else {
		args.timeout = timeout
		args.smap = smap
		args.cresv = cresmGeneric[cmn.LsoRes]{}
		results = p.bcastGroup(args)
	}

	freeBcArgs(args)

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

func finLsoA(objs *cmn.LsoRes, lsmsg *apc.LsoMsg) {
	maxSize := int(lsmsg.PageSize)
	// when recursion is disabled (apc.LsNoRecursion)
	// the result _may_ include duplicated names of the virtual subdirectories
	if lsmsg.IsFlagSet(apc.LsNoRecursion) {
		objs.Entries = dedupLso(objs.Entries, maxSize)
	}
	if l := len(objs.Entries); l >= maxSize {
		objs.Entries = objs.Entries[:maxSize]
		clear(objs.Entries[maxSize:])
		objs.ContinuationToken = objs.Entries[maxSize-1].Name
	}
}

func dedupLso(entries cmn.LsoEntries, maxSize int) []*cmn.LsoEnt {
	var j int
	for _, en := range entries {
		if j > 0 && entries[j-1].Name == en.Name {
			continue
		}

		entries[j] = en
		j++

		if maxSize > 0 && j == maxSize {
			break
		}
	}
	clear(entries[j:])
	return entries[:j]
}

// NBI merge: targets are resumable via seekAfter(token), inventories are HRW-disjoint.
// Each target returns up to pageSize entries > token. Proxy merge-sorts and truncates.
// Entries above the cut will be re-emitted by their owning target on the next call.
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
			debug.Assert(n == 0 || l.ContinuationToken == l.Entries[n-1].Name)
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
	cmn.SortLso(entries)

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
	lst, err = c.p.lsObjsR(c.bckFrom, &c.lsmsg, c.hdr, c.smap, tsi /*designated target*/, c.config, true)
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
	lst, err := c.p.lsObjsR(c.bckFrom, &c.lsmsg, c.hdr, c.smap, c.tsi, c.config, true)
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
func (c *lstcx) bcast() (err error) {
	body := cos.MustMarshal(c.altmsg)
	args := allocBcArgs()
	{
		args.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathXactions.S, Body: body}
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
