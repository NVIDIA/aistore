// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
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
	"github.com/NVIDIA/aistore/xact/xs"
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
	switch lsmsg.Props {
	case "":
		if lsmsg.IsFlagSet(apc.LsCached) {
			lsmsg.AddProps(apc.GetPropsDefaultAIS...)
		} else {
			lsmsg.AddProps(apc.GetPropsMinimal...)
			lsmsg.SetFlag(apc.LsNameSize)
		}
	case apc.GetPropsName:
		lsmsg.SetFlag(apc.LsNameOnly)
	case apc.GetPropsNameSize:
		lsmsg.SetFlag(apc.LsNameSize)
	}
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
	const a = "cannot perform remote versions check"
	if !bck.HasVersioningMD() {
		return errors.New(a + ": bucket " + bck.Cname("") + " does not provide (remote) versioning info")
	}
	if lsmsg.IsFlagSet(apc.LsNameOnly) || lsmsg.IsFlagSet(apc.LsNameSize) {
		return errors.New(a + ": flag 'LsVerChanged' is incompatible with 'LsNameOnly', 'LsNameSize'")
	}
	if !lsmsg.WantProp(apc.GetPropsCustom) {
		return fmt.Errorf(a+" without listing %q (object property)", apc.GetPropsCustom)
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

		// TODO: `status == http.StatusGone`: at this point we know that this
		// remote bucket exists and is offline. We should somehow try to list
		// cached objects. This isn't easy as we basically need to start a new
		// xaction and return a new `UUID`.
	} else {
		lst, err = p.lsObjsA(bck, lsmsg)
	}

	return lst, err
}

// list-objects: flow control helper
func (p *proxy) _lsofc(bck *meta.Bck, lsmsg *apc.LsoMsg, smap *smapX) (_ *meta.Snode, listRemote, wantOnlyRemote bool, _ error) {
	if !bck.IsRemote() {
		if lsmsg.IsFlagSet(apc.LsNotCached) {
			return nil, false, false, fmt.Errorf("%s is not a remote bucket - cannot list 'not cached' objects", bck.Cname(""))
		}
		return nil, false, false, nil
	}

	listRemote = !lsmsg.IsFlagSet(apc.LsCached)
	if !listRemote {
		return nil, false, false, nil
	}

	// --- list remote bucket ---

	if bck.Props.BID == 0 {
		// remote bucket outside cluster (not in BMD) that hasn't been added ("on the fly") by the caller
		// (lsmsg flag below)
		debug.Assert(bck.IsRemote())
		debug.Assert(lsmsg.IsFlagSet(apc.LsDontAddRemote))
		wantOnlyRemote = true
		if !lsmsg.WantOnlyRemoteProps() {
			err := fmt.Errorf("cannot list remote and not-in-cluster bucket %s for not-only-remote object properties: %q",
				bck.Cname(""), lsmsg.Props)
			return nil, true, true, err
		}
	} else {
		// default
		wantOnlyRemote = lsmsg.WantOnlyRemoteProps()
	}

	// check previously designated target vs Smap
	if lsmsg.SID != "" {
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

	// designate one target to carry-out backend.list-objects
	// (but note: when listing bucket inventory (`apc.HdrInventory`) target selection can change - see lsObjsR)
	tsi, err := smap.HrwTargetTask(lsmsg.UUID)
	if err == nil {
		lsmsg.SID = tsi.ID()
	}
	return tsi, true, wantOnlyRemote, err
}

// lsObjsA reads object list from all targets, combines, sorts and returns
// the final list. Excess of object entries from each target is remembered in the
// buffer (see: `queryBuffers`) so we won't request the same objects again.
func (p *proxy) lsObjsA(bck *meta.Bck, lsmsg *apc.LsoMsg) (allEntries *cmn.LsoRes, err error) {
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
		if len(lst.Entries) > 0 {
			lists = append(lists, lst)
		}
	}
	freeBcastRes(results)

	page := concatLso(lists, lsmsg)
	finLsoA(page, lsmsg)
	return page, nil
}

func (p *proxy) lsObjsR(bck *meta.Bck, lsmsg *apc.LsoMsg, hdr http.Header, smap *smapX, tsi *meta.Snode, config *cmn.Config,
	wantOnlyRemote bool) (*cmn.LsoRes, error) {
	var (
		results   sliceResults
		actMsgExt = p.newAmsgActVal(apc.ActList, &lsmsg)
		args      = allocBcArgs()
		timeout   = config.Client.ListObjTimeout.D()
	)
	if cos.IsParseBool(hdr.Get(apc.HdrInventory)) {
		// TODO: extend to other Clouds or, more precisely, other list-objects supporting backends
		if !bck.IsRemoteS3() {
			return nil, cmn.NewErrUnsupp("list (via bucket inventory) non-S3 bucket", bck.Cname(""))
		}
		if lsmsg.ContinuationToken == "" /*first page*/ {
			// override _lsofc selection (see above)
			var (
				err        error
				_, objName = s3.InvPrefObjname(bck.Bucket(), hdr.Get(apc.HdrInvName), hdr.Get(apc.HdrInvID))
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
		objs.Entries = dedupLso(objs.Entries, maxSize, false /*no-dirs*/)
	}
	if len(objs.Entries) >= maxSize {
		objs.Entries = objs.Entries[:maxSize]
		clear(objs.Entries[maxSize:])
		objs.ContinuationToken = objs.Entries[len(objs.Entries)-1].Name
	}
}

func dedupLso(entries cmn.LsoEntries, maxSize int, noDirs bool) []*cmn.LsoEnt {
	var j int
	for _, en := range entries {
		if j > 0 && entries[j-1].Name == en.Name {
			continue
		}

		// expecting backends for filter out accordingly
		debug.Assert(!noDirs || !en.IsAnyFlagSet(apc.EntryIsDir))

		entries[j] = en
		j++

		if maxSize > 0 && j == maxSize {
			break
		}
	}
	clear(entries[j:])
	return entries[:j]
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
		if !strings.HasPrefix(xargs.ID, xs.PrefixTcoID) {
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

	if c.xid, err = c.p.tcobjs(c.bckFrom, c.bckTo, c.config, &c.altmsg, &c.tcomsg); err != nil {
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
