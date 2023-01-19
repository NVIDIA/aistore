// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	notif "github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
)

// context structure to gather all (or most) of the relevant state in one place
// (compare with txnServerCtx)
type txnClientCtx struct {
	p        *proxy
	smap     *smapX
	msg      *aisMsg
	uuid     string
	path     string
	req      cmn.HreqArgs
	selected cluster.Nodes
	timeout  struct {
		netw time.Duration
		host time.Duration
	}
}

// TODO: IC(c.uuid) vs _committed_ xactID (currently asserted)
// TODO: cleanup upon failures

//////////////////
// txnClientCtx //
//////////////////

func (c *txnClientCtx) begin(what fmt.Stringer) (err error) {
	results := c.bcast(apc.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err != nil {
			err = c.bcastAbort(what, res.toErr())
			break
		}
	}
	freeBcastRes(results)
	return
}

// returns xaction UUID or empty
// NOTE: global ID can be further reinforced by _not_ ignoring empty returns, i.e.,
// requiring that each responding target returns a valid ID and all IDs are equal.
// Likely, an additional condition that a caller must be able to ask for.
func (c *txnClientCtx) commit(what fmt.Stringer, timeout time.Duration) (xactID string, err error) {
	globalID := true // cluster-wide xaction ID
	results := c.bcast(apc.ActCommit, timeout)
	for _, res := range results {
		if res.err != nil {
			err = res.toErr()
			glog.Errorf("Failed to commit %q %s: %v %s", c.msg.Action, what, err, c.msg)
			break
		}
		if globalID {
			if xactID == "" {
				xactID = res.header.Get(apc.HdrXactionID)
			} else if xactID != res.header.Get(apc.HdrXactionID) {
				xactID, globalID = "", false
			}
		}
	}
	freeBcastRes(results)
	return
}

func (c *txnClientCtx) cmtTout(waitmsync bool) time.Duration {
	if waitmsync {
		return c.timeout.host + c.timeout.netw
	}
	return c.timeout.netw
}

func (c *txnClientCtx) bcast(phase string, timeout time.Duration) (results sliceResults) {
	c.req.Path = cos.JoinWords(c.path, phase)
	if phase != apc.ActAbort {
		now := time.Now()
		c.req.Query.Set(apc.QparamUnixTime, cos.UnixNano2S(now.UnixNano()))
	}

	args := allocBcArgs()
	defer freeBcArgs(args)

	args.req = c.req
	args.smap = c.smap
	args.timeout = timeout
	args.to = cluster.Targets // the (0) default
	if args.selected = c.selected; args.selected == nil {
		results = c.p.bcastGroup(args)
	} else {
		args.network = cmn.NetIntraControl
		results = c.p.bcastSelected(args) // e.g. usage: promote => specific target
	}
	return
}

func (c *txnClientCtx) bcastAbort(what fmt.Stringer, err error) error {
	glog.Errorf("Abort %q %s: %v %s", c.msg.Action, what, err, c.msg)
	results := c.bcast(apc.ActAbort, 0)
	freeBcastRes(results)
	return err
}

///////////////////////////////////////////////////////////////////////////////////////////
// cp transactions (the proxy part)
//
// A typical control-plane transaction will execute, with minor variations, the same
// 6 (plus/minus) steps as shown below:
// - notice a certain symmetry between the client and the server sides whetreby
//   the control flow looks as follows:
//   	txnClientCtx =>
//   		(POST to /v1/txn) =>
//   			switch msg.Action =>
//   				txnServerCtx =>
//   					concrete transaction, etc.
///////////////////////////////////////////////////////////////////////////////////////////

// create-bucket: { check non-existence -- begin -- create locally -- metasync -- commit }
func (p *proxy) createBucket(msg *apc.ActMsg, bck *cluster.Bck, remoteHdr http.Header) error {
	var (
		bprops  *cmn.BucketProps
		nlp     = bck.GetNameLockPair()
		bmd     = p.owner.bmd.get()
		backend = bck.Backend()
	)
	if bck.Props != nil {
		bprops = bck.Props
	}

	// validate & assign bprops
	switch {
	case remoteHdr != nil: // remote exists
		remoteProps := defaultBckProps(bckPropsArgs{bck: bck, hdr: remoteHdr})
		if bprops == nil {
			bprops = remoteProps
		} else {
			// backend versioning always takes precedence
			bprops.Versioning.Enabled = remoteProps.Versioning.Enabled
		}
		if bck.IsRemoteAIS() {
			// remais alias => uuid
			bck.Ns.UUID = remoteHdr.Get(apc.HdrRemAisUUID)
			debug.Assert(cos.IsValidUUID(bck.Ns.UUID))
		}
	case backend != nil: // remote backend exists
		if bprops == nil {
			bprops = defaultBckProps(bckPropsArgs{bck: bck})
		}
		cloudProps, present := bmd.Get(backend)
		debug.Assert(present)
		bprops.Versioning.Enabled = cloudProps.Versioning.Enabled // always takes precedence
	case bck.IsRemote(): // can't create cloud buckets (NIE/NSY)
		if bck.IsCloud() {
			return cmn.NewErrNotImpl("create", bck.Provider+"(cloud) bucket")
		}
		if bck.IsHTTP() {
			return cmn.NewErrNotImpl("create", "bucket for HTTP provider")
		}
		// can do remote ais though
		if !bck.IsRemoteAIS() {
			return cmn.NewErrUnsupp("create", bck.Provider+":// bucket")
		}
	}

	if bprops == nil { // inherit (all) cluster defaults
		bprops = defaultBckProps(bckPropsArgs{bck: bck})
	}

	// 1. try add
	nlp.Lock()
	defer nlp.Unlock()
	if _, present := bmd.Get(bck); present {
		return cmn.NewErrBckAlreadyExists(bck.Bucket())
	}

	// 2. begin
	var (
		waitmsync = true // commit blocks behind metasync
		c         = p.prepTxnClient(msg, bck, waitmsync)
	)
	if err := c.begin(bck); err != nil {
		return err
	}

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:      bmodCreate,
		final:    p.bmodSync,
		wait:     waitmsync,
		msg:      &c.msg.ActMsg,
		txnID:    c.uuid,
		bcks:     []*cluster.Bck{bck},
		setProps: bprops,
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		return c.bcastAbort(bck, err)
	}

	// 4. commit
	_, err := c.commit(bck, c.cmtTout(waitmsync))
	if err != nil {
		p.undoCreateBucket(msg, bck)
	}
	return err
}

func bmodCreate(ctx *bmdModifier, clone *bucketMD) (err error) {
	bck := ctx.bcks[0]
	added := clone.add(bck, ctx.setProps)
	if !added {
		err = cmn.NewErrBckAlreadyExists(bck.Bucket())
	}
	return
}

func bmodRm(ctx *bmdModifier, clone *bucketMD) error {
	bck := ctx.bcks[0]
	if _, present := clone.Get(bck); !present {
		return cmn.NewErrBckNotFound(bck.Bucket())
	}
	deleted := clone.del(bck)
	cos.Assert(deleted)
	return nil
}

// make-n-copies: { confirm existence -- begin -- update locally -- metasync -- commit }
func (p *proxy) makeNCopies(msg *apc.ActMsg, bck *cluster.Bck) (xactID string, err error) {
	copies, err := _parseNCopies(msg.Value)
	if err != nil {
		return
	}

	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		err = cmn.NewErrBckNotFound(bck.Bucket())
		return
	}

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(msg, bck, waitmsync)
	)
	if err = c.begin(bck); err != nil {
		return
	}

	// 3. update BMD locally & metasync updated BMD
	mirrorEnabled := copies > 1
	updateProps := &cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{
			Enabled: &mirrorEnabled,
			Copies:  &copies,
		},
	}
	ctx := &bmdModifier{
		pre:           bmodMirror,
		final:         p.bmodSync,
		wait:          waitmsync,
		msg:           &c.msg.ActMsg,
		txnID:         c.uuid,
		propsToUpdate: updateProps,
		bcks:          []*cluster.Bck{bck},
	}
	bmd, err = p.owner.bmd.modify(ctx)
	if err != nil {
		err = c.bcastAbort(bck, err)
		return
	}
	c.msg.BMDVersion = bmd.version()

	// 4. IC
	nl := xact.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bck.Bucket())
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 5. commit
	xactID, err = c.commit(bck, c.cmtTout(waitmsync))
	debug.Assertf(xactID == "" || xactID == c.uuid, "committed %q vs generated %q", xactID, c.uuid)
	if err != nil {
		p.undoUpdateCopies(msg, bck, ctx.revertProps)
	}
	return
}

func bmodMirror(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
	)
	cos.Assert(present)
	nprops := bprops.Clone()
	nprops.Apply(ctx.propsToUpdate)
	ctx.revertProps = &cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{
			Copies:  &bprops.Mirror.Copies,
			Enabled: &bprops.Mirror.Enabled,
		},
	}
	clone.set(bck, nprops)
	return nil
}

// set-bucket-props: { confirm existence -- begin -- apply props -- metasync -- commit }
func (p *proxy) setBucketProps(msg *apc.ActMsg, bck *cluster.Bck, nprops *cmn.BucketProps) (string /*xactID*/, error) {
	// 1. confirm existence
	bprops, present := p.owner.bmd.get().Get(bck)
	if !present {
		return "", cmn.NewErrBckNotFound(bck.Bucket())
	}
	bck.Props = bprops

	// 2. begin
	switch msg.Action {
	case apc.ActSetBprops:
		// do nothing here (caller's responsible for validation)
	case apc.ActResetBprops:
		bargs := bckPropsArgs{bck: bck}
		if bck.IsRemote() {
			if backend := bck.Backend(); backend != nil {
				err := fmt.Errorf("%q has backend %q (hint: detach prior to resetting the props)",
					bck, backend)
				return "", err
			}
			remoteBckProps, _, err := p.headRemoteBck(bck.Bucket(), nil)
			if err != nil {
				return "", err
			}
			bargs.hdr = remoteBckProps
		}
		nprops = defaultBckProps(bargs)
	default:
		return "", fmt.Errorf(fmtErrInvaldAction, msg.Action, []string{apc.ActSetBprops, apc.ActResetBprops})
	}
	// msg{propsToUpdate} => nmsg{nprops} and prep context(nmsg)
	nmsg := *msg
	nmsg.Value = nprops
	var (
		waitmsync = true
		c         = p.prepTxnClient(&nmsg, bck, waitmsync)
	)
	if err := c.begin(bck); err != nil {
		return "", err
	}

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:      p.bmodSetProps,
		final:    p.bmodSync,
		wait:     waitmsync,
		msg:      msg,
		txnID:    c.uuid,
		setProps: nprops,
		bcks:     []*cluster.Bck{bck},
	}
	bmd, err := p.owner.bmd.modify(ctx)
	if err != nil {
		debug.AssertNoErr(err)
		err = c.bcastAbort(bck, err)
		return "", err
	}
	c.msg.BMDVersion = bmd.version()

	// 4. if remirror|re-EC|TBD-storage-svc
	// NOTE: setting up IC listening prior to committing (and confirming xactID) here and elsewhere
	if ctx.needReMirror || ctx.needReEC {
		action := apc.ActMakeNCopies
		if ctx.needReEC {
			action = apc.ActECEncode
		}
		nl := xact.NewXactNL(c.uuid, action, &c.smap.Smap, nil, bck.Bucket())
		nl.SetOwner(equalIC)
		p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})
	}

	// 5. commit
	return c.commit(bck, c.cmtTout(waitmsync))
}

// compare w/ bmodUpdateProps
func (p *proxy) bmodSetProps(ctx *bmdModifier, clone *bucketMD) (err error) {
	var (
		targetCnt       int
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck)
	)
	debug.Assert(present)
	if ctx.msg.Action == apc.ActSetBprops {
		bck.Props = bprops
	}
	ctx.needReMirror = _reMirror(bprops, ctx.setProps)
	targetCnt, ctx.needReEC = _reEC(bprops, ctx.setProps, bck, p.owner.smap.get())
	debug.Assert(!ctx.needReEC || ctx.setProps.Validate(targetCnt) == nil)
	clone.set(bck, ctx.setProps)
	return nil
}

// rename-bucket: { confirm existence -- begin -- RebID -- metasync -- commit -- wait for rebalance and unlock }
func (p *proxy) renameBucket(bckFrom, bckTo *cluster.Bck, msg *apc.ActMsg) (xactID string, err error) {
	if err = p.canRunRebalance(); err != nil {
		err = cmn.NewErrFailedTo(p, "rename", bckFrom, err)
		return
	}
	// 1. confirm existence & non-existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		err = cmn.NewErrBckNotFound(bckFrom.Bucket())
		return
	}
	if _, present := bmd.Get(bckTo); present {
		err = cmn.NewErrBckAlreadyExists(bckTo.Bucket())
		return
	}

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(msg, bckFrom, waitmsync)
	)
	_ = bckTo.AddUnameToQuery(c.req.Query, apc.QparamBckTo)
	if err = c.begin(bckFrom); err != nil {
		return
	}

	// 3. update BMD locally & metasync updated BMD
	bmdCtx := &bmdModifier{
		pre:          bmodMv,
		final:        p.bmodSync,
		msg:          msg,
		txnID:        c.uuid,
		bcks:         []*cluster.Bck{bckFrom, bckTo},
		wait:         waitmsync,
		singleTarget: c.smap.CountActiveTs() == 1,
	}

	bmd, err = p.owner.bmd.modify(bmdCtx)
	if err != nil {
		debug.AssertNoErr(err)
		err = c.bcastAbort(bckFrom, err)
		return
	}
	c.msg.BMDVersion = bmd.version()

	ctx := &rmdModifier{
		pre: func(_ *rmdModifier, clone *rebMD) {
			clone.inc()
			clone.Resilver = cos.GenUUID()
		},
	}
	rmd, err := p.owner.rmd.modify(ctx)
	if err != nil {
		glog.Error(err)
		debug.AssertNoErr(err)
	}
	c.msg.RMDVersion = rmd.version()

	// 4. IC
	nl := xact.NewXactNL(c.uuid, c.msg.Action, &c.smap.Smap, nil, bckFrom.Bucket(), bckTo.Bucket())
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{smap: c.smap, nl: nl, query: c.req.Query})

	// 5. commit
	c.req.Body = cos.MustMarshal(c.msg)
	xactID, err = c.commit(bckFrom, c.cmtTout(waitmsync))
	debug.Assertf(xactID == "" || xactID == c.uuid, "committed %q vs generated %q", xactID, c.uuid)
	if err != nil {
		glog.Errorf("%s: failed to commit %q, err: %v", p, msg.Action, err)
		return
	}

	// 6. start rebalance and resilver
	wg := p.metasyncer.sync(revsPair{rmd, c.msg})

	// Register rebalance `nl`
	nl = xact.NewXactNL(xact.RebID2S(rmd.Version), apc.ActRebalance, &c.smap.Smap, nil)
	nl.SetOwner(equalIC)
	err = p.notifs.add(nl)
	debug.AssertNoErr(err)

	// Register resilver `nl`
	nl = xact.NewXactNL(rmd.Resilver, apc.ActResilver, &c.smap.Smap, nil)
	nl.SetOwner(equalIC)
	_ = p.notifs.add(nl)

	wg.Wait()
	return
}

func bmodMv(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bckFrom, bckTo  = ctx.bcks[0], ctx.bcks[1]
		bprops, present = clone.Get(bckFrom)
	)
	debug.Assert(present)
	bckFrom.Props = bprops.Clone()
	bckTo.Props = bprops.Clone()
	added := clone.add(bckTo, bckTo.Props)
	cos.Assert(added)
	bckFrom.Props.Renamed = apc.ActMoveBck // NOTE: state until `BMDVersionFixup` by renaming xaction
	clone.set(bckFrom, bckFrom.Props)
	return nil
}

// transform (or simply copy) bucket to another bucket
// { confirm existence -- begin -- conditional metasync -- start waiting for operation done -- commit }
func (p *proxy) tcb(bckFrom, bckTo *cluster.Bck, msg *apc.ActMsg, dryRun bool) (xactID string, err error) {
	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, existsFrom := bmd.Get(bckFrom); !existsFrom {
		err = cmn.NewErrBckNotFound(bckFrom.Bucket())
		return
	}
	_, existsTo := bmd.Get(bckTo)
	debug.Assert(existsTo || bckTo.IsAIS())

	// 2. begin
	var (
		waitmsync = !dryRun
		c         = p.prepTxnClient(msg, bckFrom, waitmsync)
	)
	_ = bckTo.AddUnameToQuery(c.req.Query, apc.QparamBckTo)
	if err = c.begin(bckFrom); err != nil {
		return
	}

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:   bmodTCB,
		final: p.bmodSync,
		msg:   msg,
		txnID: c.uuid,
		bcks:  []*cluster.Bck{bckFrom, bckTo},
		wait:  waitmsync,
	}
	if !dryRun {
		bmd, err = p.owner.bmd.modify(ctx)
		if err != nil {
			debug.AssertNoErr(err)
			err = c.bcastAbort(bckFrom, err)
			return
		}
		c.msg.BMDVersion = bmd.version()
		if !ctx.terminate {
			debug.Assert(!existsTo)
			c.req.Query.Set(apc.QparamWaitMetasync, "true")
		}
	}

	// 4. IC
	nl := xact.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bckFrom.Bucket(), bckTo.Bucket())
	nl.SetOwner(equalIC)
	// cleanup upon failure via notification listener callback
	// (note synchronous cleanup below)
	nl.F = func(nl notif.NotifListener) {
		if errNl := nl.Err(); errNl != nil {
			if !ctx.terminate { // undo bmd.modify() - see above
				glog.Error(errNl)
				_ = p.destroyBucket(&apc.ActMsg{Action: apc.ActDestroyBck}, bckTo)
			}
		}
	}
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 5. commit
	xactID, err = c.commit(bckFrom, c.cmtTout(waitmsync))
	debug.Assertf(xactID == "" || xactID == c.uuid, "committed %q vs generated %q", xactID, c.uuid)
	if err != nil {
		// cleanup
		_ = p.destroyBucket(&apc.ActMsg{Action: apc.ActDestroyBck}, bckTo)
	}
	return
}

// transform or copy a list or a range of objects
func (p *proxy) tcobjs(bckFrom, bckTo *cluster.Bck, msg *apc.ActMsg) (xactID string, err error) {
	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		err = cmn.NewErrBckNotFound(bckFrom.Bucket())
		return
	}
	// 2. begin
	var (
		waitmsync = false
		c         = p.prepTxnClient(msg, bckFrom, waitmsync)
	)
	_ = bckTo.AddUnameToQuery(c.req.Query, apc.QparamBckTo)
	if err = c.begin(bckFrom); err != nil {
		return
	}

	// 3. commit
	xactID, err = c.commit(bckFrom, c.cmtTout(waitmsync))
	if xactID != "" {
		// happens to grab cluster-wide ID
		glog.Infof("%s: x-%s[%s]", p, msg.Action, xactID)
	}
	return
}

func bmodTCB(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bckFrom, bckTo  = ctx.bcks[0], ctx.bcks[1]
		bprops, present = clone.Get(bckFrom) // TODO: Bucket could be removed during begin.
	)
	cos.Assert(present)

	// Skip destination bucket creation if it's dry run or it's already present.
	if _, present = clone.Get(bckTo); present {
		ctx.terminate = true
		return nil
	}

	debug.Assert(bckTo.IsAIS())
	bckFrom.Props = bprops.Clone()
	// replicate bucket props - but only if the source is ais as well
	if bckFrom.IsAIS() || bckFrom.IsRemoteAIS() {
		bckTo.Props = bprops.Clone()
	} else {
		bckTo.Props = defaultBckProps(bckPropsArgs{bck: bckTo})
	}
	added := clone.add(bckTo, bckTo.Props)
	cos.Assert(added)
	return nil
}

func parseECConf(value any) (*cmn.ECConfToUpdate, error) {
	switch v := value.(type) {
	case string:
		conf := &cmn.ECConfToUpdate{}
		err := jsoniter.Unmarshal([]byte(v), conf)
		return conf, err
	case []byte:
		conf := &cmn.ECConfToUpdate{}
		err := jsoniter.Unmarshal(v, conf)
		return conf, err
	default:
		return nil, errors.New("invalid request")
	}
}

// ec-encode: { confirm existence -- begin -- update locally -- metasync -- commit }
func (p *proxy) ecEncode(bck *cluster.Bck, msg *apc.ActMsg) (xactID string, err error) {
	nlp := bck.GetNameLockPair()
	ecConf, err := parseECConf(msg.Value)
	if err != nil {
		return
	}
	if ecConf.DataSlices == nil || *ecConf.DataSlices < 1 ||
		ecConf.ParitySlices == nil || *ecConf.ParitySlices < 1 {
		err = errors.New("invalid number of slices")
		return
	}
	if !nlp.TryLock(cmn.Timeout.CplaneOperation() / 2) {
		err = cmn.NewErrBckIsBusy(bck.Bucket())
		return
	}
	defer nlp.Unlock()

	// 1. confirm existence
	props, present := p.owner.bmd.get().Get(bck)
	if !present {
		err = cmn.NewErrBckNotFound(bck.Bucket())
		return
	}
	if props.EC.Enabled {
		// Changing data or parity slice count on the fly is unsupported yet
		err = fmt.Errorf("%s: EC is already enabled for bucket %s", p, bck)
		return
	}

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(msg, bck, waitmsync)
	)
	if err = c.begin(bck); err != nil {
		return
	}

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:           bmodUpdateProps,
		final:         p.bmodSync,
		bcks:          []*cluster.Bck{bck},
		wait:          waitmsync,
		msg:           &c.msg.ActMsg,
		txnID:         c.uuid,
		propsToUpdate: &cmn.BucketPropsToUpdate{EC: ecConf},
	}
	bmd, err := p.owner.bmd.modify(ctx)
	if err != nil {
		debug.AssertNoErr(err)
		err = c.bcastAbort(bck, err)
		return
	}
	c.msg.BMDVersion = bmd.version()

	// 5. IC
	nl := xact.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bck.Bucket())
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	xactID, err = c.commit(bck, c.cmtTout(waitmsync))
	debug.Assertf(xactID == "" || xactID == c.uuid, "committed %q vs generated %q", xactID, c.uuid)
	return
}

// compare w/ bmodSetProps
func bmodUpdateProps(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
	)
	if !present {
		ctx.terminate = true
		return nil
	}
	nprops := bprops.Clone()
	nprops.Apply(ctx.propsToUpdate)
	clone.set(bck, nprops)
	return nil
}

func (p *proxy) createArchMultiObj(bckFrom, bckTo *cluster.Bck, msg *apc.ActMsg) (xactID string, err error) {
	// begin
	c := p.prepTxnClient(msg, bckFrom, false /*waitmsync*/)
	_ = bckTo.AddUnameToQuery(c.req.Query, apc.QparamBckTo)
	if err = c.begin(bckFrom); err != nil {
		return
	}
	// commit
	xactID, err = c.commit(bckFrom, c.cmtTout(false /*waitmsync*/))
	if xactID != "" {
		// happens to grab cluster-wide ID
		glog.Infof("%s: x-%s[%s]", p, msg.Action, xactID)
	}
	return
}

// maintenance: { begin -- enable GFN -- commit -- start rebalance }
func (p *proxy) startMaintenance(si *cluster.Snode, msg *apc.ActMsg, opts *apc.ActValRmNode) (rebID string, err error) {
	var (
		waitmsync  = false
		c          = p.prepTxnClient(msg, nil, waitmsync)
		rebEnabled = cmn.GCO.Get().Rebalance.Enabled
	)
	if si.IsTarget() && !opts.SkipRebalance && rebEnabled {
		if err = p.canRunRebalance(); err != nil {
			return
		}
	}
	// 1. begin
	if err = c.begin(si); err != nil {
		return
	}

	// 2. Put node under maintenance
	if err = p.markMaintenance(msg, si); err != nil {
		c.bcastAbort(si, err)
		return
	}

	// 3. Commit
	// NOTE: Call only the target that's being decommissioned (commit is a no-op for the rest)
	if msg.Action == apc.ActDecommissionNode || msg.Action == apc.ActShutdownNode {
		c.req.Path = cos.JoinWords(c.path, apc.ActCommit)
		cargs := allocCargs()
		{
			cargs.si = si
			cargs.req = c.req
			cargs.timeout = c.cmtTout(waitmsync)
		}
		res := p.call(cargs)
		err = res.toErr()
		freeCargs(cargs)
		freeCR(res)
		if err != nil {
			glog.Error(err)
			return
		}
	}

	// 4. Start rebalance
	if !opts.SkipRebalance && rebEnabled {
		return p.rebalanceAndRmSelf(msg, si)
	} else if msg.Action == apc.ActDecommissionNode {
		_, err = p.callRmSelf(msg, si, true /*skipReb*/)
	}
	return
}

// Put node under maintenance
func (p *proxy) markMaintenance(msg *apc.ActMsg, si *cluster.Snode) error {
	var flags cos.BitFlags
	switch msg.Action {
	case apc.ActDecommissionNode:
		flags = cluster.NodeFlagDecomm
	case apc.ActStartMaintenance, apc.ActShutdownNode:
		flags = cluster.NodeFlagMaint
	default:
		err := fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{apc.ActDecommissionNode, apc.ActStartMaintenance, apc.ActShutdownNode})
		debug.AssertNoErr(err)
		return err
	}
	ctx := &smapModifier{
		pre:   p._markMaint,
		final: p._syncFinal,
		sid:   si.ID(),
		flags: flags,
		msg:   msg,
	}
	return p.owner.smap.modify(ctx)
}

func (p *proxy) _markMaint(ctx *smapModifier, clone *smapX) error {
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot put %s in maintenance", ctx.sid))
	}
	clone.setNodeFlags(ctx.sid, ctx.flags)
	clone.staffIC()
	return nil
}

// destroy bucket: { begin -- commit }
func (p *proxy) destroyBucket(msg *apc.ActMsg, bck *cluster.Bck) error {
	nlp := bck.GetNameLockPair()
	nlp.Lock()
	defer nlp.Unlock()

	actMsg := &apc.ActMsg{}
	*actMsg = *msg

	// 1. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(actMsg, bck, waitmsync)
		config    = cmn.GCO.Get()
	)
	// NOTE: testing only: to avoid premature aborts when loopback devices get 100% utilized
	//       (under heavy writing)
	if config.TestingEnv() {
		c.timeout.netw = config.Timeout.MaxHostBusy.D() + config.Timeout.MaxHostBusy.D()/2
		c.timeout.host = c.timeout.netw
	}
	if err := c.begin(bck); err != nil {
		return err
	}

	// 2. Distribute new BMD
	ctx := &bmdModifier{
		pre:   bmodRm,
		final: p.bmodSync,
		msg:   msg,
		txnID: c.uuid,
		wait:  waitmsync,
		bcks:  []*cluster.Bck{bck},
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		return c.bcastAbort(bck, err)
	}

	// 3. Commit
	_, err := c.commit(bck, c.cmtTout(waitmsync))
	return err
}

// erase bucket data from all targets (keep metadata)
func (p *proxy) destroyBucketData(msg *apc.ActMsg, bck *cluster.Bck) error {
	query := bck.AddToQuery(url.Values{apc.QparamKeepRemote: []string{"true"}})
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodDelete,
		Path:   apc.URLPathBuckets.Join(bck.Name),
		Body:   cos.MustMarshal(msg),
		Query:  query,
	}
	args.to = cluster.Targets
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			return res.err
		}
	}
	freeBcastRes(results)
	return nil
}

// promote synchronously if the number of files (to promote) is less or equal
const promoteNumSync = 16

func (p *proxy) promote(bck *cluster.Bck, msg *apc.ActMsg, tsi *cluster.Snode) (xactID string, err error) {
	var (
		totalN           int64
		waitmsync        bool
		allAgree, noXact bool
		singleT          bool
	)
	c := p.prepTxnClient(msg, bck, waitmsync)
	if c.smap.CountActiveTs() == 1 {
		singleT = true
	} else if tsi != nil {
		c.selected = []*cluster.Snode{tsi}
		singleT = true
	}

	// begin
	if totalN, allAgree, err = prmBegin(c, bck, singleT); err != nil {
		return
	}

	// feat
	if allAgree {
		// confirm file share when, and only if, all targets see identical content
		// (so that they go ahead and partition the work accordingly)
		c.req.Query.Set(apc.QparamConfirmFshare, "true")
	} else if totalN <= promoteNumSync {
		// targets to operate autonomously and synchronously
		c.req.Query.Set(apc.QparamActNoXact, "true")
		noXact = true
	}

	// IC
	if !noXact {
		nl := xact.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bck.Bucket())
		nl.SetOwner(equalIC)
		p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})
	}

	// commit
	xactID, err = c.commit(bck, c.cmtTout(waitmsync))
	debug.Assertf(noXact || xactID == c.uuid, "noXact=%t, committed %q vs generated %q", noXact, xactID, c.uuid)
	return
}

// begin phase customized to (specifically) detect file share
func prmBegin(c *txnClientCtx, bck *cluster.Bck, singleT bool) (num int64, allAgree bool, err error) {
	var cksumVal, totalN string
	allAgree = !singleT

	results := c.bcast(apc.ActBegin, c.timeout.netw)
	for i, res := range results {
		if res.err != nil {
			err = c.bcastAbort(bck, res.toErr())
			break
		}
		if singleT {
			totalN = res.header.Get(apc.HdrPromoteNamesNum)
			debug.Assert(len(results) == 1)
			break
		}

		// all agree?
		if i == 0 {
			cksumVal = res.header.Get(apc.HdrPromoteNamesHash)
			totalN = res.header.Get(apc.HdrPromoteNamesNum)
		} else if val := res.header.Get(apc.HdrPromoteNamesHash); val == "" || val != cksumVal {
			allAgree = false
		} else if allAgree {
			debug.Assert(totalN == res.header.Get(apc.HdrPromoteNamesNum))
		}
	}
	if err == nil {
		num, err = strconv.ParseInt(totalN, 10, 64)
	}
	freeBcastRes(results)
	return
}

//
// misc helpers and utilities
///

func (p *proxy) prepTxnClient(msg *apc.ActMsg, bck *cluster.Bck, waitmsync bool) *txnClientCtx {
	c := &txnClientCtx{p: p, uuid: cos.GenUUID(), smap: p.owner.smap.get()}
	c.msg = p.newAmsg(msg, nil, c.uuid)
	body := cos.MustMarshal(c.msg)

	query := make(url.Values, 2)
	if bck == nil {
		c.path = apc.URLPathTxn.S
	} else {
		c.path = apc.URLPathTxn.Join(bck.Name)
		query = bck.AddToQuery(query)
	}
	config := cmn.GCO.Get()
	c.timeout.netw = 2 * config.Timeout.MaxKeepalive.D()
	c.timeout.host = config.Timeout.MaxHostBusy.D()
	if !waitmsync { // when commit does not block behind metasync
		query.Set(apc.QparamNetwTimeout, cos.UnixNano2S(int64(c.timeout.netw)))
	}
	query.Set(apc.QparamHostTimeout, cos.UnixNano2S(int64(c.timeout.host)))

	c.req = cmn.HreqArgs{Method: http.MethodPost, Query: query, Body: body}
	return c
}

// rollback create-bucket
func (p *proxy) undoCreateBucket(msg *apc.ActMsg, bck *cluster.Bck) {
	ctx := &bmdModifier{
		pre:   bmodRm,
		final: p.bmodSync,
		msg:   msg,
		bcks:  []*cluster.Bck{bck},
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		debug.AssertNoErr(err)
	}
}

// rollback make-n-copies
func (p *proxy) undoUpdateCopies(msg *apc.ActMsg, bck *cluster.Bck, propsToUpdate *cmn.BucketPropsToUpdate) {
	ctx := &bmdModifier{
		pre:           bmodUpdateProps,
		final:         p.bmodSync,
		msg:           msg,
		propsToUpdate: propsToUpdate,
		bcks:          []*cluster.Bck{bck},
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		debug.AssertNoErr(err)
	}
}

// Make and validate new bucket props.
func (p *proxy) makeNewBckProps(bck *cluster.Bck, propsToUpdate *cmn.BucketPropsToUpdate,
	creating ...bool) (nprops *cmn.BucketProps, err error) {
	var (
		cfg    = cmn.GCO.Get()
		bprops = bck.Props
	)
	nprops = bprops.Clone()
	nprops.Apply(propsToUpdate)
	if bck.IsCloud() {
		bv, nv := bck.VersionConf().Enabled, nprops.Versioning.Enabled
		if bv != nv {
			// NOTE: bprops.Versioning.Enabled must be previously set via httpbckhead
			err = fmt.Errorf("%s: cannot modify existing Cloud bucket versioning (%s, %s)",
				p.si, bck, _versioning(bv))
			return
		}
	} else if bck.IsHDFS() {
		nprops.Versioning.Enabled = false
		// TODO: Check if the `RefDirectory` does not overlap with other buckets.
	}
	if bprops.EC.Enabled && nprops.EC.Enabled {
		sameSlices := bprops.EC.DataSlices == nprops.EC.DataSlices && bprops.EC.ParitySlices == nprops.EC.ParitySlices
		sameLimit := bprops.EC.ObjSizeLimit == nprops.EC.ObjSizeLimit
		if !sameSlices || (!sameLimit && !propsToUpdate.Force) {
			err = fmt.Errorf("%s: once enabled, EC configuration can be only disabled but cannot change", p.si)
			return
		}
	} else if nprops.EC.Enabled {
		if nprops.EC.DataSlices == 0 {
			nprops.EC.DataSlices = 1
		}
		if nprops.EC.ParitySlices == 0 {
			nprops.EC.ParitySlices = 1
		}
	}
	if !bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		if nprops.Mirror.Copies == 1 {
			nprops.Mirror.Copies = cos.MaxI64(cfg.Mirror.Copies, 2)
		}
	} else if nprops.Mirror.Copies == 1 {
		nprops.Mirror.Enabled = false
	}
	if provider := nprops.BackendBck.Provider; nprops.BackendBck.Name != "" {
		nprops.BackendBck.Provider, err = cmn.NormalizeProvider(provider)
		if err != nil {
			return
		}
	}
	// cannot have re-mirroring and erasure coding on the same bucket at the same time
	remirror := _reMirror(bprops, nprops)
	targetCnt, reec := _reEC(bprops, nprops, bck, p.owner.smap.get())
	if len(creating) == 0 && remirror && reec {
		err = cmn.NewErrBckIsBusy(bck.Bucket())
		return
	}
	err = nprops.Validate(targetCnt)
	if cmn.IsErrSoft(err) && propsToUpdate.Force {
		glog.Warningf("Ignoring soft error: %v", err)
		err = nil
	}
	return
}

func _versioning(v bool) string {
	if v {
		return "enabled"
	}
	return "disabled"
}

func (p *proxy) initBackendProp(nprops *cmn.BucketProps) (err error) {
	if nprops.BackendBck.IsEmpty() {
		return
	}
	backend := cluster.CloneBck(&nprops.BackendBck)
	if err = backend.Validate(); err != nil {
		return
	}
	if err = backend.InitNoBackend(p.owner.bmd); err != nil {
		return
	}
	// NOTE: backend versioning override
	nprops.Versioning.Enabled = backend.Props.Versioning.Enabled
	return
}
