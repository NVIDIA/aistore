// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	notif "github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnServerCtx & prepTxnServer)
type (
	txnClientCtx struct {
		p       *proxyrunner
		uuid    string
		smap    *smapX
		msg     *aisMsg
		path    string
		timeout struct {
			netw time.Duration
			host time.Duration
		}
		req cmn.ReqArgs
	}
)

// NOTE
// - implementation-wise, a typical CP transaction will execute, with minor variations,
//   the same 6 (plus/minus) steps as shown below.
// - notice a certain symmetry between the client and the server sides whetreby
//   the control flow looks as follows:
//   	txnClientCtx =>
//   		(POST to /v1/txn) =>
//   			switch msg.Action =>
//   				txnServerCtx =>
//   					concrete transaction, etc.

// create-bucket: { check non-existence -- begin -- create locally -- metasync -- commit }
func (p *proxyrunner) createBucket(msg *cmn.ActionMsg, bck *cluster.Bck, remoteHeader ...http.Header) error {
	var (
		bucketProps *cmn.BucketProps
		nlp         = bck.GetNameLockPair()
		bmd         = p.owner.bmd.get()
	)
	if bck.Props != nil {
		bucketProps = bck.Props
	}
	if len(remoteHeader) != 0 && len(remoteHeader[0]) > 0 {
		remoteProps := defaultBckProps(bckPropsArgs{bck: bck, hdr: remoteHeader[0]})
		if bucketProps == nil {
			bucketProps = remoteProps
		} else {
			bucketProps.Versioning.Enabled = remoteProps.Versioning.Enabled // always takes precedence
		}
	} else if bck.HasBackendBck() {
		if bucketProps == nil {
			bucketProps = defaultBckProps(bckPropsArgs{bck: bck})
		}
		backend := cluster.BackendBck(bck)
		cloudProps, present := bmd.Get(backend)
		debug.Assert(present)
		bucketProps.Versioning.Enabled = cloudProps.Versioning.Enabled // always takes precedence
	} else if bck.IsCloud() || bck.IsHTTP() {
		return fmt.Errorf("creating a bucket for any of the cloud or HTTP providers is not supported")
	} else if bucketProps == nil {
		bucketProps = defaultBckProps(bckPropsArgs{bck: bck})
	}

	nlp.Lock()
	defer nlp.Unlock()

	// 1. try add
	if _, present := bmd.Get(bck); present {
		return cmn.NewErrBckAlreadyExists(bck.Bck)
	}

	// 2. begin
	var (
		waitmsync = true // commit blocks behind metasync
		c         = p.prepTxnClient(msg, bck, waitmsync)
	)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err := c.bcastAbort(bck, res.error())
		freeBcastRes(results)
		return err
	}
	freeBcastRes(results)

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:      _createBMDPre,
		final:    p._syncBMDFinal,
		wait:     waitmsync,
		msg:      &c.msg.ActionMsg,
		txnID:    c.uuid,
		bcks:     []*cluster.Bck{bck},
		setProps: bucketProps,
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		return c.bcastAbort(bck, err)
	}

	// 4. commit
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	for _, res := range results {
		if res.err == nil {
			continue
		}
		glog.Errorf("Failed to commit create-bucket (msg: %v, bck: %s, err: %v)", msg, bck, res.err)
		p.undoCreateBucket(msg, bck)
		err := res.error()
		freeBcastRes(results)
		return err
	}
	freeBcastRes(results)
	return nil
}

func _createBMDPre(ctx *bmdModifier, clone *bucketMD) (err error) {
	bck := ctx.bcks[0]
	added := clone.add(bck, ctx.setProps)
	if !added {
		err = cmn.NewErrBckAlreadyExists(bck.Bck)
	}
	return
}

func _destroyBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	bck := ctx.bcks[0]
	if _, present := clone.Get(bck); !present {
		return cmn.NewErrBckNotFound(bck.Bck)
	}
	deleted := clone.del(bck)
	cos.Assert(deleted)
	return nil
}

// make-n-copies: { confirm existence -- begin -- update locally -- metasync -- commit }
func (p *proxyrunner) makeNCopies(msg *cmn.ActionMsg, bck *cluster.Bck) (xactID string, err error) {
	copies, err := _parseNCopies(msg.Value)
	if err != nil {
		return
	}

	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		err = cmn.NewErrBckNotFound(bck.Bck)
		return
	}

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(msg, bck, waitmsync)
	)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = c.bcastAbort(bck, res.error())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// 3. update BMD locally & metasync updated BMD
	mirrorEnabled := copies > 1
	updateProps := &cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{
			Enabled: &mirrorEnabled,
			Copies:  &copies,
		},
	}
	ctx := &bmdModifier{
		pre:           _mirrorBMDPre,
		final:         p._syncBMDFinal,
		wait:          waitmsync,
		msg:           &c.msg.ActionMsg,
		txnID:         c.uuid,
		propsToUpdate: updateProps,
		bcks:          []*cluster.Bck{bck},
	}
	bmd, err = p.owner.bmd.modify(ctx)
	if err != nil {
		debug.AssertNoErr(err)
		err = c.bcastAbort(bck, err)
		return
	}
	c.msg.BMDVersion = bmd.version()

	// 4. IC
	nl := xact.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bck.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 5. commit
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	for _, res := range results {
		if res.err == nil {
			continue
		}
		glog.Error(res.err) // commit must go thru
		p.undoUpdateCopies(msg, bck, ctx.revertProps)
		err = res.error()
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)
	xactID = c.uuid
	return
}

func _mirrorBMDPre(ctx *bmdModifier, clone *bucketMD) error {
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
func (p *proxyrunner) setBucketProps(msg *cmn.ActionMsg, bck *cluster.Bck, nprops *cmn.BucketProps) (xactID string, err error) {
	// 1. confirm existence
	bprops, present := p.owner.bmd.get().Get(bck)
	if !present {
		err = cmn.NewErrBckNotFound(bck.Bck)
		return
	}
	bck.Props = bprops

	// 2. begin
	switch msg.Action {
	case cmn.ActSetBprops:
		// do nothing here (caller's responsible for validation)
	case cmn.ActResetBprops:
		var remoteBckProps http.Header
		if bck.IsRemote() {
			if bck.HasBackendBck() {
				err = fmt.Errorf("%q has backend %q - detach it prior to resetting the props",
					bck.Bck, bck.BackendBck())
				return
			}
			remoteBckProps, _, err = p.headRemoteBck(bck.Bck, nil)
			if err != nil {
				return "", err
			}
		}
		nprops = defaultBckProps(bckPropsArgs{bck: bck, hdr: remoteBckProps})
	default:
		cos.Assert(false)
	}
	// msg{propsToUpdate} => nmsg{nprops} and prep context(nmsg)
	nmsg := *msg
	nmsg.Value = nprops
	var (
		waitmsync = true
		c         = p.prepTxnClient(&nmsg, bck, waitmsync)
	)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = c.bcastAbort(bck, res.error())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:      p._setPropsPre,
		final:    p._syncBMDFinal,
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
	if ctx.needReMirror || ctx.needReEC {
		action := cmn.ActMakeNCopies
		if ctx.needReEC {
			action = cmn.ActECEncode
		}
		nl := xact.NewXactNL(c.uuid, action, &c.smap.Smap, nil, bck.Bck)
		nl.SetOwner(equalIC)
		p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})
		xactID = c.uuid
	}

	// 5. commit
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	freeBcastRes(results)
	return
}

func (p *proxyrunner) _setPropsPre(ctx *bmdModifier, clone *bucketMD) (err error) {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
	)
	cos.Assert(present)
	if ctx.msg.Action == cmn.ActSetBprops {
		bck.Props = bprops
	} else {
		targetCnt := p.owner.smap.Get().CountActiveTargets()
		debug.Assert(ctx.setProps != nil)
		debug.AssertNoErr(ctx.setProps.Validate(targetCnt))
	}
	ctx.needReMirror = reMirror(bprops, ctx.setProps)
	ctx.needReEC = reEC(bprops, ctx.setProps, bck)
	clone.set(bck, ctx.setProps)
	return nil
}

// rename-bucket: { confirm existence -- begin -- RebID -- metasync -- commit -- wait for rebalance and unlock }
func (p *proxyrunner) renameBucket(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (xactID string, err error) {
	if err = p.canRunRebalance(); err != nil {
		err = fmt.Errorf("%s: bucket %s cannot be renamed: %w", p.si, bckFrom, err)
		return
	}
	// 1. confirm existence & non-existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		err = cmn.NewErrBckNotFound(bckFrom.Bck)
		return
	}
	if _, present := bmd.Get(bckTo); present {
		err = cmn.NewErrBckAlreadyExists(bckTo.Bck)
		return
	}

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(msg, bckFrom, waitmsync)
	)
	_ = cmn.AddBckUnameToQuery(c.req.Query, bckTo.Bck, cmn.URLParamBucketTo)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = c.bcastAbort(bckFrom, res.error())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// 3. update BMD locally & metasync updated BMD
	bmdCtx := &bmdModifier{
		pre:          _renameBMDPre,
		final:        p._syncBMDFinal,
		msg:          msg,
		txnID:        c.uuid,
		bcks:         []*cluster.Bck{bckFrom, bckTo},
		wait:         waitmsync,
		singleTarget: c.smap.CountActiveTargets() == 1,
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
	nl := xact.NewXactNL(c.uuid, c.msg.Action, &c.smap.Smap, nil, bckFrom.Bck, bckTo.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{smap: c.smap, nl: nl, query: c.req.Query})

	// 5. commit
	xactID = c.uuid
	c.req.Body = cos.MustMarshal(c.msg)
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	freeBcastRes(results)

	// 6. start rebalance and resilver
	wg := p.metasyncer.sync(revsPair{rmd, c.msg})

	// Register rebalance `nl`
	nl = xact.NewXactNL(xact.RebID2S(rmd.Version), cmn.ActRebalance, &c.smap.Smap, nil)
	nl.SetOwner(equalIC)
	err = p.notifs.add(nl)
	debug.AssertNoErr(err)

	// Register resilver `nl`
	nl = xact.NewXactNL(rmd.Resilver, cmn.ActResilver, &c.smap.Smap, nil)
	nl.SetOwner(equalIC)
	err = p.notifs.add(nl)
	debug.AssertNoErr(err)

	wg.Wait()
	return
}

func _renameBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bckFrom, bckTo  = ctx.bcks[0], ctx.bcks[1]
		bprops, present = clone.Get(bckFrom)
	)
	debug.Assert(present)
	bckFrom.Props = bprops.Clone()
	bckTo.Props = bprops.Clone()
	added := clone.add(bckTo, bckTo.Props)
	cos.Assert(added)
	bckFrom.Props.Renamed = cmn.ActMoveBck // NOTE: state until `BMDVersionFixup` by renaming xaction
	clone.set(bckFrom, bckFrom.Props)
	return nil
}

// transform (or simply copy) bucket to another bucket
// { confirm existence -- begin -- conditional metasync -- start waiting for operation done -- commit }
func (p *proxyrunner) tcb(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg, dryRun bool) (xactID string, err error) {
	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, existsFrom := bmd.Get(bckFrom); !existsFrom {
		err = cmn.NewErrBckNotFound(bckFrom.Bck)
		return
	}
	_, existsTo := bmd.Get(bckTo)
	debug.Assert(existsTo || bckTo.IsAIS())

	// 2. begin
	var (
		waitmsync = !dryRun
		c         = p.prepTxnClient(msg, bckFrom, waitmsync)
	)
	_ = cmn.AddBckUnameToQuery(c.req.Query, bckTo.Bck, cmn.URLParamBucketTo)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			if xactID == "" {
				xactID = res.header.Get(cmn.HdrXactionID)
			}
			continue
		}
		err = c.bcastAbort(bckFrom, res.error())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:   _b2bBMDPre,
		final: p._syncBMDFinal,
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
			c.req.Query.Set(cmn.URLParamWaitMetasync, "true")
		}
	}

	// 4. IC
	nl := xact.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bckFrom.Bck, bckTo.Bck)
	nl.SetOwner(equalIC)
	// setup notification listener callback to cleanup upon failure
	nl.F = func(nl notif.NotifListener) {
		if errNl := nl.Err(); errNl != nil {
			if !ctx.terminate { // undo bmd.modify() - see above
				glog.Error(errNl)
				p.destroyBucket(&cmn.ActionMsg{Action: cmn.ActDestroyBck}, bckTo)
			}
		}
	}
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 5. commit
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	freeBcastRes(results)
	xactID = c.uuid
	return
}

// transform or copy a list or a range of objects
func (p *proxyrunner) tcobjs(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (xactID string, err error) {
	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		err = cmn.NewErrBckNotFound(bckFrom.Bck)
		return
	}
	// 2. begin
	var (
		waitmsync = false
		c         = p.prepTxnClient(msg, bckFrom, waitmsync)
	)
	_ = cmn.AddBckUnameToQuery(c.req.Query, bckTo.Bck, cmn.URLParamBucketTo)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			if xactID == "" {
				xactID = res.header.Get(cmn.HdrXactionID)
			}
			continue
		}
		err = c.bcastAbort(bckFrom, res.error())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// 3. commit
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	freeBcastRes(results)
	return
}

func _b2bBMDPre(ctx *bmdModifier, clone *bucketMD) error {
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

func parseECConf(value interface{}) (*cmn.ECConfToUpdate, error) {
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
func (p *proxyrunner) ecEncode(bck *cluster.Bck, msg *cmn.ActionMsg) (xactID string, err error) {
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
	config := cmn.GCO.Get()
	if !nlp.TryLock(config.Timeout.CplaneOperation.D() / 2) {
		err = cmn.NewErrBckIsBusy(bck.Bck)
		return
	}
	defer nlp.Unlock()

	// 1. confirm existence
	props, present := p.owner.bmd.get().Get(bck)
	if !present {
		err = cmn.NewErrBckNotFound(bck.Bck)
		return
	}
	if props.EC.Enabled {
		// Changing data or parity slice count on the fly is unsupported yet
		err = fmt.Errorf("%s: EC is already enabled for bucket %s", p.si, bck)
		return
	}

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(msg, bck, waitmsync)
	)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = c.bcastAbort(bck, res.error())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:           _updatePropsBMDPre,
		final:         p._syncBMDFinal,
		bcks:          []*cluster.Bck{bck},
		wait:          waitmsync,
		msg:           &c.msg.ActionMsg,
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
	nl := xact.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bck.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	for _, res := range results {
		if res.err == nil {
			continue
		}
		glog.Error(res.err)
		err = res.error()
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)
	xactID = c.uuid
	return
}

func _updatePropsBMDPre(ctx *bmdModifier, clone *bucketMD) error {
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

func (p *proxyrunner) createArchMultiObj(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (xactID string, err error) {
	// begin
	c := p.prepTxnClient(msg, bckFrom, false /*waitmsync*/)
	_ = cmn.AddBckUnameToQuery(c.req.Query, bckTo.Bck, cmn.URLParamBucketTo)
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			if xactID == "" {
				xactID = res.header.Get(cmn.HdrXactionID)
			}
			continue
		}
		err = c.bcastAbort(bckFrom, res.error())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// commit
	results = c.bcast(cmn.ActCommit, 2*c.timeout.netw) // massive archiving vs workCh capacity
	for _, res := range results {
		if res.err == nil {
			continue
		}
		glog.Error(res.err)
		err = res.error()
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)
	return
}

// maintenance: { begin -- enable GFN -- commit -- start rebalance }
func (p *proxyrunner) startMaintenance(si *cluster.Snode, msg *cmn.ActionMsg, opts *cmn.ActValRmNode) (rebID string, err error) {
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
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = c.bcastAbort(si, res.error(), cmn.Target)
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// 2. Put node under maintenance
	if err = p.markMaintenance(msg, si); err != nil {
		c.bcastAbort(si, err, cmn.Target)
		return
	}

	// 3. Commit
	// NOTE: Call only the target that's being decommissioned (commit is a no-op for the rest)
	if msg.Action == cmn.ActDecommissionNode || msg.Action == cmn.ActShutdownNode {
		c.req.Path = cos.JoinWords(c.path, cmn.ActCommit)
		res := p.call(callArgs{si: si, req: c.req, timeout: c.commitTimeout(waitmsync)})
		err = res.error()
		freeCR(res)
		if err != nil {
			glog.Error(err)
			return
		}
	}

	// 4. Start rebalance
	if !opts.SkipRebalance && rebEnabled {
		return p.rebalanceAndRmSelf(msg, si)
	} else if msg.Action == cmn.ActDecommissionNode {
		_, err = p.callRmSelf(msg, si, true /*skipReb*/)
	}
	return
}

// Put node under maintenance
func (p *proxyrunner) markMaintenance(msg *cmn.ActionMsg, si *cluster.Snode) error {
	var flags cos.BitFlags
	switch msg.Action {
	case cmn.ActDecommissionNode:
		flags = cluster.NodeFlagDecomm
	case cmn.ActStartMaintenance, cmn.ActShutdownNode:
		flags = cluster.NodeFlagMaint
	default:
		err := fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{cmn.ActDecommissionNode, cmn.ActStartMaintenance, cmn.ActShutdownNode})
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

func (p *proxyrunner) _markMaint(ctx *smapModifier, clone *smapX) error {
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot put %s in maintenance", ctx.sid))
	}
	clone.setNodeFlags(ctx.sid, ctx.flags)
	clone.staffIC()
	return nil
}

// destroy bucket: { begin -- commit }
func (p *proxyrunner) destroyBucket(msg *cmn.ActionMsg, bck *cluster.Bck) error {
	nlp := bck.GetNameLockPair()
	nlp.Lock()
	defer nlp.Unlock()

	actMsg := &cmn.ActionMsg{}
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
	results := c.bcast(cmn.ActBegin, c.timeout.netw)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err := c.bcastAbort(bck, res.error())
		freeBcastRes(results)
		return err
	}
	freeBcastRes(results)

	// 2. Distribute new BMD
	ctx := &bmdModifier{
		pre:   _destroyBMDPre,
		final: p._syncBMDFinal,
		msg:   msg,
		txnID: c.uuid,
		wait:  waitmsync,
		bcks:  []*cluster.Bck{bck},
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		return c.bcastAbort(bck, err)
	}

	// 3. Commit
	results = c.bcast(cmn.ActCommit, c.commitTimeout(waitmsync))
	for _, res := range results {
		if res.err == nil {
			continue
		}
		glog.Errorf("Failed to commit destroy-bucket (msg: %v, bck: %s, err: %v)", msg, bck, res.err)
		err := res.error()
		freeBcastRes(results)
		return err
	}
	freeBcastRes(results)
	return nil
}

// erase bucket data from all targets (keep metadata)
func (p *proxyrunner) destroyBucketData(msg *cmn.ActionMsg, bck *cluster.Bck) error {
	query := cmn.AddBckToQuery(
		url.Values{cmn.URLParamKeepBckMD: []string{"true"}},
		bck.Bck)
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{
		Method: http.MethodDelete,
		Path:   cmn.URLPathBuckets.Join(bck.Name),
		Body:   cos.MustMarshal(msg),
		Query:  query,
	}
	args.to = cluster.Targets
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err != nil {
			return res.err
		}
	}
	freeBcastRes(results)
	return nil
}

//////////////////////////////////////
// context, rollback & misc helpers //
//////////////////////////////////////

func (c *txnClientCtx) commitTimeout(waitmsync bool) time.Duration {
	if waitmsync {
		return c.timeout.host + c.timeout.netw
	}
	return c.timeout.netw
}

func (c *txnClientCtx) bcast(phase string, timeout time.Duration) sliceResults {
	c.req.Path = cos.JoinWords(c.path, phase)
	if phase != cmn.ActAbort {
		now := time.Now()
		c.req.Query.Set(cmn.URLParamUnixTime, cos.UnixNano2S(now.UnixNano()))
	}

	args := allocBcastArgs()
	defer freeBcastArgs(args)

	args.req = c.req
	args.smap = c.smap
	args.timeout = timeout
	return c.p.bcastGroup(args)
}

func (c *txnClientCtx) bcastAbort(val fmt.Stringer, err error, key ...string) error {
	k := "bucket"
	if len(key) > 0 {
		k = key[0]
	}
	glog.Errorf("Abort %q, %s %s, err: %v)", c.msg.Action, k, val, err)
	results := c.bcast(cmn.ActAbort, 0)
	freeBcastRes(results)
	return err
}

// txn client context
func (p *proxyrunner) prepTxnClient(msg *cmn.ActionMsg, bck *cluster.Bck, waitmsync bool) *txnClientCtx {
	c := &txnClientCtx{p: p, uuid: cos.GenUUID(), smap: p.owner.smap.get()}
	c.msg = p.newAmsg(msg, nil, c.uuid)
	body := cos.MustMarshal(c.msg)

	query := make(url.Values, 2)
	if bck == nil {
		c.path = cmn.URLPathTxn.S
	} else {
		c.path = cmn.URLPathTxn.Join(bck.Name)
		query = cmn.AddBckToQuery(query, bck.Bck)
	}
	config := cmn.GCO.Get()
	c.timeout.netw = config.Timeout.MaxKeepalive.D()
	if !waitmsync { // when commit does not block behind metasync
		query.Set(cmn.URLParamNetwTimeout, cos.UnixNano2S(int64(c.timeout.netw)))
	}
	c.timeout.host = config.Timeout.MaxHostBusy.D()
	query.Set(cmn.URLParamHostTimeout, cos.UnixNano2S(int64(c.timeout.host)))

	c.req = cmn.ReqArgs{Method: http.MethodPost, Query: query, Body: body}
	return c
}

// rollback create-bucket
func (p *proxyrunner) undoCreateBucket(msg *cmn.ActionMsg, bck *cluster.Bck) {
	ctx := &bmdModifier{
		pre:   _destroyBMDPre,
		final: p._syncBMDFinal,
		msg:   msg,
		bcks:  []*cluster.Bck{bck},
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		cos.AssertNoErr(err)
	}
}

// rollback make-n-copies
func (p *proxyrunner) undoUpdateCopies(msg *cmn.ActionMsg, bck *cluster.Bck, propsToUpdate *cmn.BucketPropsToUpdate) {
	ctx := &bmdModifier{
		pre:           _updatePropsBMDPre,
		final:         p._syncBMDFinal,
		msg:           msg,
		propsToUpdate: propsToUpdate,
		bcks:          []*cluster.Bck{bck},
	}
	if _, err := p.owner.bmd.modify(ctx); err != nil {
		cos.AssertNoErr(err)
	}
}

// Make and validate new bucket props.
func (p *proxyrunner) makeNewBckProps(bck *cluster.Bck, propsToUpdate *cmn.BucketPropsToUpdate,
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

	// cannot run make-n-copies and EC on the same bucket at the same time
	remirror := reMirror(bprops, nprops)
	reec := reEC(bprops, nprops, bck)
	if len(creating) == 0 && remirror && reec {
		err = cmn.NewErrBckIsBusy(bck.Bck)
		return
	}

	targetCnt := p.owner.smap.Get().CountActiveTargets()
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

func (p *proxyrunner) initBackendProp(nprops *cmn.BucketProps) (err error) {
	if nprops.BackendBck.IsEmpty() {
		return
	}
	backend := cluster.NewBckEmbed(nprops.BackendBck)
	if err = backend.InitNoBackend(p.owner.bmd); err != nil {
		return
	}
	// NOTE: backend versioning override
	nprops.Versioning.Enabled = backend.Props.Versioning.Enabled
	return
}
