// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnServerCtx & prepTxnServer)
type (
	txnClientCtx struct {
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
func (p *proxyrunner) createBucket(msg *cmn.ActionMsg, bck *cluster.Bck, cloudHeader ...http.Header) error {
	var (
		bucketProps = cmn.DefaultAISBckProps()
		nlp         = bck.GetNameLockPair()
	)

	if bck.Props != nil {
		bucketProps = bck.Props
	}
	if len(cloudHeader) != 0 {
		cloudProps := cmn.DefaultCloudBckProps(cloudHeader[0])
		if bck.Props == nil {
			bucketProps = cloudProps
		} else {
			bucketProps.Versioning.Enabled = cloudProps.Versioning.Enabled // always takes precedence
		}
	}

	nlp.Lock()
	defer nlp.Unlock()

	// 1. try add
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); present {
		return cmn.NewErrorBucketAlreadyExists(bck.Bck, p.si.String())
	}

	// 2. begin
	var (
		waitmsync = true // commit blocks behind metasync
		c         = p.prepTxnClient(msg, bck, waitmsync)
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			return res.err
		}
	}

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:      p._createBMDPre,
		final:    p._syncBMDFinal,
		wait:     waitmsync,
		msg:      &c.msg.ActionMsg,
		txnID:    c.uuid,
		bcks:     []*cluster.Bck{bck},
		setProps: bucketProps,
	}
	bmd, _ = p.owner.bmd.modify(ctx)
	c.msg.BMDVersion = bmd.version()

	// 4. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)

	results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: c.commitTimeout(waitmsync)})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err) // commit must go thru
			p.undoCreateBucket(msg, bck)
			return res.err
		}
	}
	return nil
}

func (p *proxyrunner) _createBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	added := clone.add(ctx.bcks[0], ctx.setProps) // TODO: Bucket could be added during begin.
	cmn.Assert(added)
	return nil
}

func (p *proxyrunner) _destroyBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	bck := ctx.bcks[0]
	if _, present := clone.Get(bck); !present {
		return cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
	}
	deleted := clone.del(bck)
	cmn.Assert(deleted)
	return nil
}

// make-n-copies: { confirm existence -- begin -- update locally -- metasync -- commit }
func (p *proxyrunner) makeNCopies(msg *cmn.ActionMsg, bck *cluster.Bck) (xactID string, err error) {
	copies, err := p.parseNCopies(msg.Value)
	if err != nil {
		return
	}

	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		err = cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
		return
	}

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(msg, bck, waitmsync)
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
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
		pre:           p._mirrorBMDPre,
		final:         p._syncBMDFinal,
		wait:          waitmsync,
		msg:           &c.msg.ActionMsg,
		txnID:         c.uuid,
		propsToUpdate: updateProps,
		bcks:          []*cluster.Bck{bck},
	}
	bmd, _ = p.owner.bmd.modify(ctx)
	c.msg.BMDVersion = bmd.version()

	// 4. IC
	nl := xaction.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bck.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 5. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
	results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err) // commit must go thru
			p.undoUpdateCopies(msg, bck, ctx.revertProps)
			err = res.err
			return
		}
	}
	xactID = c.uuid
	return
}

func (p *proxyrunner) _mirrorBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
	)
	cmn.Assert(present)
	nprops := bprops.Clone()
	nprops.Apply(*ctx.propsToUpdate)
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
func (p *proxyrunner) setBucketProps(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, bck *cluster.Bck,
	propsToUpdate cmn.BucketPropsToUpdate) (xactID string, err error) {
	var (
		nprops *cmn.BucketProps   // complete version of bucket props containing propsToUpdate changes
		nmsg   = &cmn.ActionMsg{} // with nprops
	)

	// 1. confirm existence
	bprops, present := p.owner.bmd.get().Get(bck)
	if !present {
		err = cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
		return
	}
	bck.Props = bprops

	// 2. begin
	switch msg.Action {
	case cmn.ActSetBprops:
		// make and validate new props
		if nprops, err = p.makeNprops(bck, propsToUpdate); err != nil {
			return
		}

		if !nprops.BackendBck.IsEmpty() {
			// Makes sure that there will be no other forwarding to proxy.
			cmn.Assert(p.owner.smap.get().isPrimary(p.si))
			// Make sure that destination bucket exists.
			backendBck := cluster.NewBckEmbed(nprops.BackendBck)
			args := remBckAddArgs{p: p, w: w, r: r, queryBck: backendBck, err: err, msg: msg}
			if _, err = args.initAndTry(backendBck.Name); err != nil {
				return
			}
		}

		// Make sure that backend bucket was initialized correctly.
		if err = p.checkBackendBck(nprops); err != nil {
			return
		}
	case cmn.ActResetBprops:
		if bck.IsCloud() {
			if bck.HasBackendBck() {
				err = fmt.Errorf("%q has backend %q - detach it prior to resetting the props",
					bck.Bck, bck.BackendBck())
				return
			}
			cloudProps, _, err := p.headCloudBck(bck.Bck, nil)
			if err != nil {
				return "", err
			}
			nprops = cmn.DefaultCloudBckProps(cloudProps)
		} else {
			nprops = cmn.DefaultAISBckProps()
		}
	default:
		cmn.Assert(false)
	}
	// msg{propsToUpdate} => nmsg{nprops} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = nprops
	var (
		waitmsync = true
		c         = p.prepTxnClient(nmsg, bck, waitmsync)
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:           p._setPropsPre,
		final:         p._syncBMDFinal,
		wait:          waitmsync,
		msg:           msg,
		txnID:         c.uuid,
		setProps:      nprops,
		propsToUpdate: &propsToUpdate,
		bcks:          []*cluster.Bck{bck},
	}
	bmd, err := p.owner.bmd.modify(ctx)
	if err != nil {
		return "", err
	}
	c.msg.BMDVersion = bmd.version()

	// 4. if remirror|re-EC|TBD-storage-svc
	if ctx.needReMirror || ctx.needReEC {
		action := cmn.ActMakeNCopies
		if ctx.needReEC {
			action = cmn.ActECEncode
		}
		nl := xaction.NewXactNL(c.uuid, action, &c.smap.Smap, nil, bck.Bck)
		nl.SetOwner(equalIC)
		p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})
		xactID = c.uuid
	}

	// 5. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
	_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	return
}

func (p *proxyrunner) _setPropsPre(ctx *bmdModifier, clone *bucketMD) (err error) {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
	)
	cmn.Assert(present)

	if ctx.msg.Action == cmn.ActSetBprops {
		bck.Props = bprops
		ctx.setProps, err = p.makeNprops(bck, *ctx.propsToUpdate)
		if err != nil {
			return err
		}

		// BackendBck (if present) should be already locally available (see cmn.ActSetBprops).
		if err := p.checkBackendBck(ctx.setProps); err != nil {
			return err
		}
	}

	ctx.needReMirror = reMirror(bprops, ctx.setProps)
	ctx.needReEC = reEC(bprops, ctx.setProps, bck)
	clone.set(bck, ctx.setProps)
	return nil
}

// rename-bucket: { confirm existence -- begin -- RebID -- metasync -- commit -- wait for rebalance and unlock }
func (p *proxyrunner) renameBucket(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (xactID string, err error) {
	nmsg := &cmn.ActionMsg{} // + bckTo
	if rebErr := p.canStartRebalance(); rebErr != nil {
		err = fmt.Errorf("%s: bucket cannot be renamed: %w", p.si, rebErr)
		return
	}
	// 1. confirm existence & non-existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		err = cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, p.si.String())
		return
	}
	if _, present := bmd.Get(bckTo); present {
		err = cmn.NewErrorBucketAlreadyExists(bckTo.Bck, p.si.String())
		return
	}

	// msg{} => nmsg{bckTo} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = bckTo.Bck

	// 2. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(nmsg, bckFrom, waitmsync)
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. update BMD locally & metasync updated BMD
	bmdCtx := &bmdModifier{
		pre:   p._renameBMDPre,
		final: p._syncBMDFinal,
		msg:   msg,
		txnID: c.uuid,
		bcks:  []*cluster.Bck{bckFrom, bckTo},
		wait:  waitmsync,
	}

	bmd, _ = p.owner.bmd.modify(bmdCtx)
	c.msg.BMDVersion = bmd.version()

	ctx := &rmdModifier{
		pre: func(_ *rmdModifier, clone *rebMD) {
			clone.inc()
			clone.Resilver = cmn.GenUUID()
		},
	}

	rmd := p.owner.rmd.modify(ctx)
	c.msg.RMDVersion = rmd.version()

	// 4. IC
	nl := xaction.NewXactNL(c.uuid, c.msg.Action,
		&c.smap.Smap, nil, bckFrom.Bck, bckTo.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{smap: c.smap, nl: nl, query: c.req.Query})

	// 5. commit
	xactID = c.uuid
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
	c.req.Body = cmn.MustMarshal(c.msg)

	_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})

	// 6. start rebalance and resilver
	wg := p.metasyncer.sync(revsPair{rmd, c.msg})

	// Register rebalance `nl`
	nl = xaction.NewXactNL(xaction.RebID(rmd.Version).String(),
		cmn.ActRebalance, &c.smap.Smap, nil)
	nl.SetOwner(equalIC)

	// Rely on metasync to register rebalance/resilver `nl` on all IC members.  See `p.receiveRMD`.
	err = p.notifs.add(nl)
	cmn.AssertNoErr(err)

	// Register resilver `nl`
	nl = xaction.NewXactNL(rmd.Resilver, cmn.ActResilver, &c.smap.Smap, nil)
	nl.SetOwner(equalIC)

	// Rely on metasync to register rebalanace/resilver `nl` on all IC members.  See `p.receiveRMD`.
	err = p.notifs.add(nl)
	cmn.AssertNoErr(err)

	wg.Wait()
	return
}

func (p *proxyrunner) _renameBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bckFrom, bckTo  = ctx.bcks[0], ctx.bcks[1]
		bprops, present = clone.Get(bckFrom) // TODO: Bucket could be removed during begin.
	)

	cmn.Assert(present)
	bckFrom.Props = bprops.Clone()
	bckTo.Props = bprops.Clone()

	added := clone.add(bckTo, bckTo.Props)
	cmn.Assert(added)
	bckFrom.Props.Renamed = cmn.ActRenameLB
	clone.set(bckFrom, bckFrom.Props)
	return nil
}

// copy-bucket/offline ETL:
// { confirm existence -- begin -- conditional metasync -- start waiting for operation done -- commit }
func (p *proxyrunner) bucketToBucketTxn(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg, dryRun bool) (xactID string, err error) {
	cmn.Assert(!bckTo.IsHTTP())
	cmn.Assert(msg.Value != nil)

	// 1. confirm existence
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		err = cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, p.si.String())
		return
	}

	// 2. begin
	var (
		waitmsync = !dryRun
		c         = p.prepTxnClient(msg, bckFrom, waitmsync)
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. update BMD locally & metasync updated BMD
	if !dryRun {
		ctx := &bmdModifier{
			pre:   p._b2bBMDPre,
			final: p._syncBMDFinal,
			msg:   msg,
			txnID: c.uuid,
			bcks:  []*cluster.Bck{bckFrom, bckTo},
			wait:  waitmsync,
		}
		bmd, _ = p.owner.bmd.modify(ctx)
		c.msg.BMDVersion = bmd.version()
		if !ctx.terminate {
			c.req.Query.Set(cmn.URLParamWaitMetasync, "true")
		}
	}

	// 4. IC
	nl := xaction.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bckFrom.Bck, bckTo.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 5. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
	_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	xactID = c.uuid
	return
}

func (p *proxyrunner) _b2bBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bckFrom, bckTo  = ctx.bcks[0], ctx.bcks[1]
		bprops, present = clone.Get(bckFrom) // TODO: Bucket could be removed during begin.
	)
	cmn.Assert(present)

	// Skip destination bucket creation if it's dry run or it's already present.
	if _, present = clone.Get(bckTo); present {
		ctx.terminate = true
		return nil
	}

	cmn.Assert(!bckTo.IsRemote())
	bckFrom.Props = bprops.Clone()
	bckTo.Props = bprops.Clone()
	added := clone.add(bckTo, bckTo.Props)
	cmn.Assert(added)
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
	var (
		pname      = p.si.String()
		nlp        = bck.GetNameLockPair()
		unlockUpon bool
	)

	ecConf, err := parseECConf(msg.Value)
	if err != nil {
		return
	}
	if ecConf.DataSlices == nil || *ecConf.DataSlices < 1 ||
		ecConf.ParitySlices == nil || *ecConf.ParitySlices < 1 {
		err = errors.New("invalid number of slices")
		return
	}

	if !nlp.TryLock() {
		err = cmn.NewErrorBucketIsBusy(bck.Bck, pname)
		return
	}
	defer func() {
		if !unlockUpon {
			nlp.Unlock()
		}
	}()

	// 1. confirm existence
	props, present := p.owner.bmd.get().Get(bck)
	if !present {
		err = cmn.NewErrorBucketDoesNotExist(bck.Bck, pname)
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
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. update BMD locally & metasync updated BMD
	ctx := &bmdModifier{
		pre:           p._updatePropsBMDPre,
		final:         p._syncBMDFinal,
		bcks:          []*cluster.Bck{bck},
		wait:          waitmsync,
		msg:           &c.msg.ActionMsg,
		txnID:         c.uuid,
		propsToUpdate: &cmn.BucketPropsToUpdate{EC: ecConf},
	}
	bmd, _ := p.owner.bmd.modify(ctx)
	c.msg.BMDVersion = bmd.version()

	// 5. IC
	nl := xaction.NewXactNL(c.uuid, msg.Action, &c.smap.Smap, nil, bck.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	unlockUpon = true
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
	results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: c.commitTimeout(waitmsync)})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err)
			err = res.err
			return
		}
	}
	xactID = c.uuid
	return
}

func (p *proxyrunner) _updatePropsBMDPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
	)
	if !present {
		ctx.terminate = true
		return nil
	}
	nprops := bprops.Clone()
	nprops.Apply(*ctx.propsToUpdate)
	clone.set(bck, nprops)
	return nil
}

// maintenance: { begin -- enable GFN -- commit -- start rebalance }
func (p *proxyrunner) startMaintenance(si *cluster.Snode, msg *cmn.ActionMsg,
	opts *cmn.ActValDecommision) (rebID xaction.RebID, err error) {
	if si.IsProxy() {
		p.markMaintenance(msg, si)
		if msg.Action == cmn.ActDecommission {
			_, err = p.unregisterNode(msg, si, true /*skipReb*/)
		}
		return
	}

	// 1. begin
	var (
		waitmsync = false
		c         = p.prepTxnClient(msg, nil, waitmsync)
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 2. Put node under maintenance
	if err = p.markMaintenance(msg, si); err != nil {
		c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
		_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
		return
	}

	// 3. Commit
	// NOTE: Call only the target being decommissioned, on all other targets commit phase is a null operation.
	if msg.Action == cmn.ActDecommission {
		c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
		res := p.call(callArgs{si: si, req: c.req, timeout: c.commitTimeout(waitmsync)})
		if res.err != nil {
			glog.Error(res.err)
			err = res.err
			return
		}
	}

	// 4. Start rebalance
	if !opts.SkipRebalance {
		return p.finalizeMaintenance(msg, si)
	} else if msg.Action == cmn.ActDecommission {
		_, err = p.unregisterNode(msg, si, true /*skipReb*/)
	}
	return
}

// destroy bucket: { begin -- commit }
func (p *proxyrunner) destroyBucket(msg *cmn.ActionMsg, bck *cluster.Bck) (err error) {
	nlp := bck.GetNameLockPair()
	nlp.Lock()
	defer nlp.Unlock()

	actMsg := &cmn.ActionMsg{}
	*actMsg = *msg

	// 1. begin
	var (
		waitmsync = true
		c         = p.prepTxnClient(actMsg, bck, waitmsync)
		results   = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 2. Distribute new BMD
	ctx := &bmdModifier{
		pre:   p._destroyBMDPre,
		final: p._syncBMDFinal,
		msg:   msg,
		wait:  waitmsync,
		bcks:  []*cluster.Bck{bck},
	}
	_, err = p.owner.bmd.modify(ctx)
	if err != nil {
		c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
		_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
		return
	}

	// 3. Commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
	results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: c.commitTimeout(waitmsync)})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err)
			err = res.err
			return
		}
	}

	return
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

// txn client context
func (p *proxyrunner) prepTxnClient(msg *cmn.ActionMsg, bck *cluster.Bck, waitmsync bool) *txnClientCtx {
	c := &txnClientCtx{
		uuid: cmn.GenUUID(),
		smap: p.owner.smap.get(),
	}
	c.msg = p.newAisMsg(msg, c.smap, nil, c.uuid)
	body := cmn.MustMarshal(c.msg)

	query := make(url.Values, 2)
	if bck == nil {
		c.path = cmn.JoinWords(cmn.Version, cmn.Txn)
	} else {
		c.path = cmn.JoinWords(cmn.Version, cmn.Txn, bck.Name)
		query = cmn.AddBckToQuery(query, bck.Bck)
	}
	config := cmn.GCO.Get()
	c.timeout.netw = config.Timeout.MaxKeepalive
	if !waitmsync { // when commit does not block behind metasync
		query.Set(cmn.URLParamNetwTimeout, cmn.UnixNano2S(int64(c.timeout.netw)))
	}
	c.timeout.host = config.Timeout.MaxHostBusy
	query.Set(cmn.URLParamHostTimeout, cmn.UnixNano2S(int64(c.timeout.host)))

	c.req = cmn.ReqArgs{Method: http.MethodPost, Path: cmn.JoinWords(c.path, cmn.ActBegin), Query: query, Body: body}
	return c
}

// rollback create-bucket
func (p *proxyrunner) undoCreateBucket(msg *cmn.ActionMsg, bck *cluster.Bck) {
	ctx := &bmdModifier{
		pre:   p._destroyBMDPre,
		final: p._syncBMDFinal,
		msg:   msg,
		bcks:  []*cluster.Bck{bck},
	}
	p.owner.bmd.modify(ctx)
}

// rollback make-n-copies
func (p *proxyrunner) undoUpdateCopies(msg *cmn.ActionMsg, bck *cluster.Bck, propsToUpdate *cmn.BucketPropsToUpdate) {
	ctx := &bmdModifier{
		pre:           p._updatePropsBMDPre,
		final:         p._syncBMDFinal,
		msg:           msg,
		propsToUpdate: propsToUpdate,
		bcks:          []*cluster.Bck{bck},
	}
	p.owner.bmd.modify(ctx)
}

// make and validate nprops
func (p *proxyrunner) makeNprops(bck *cluster.Bck, propsToUpdate cmn.BucketPropsToUpdate,
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
	}
	if bprops.EC.Enabled && nprops.EC.Enabled {
		if !reflect.DeepEqual(bprops.EC, nprops.EC) {
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
			nprops.Mirror.Copies = cmn.MaxI64(cfg.Mirror.Copies, 2)
		}
	} else if nprops.Mirror.Copies == 1 {
		nprops.Mirror.Enabled = false
	}

	// cannot run make-n-copies and EC on the same bucket at the same time
	remirror := reMirror(bprops, nprops)
	reec := reEC(bprops, nprops, bck)
	if len(creating) == 0 && remirror && reec {
		err = cmn.NewErrorBucketIsBusy(bck.Bck, p.si.String())
		return
	}

	targetCnt := p.owner.smap.Get().CountActiveTargets()
	err = nprops.Validate(targetCnt)
	return
}

func _versioning(v bool) string {
	if v {
		return "enabled"
	}
	return "disabled"
}

func (p *proxyrunner) checkBackendBck(nprops *cmn.BucketProps) (err error) {
	if nprops.BackendBck.IsEmpty() {
		return
	}
	backend := cluster.NewBckEmbed(nprops.BackendBck)
	if err = backend.InitNoBackend(p.owner.bmd, p.si); err != nil {
		return
	}

	// NOTE: backend versioning override
	nprops.Versioning.Enabled = backend.Props.Versioning.Enabled
	return
}
