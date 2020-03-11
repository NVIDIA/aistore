// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnServerCtx & prepTxnServer)
type txnClientCtx struct {
	uuid    string
	smap    *smapX
	msgInt  *actionMsgInternal
	body    []byte
	query   url.Values
	path    string
	timeout time.Duration
	req     cmn.ReqArgs
}

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

// create-bucket transaction: { create bucket locally -- begin -- metasync -- commit } - 6 steps total
func (p *proxyrunner) createBucket(msg *cmn.ActionMsg, bck *cluster.Bck, cloudHeader ...http.Header) error {
	var (
		bucketProps = cmn.DefaultBucketProps()
		nlp         = bck.GetNameLockPair()
	)
	if len(cloudHeader) != 0 {
		bucketProps = cmn.CloudBucketProps(cloudHeader[0])
	}

	nlp.Lock()
	defer nlp.Unlock()

	// 1. lock & try add
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); present {
		p.owner.bmd.Unlock()
		return cmn.NewErrorBucketAlreadyExists(bck.Bck, p.si.String())
	}

	// 2. prep context, lock bucket and unlock BMD (in that order)
	var (
		c = p.prepTxnClient(msg, bck)
	)
	p.owner.bmd.Unlock()

	// 3. begin
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for result := range results {
		if result.err != nil {
			// 3. abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			return result.err
		}
	}

	// 4. lock & update BMD locally, unlock bucket
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	added := clone.add(bck, bucketProps)
	cmn.Assert(added)
	p.owner.bmd.put(clone)

	// 5. metasync updated BMD & unlock BMD
	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revsPair{clone, msgInt})
	p.owner.bmd.Unlock()

	// 6. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	results = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for result := range results {
		if result.err != nil {
			p.undoCreateBucket(msg, bck)
			return result.err
		}
	}
	return nil
}

// make-n-copies transaction: { setprop bucket locally -- begin -- metasync -- commit } - 6 steps total
func (p *proxyrunner) makeNCopies(msg *cmn.ActionMsg, bck *cluster.Bck) error {
	var (
		c           = p.prepTxnClient(msg, bck)
		nlp         = bck.GetNameLockPair()
		copies, err = p.parseNCopies(msg.Value)
	)
	if err != nil {
		return err
	}

	nlp.Lock()
	defer nlp.Unlock()

	// 1. lock bucket within a crit. section
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		p.owner.bmd.Unlock()
		return cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
	}
	p.owner.bmd.Unlock()

	// 2. begin
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for result := range results {
		if result.err != nil {
			// 3. abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			return result.err
		}
	}

	// 4. lock & update BMD locally, unlock bucket
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present := clone.Get(bck)
	cmn.Assert(present)
	nprops := bprops.Clone()
	nprops.Mirror.Enabled = copies > 1
	nprops.Mirror.Copies = copies

	clone.set(bck, nprops)
	p.owner.bmd.put(clone)

	// 5. metasync updated BMD; unlock BMD
	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revsPair{clone, msgInt})
	p.owner.bmd.Unlock()

	// 6. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	results = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for result := range results {
		if result.err != nil {
			p.undoUpdateCopies(msg, bck, bprops.Mirror.Copies, bprops.Mirror.Enabled)
			return result.err
		}
	}

	return nil
}

// update-bucket-props transaction: { setprop bucket locally -- begin -- metasync -- commit }
func (p *proxyrunner) setBucketProps(msg *cmn.ActionMsg, bck *cluster.Bck, propsToUpdate cmn.BucketPropsToUpdate) (err error) {
	var (
		c      *txnClientCtx
		nlp    = bck.GetNameLockPair()
		nprops *cmn.BucketProps   // complete version of bucket props containing propsToUpdate changes
		nmsg   = &cmn.ActionMsg{} // with nprops
	)

	nlp.Lock()
	defer nlp.Unlock()

	// 1. lock bucket within a crit. section
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	bprops, present := bmd.Get(bck)
	if !present {
		p.owner.bmd.Unlock()
		return cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
	}
	bck.Props = bprops
	p.owner.bmd.Unlock()

	// 2. begin
	if nprops, err = p.makeNprops(bck, propsToUpdate); err != nil {
		return
	}
	// msg{propsToUpdate} => nmsg{nprops} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = nprops
	c = p.prepTxnClient(nmsg, bck)

	// 2. begin
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for result := range results {
		if result.err != nil {
			// 3. abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			return result.err
		}
	}

	// 4. lock and update BMD locally; unlock bucket
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present = clone.Get(bck)
	cmn.Assert(present)
	bck.Props = bprops
	nprops, err = p.makeNprops(bck, propsToUpdate)
	cmn.AssertNoErr(err)

	clone.set(bck, nprops)
	p.owner.bmd.put(clone)

	// 5. metasync updated BMD; unlock BMD
	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revsPair{clone, msgInt})
	p.owner.bmd.Unlock()

	// 6. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})

	return nil
}

/////////////////////////////
// rollback & misc helpers //
/////////////////////////////

// all in one place
func (p *proxyrunner) prepTxnClient(msg *cmn.ActionMsg, bck *cluster.Bck) *txnClientCtx {
	var (
		c = &txnClientCtx{}
	)
	c.uuid = cmn.GenUUID()
	c.smap = p.owner.smap.get()
	c.msgInt = p.newActionMsgInternal(msg, c.smap, nil) // NOTE: on purpose not including updated BMD (not yet)
	c.body = cmn.MustMarshal(c.msgInt)
	c.path = cmn.URLPath(cmn.Version, cmn.Txn, bck.Name)

	c.query = make(url.Values)
	if bck != nil {
		_ = cmn.AddBckToQuery(c.query, bck.Bck)
	}
	c.query.Set(cmn.URLParamTxnID, c.uuid)
	c.timeout = cmn.GCO.Get().Timeout.MaxKeepalive // TODO -- FIXME: reduce w/caution
	c.query.Set(cmn.URLParamTxnTimeout, cmn.UnixNano2S(int64(c.timeout)))

	c.req = cmn.ReqArgs{Path: cmn.URLPath(c.path, cmn.ActBegin), Query: c.query, Body: c.body}
	return c
}

// rollback create-bucket
func (p *proxyrunner) undoCreateBucket(msg *cmn.ActionMsg, bck *cluster.Bck) {
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	if !clone.del(bck) { // once-in-a-million
		p.owner.bmd.Unlock()
		return
	}
	p.owner.bmd.put(clone)
	p.owner.bmd.Unlock()

	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revsPair{clone, msgInt})
}

// rollback make-n-copies
func (p *proxyrunner) undoUpdateCopies(msg *cmn.ActionMsg, bck *cluster.Bck, copies int64, enabled bool) {
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	nprops, present := clone.Get(bck)
	if !present { // ditto
		p.owner.bmd.Unlock()
		return
	}
	bprops := nprops.Clone()
	bprops.Mirror.Enabled = enabled
	bprops.Mirror.Copies = copies
	clone.set(bck, bprops)
	p.owner.bmd.put(clone)
	p.owner.bmd.Unlock()

	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revsPair{clone, msgInt})
}

func (p *proxyrunner) makeNprops(bck *cluster.Bck, propsToUpdate cmn.BucketPropsToUpdate) (nprops *cmn.BucketProps, err error) {
	var (
		cfg    = cmn.GCO.Get()
		bprops = bck.Props
	)
	nprops = bprops.Clone()
	nprops.Apply(propsToUpdate)
	if bprops.EC.Enabled && nprops.EC.Enabled {
		if !reflect.DeepEqual(bprops.EC, nprops.EC) {
			err = errors.New("once enabled, EC configuration can be only disabled but cannot be changed")
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

	if nprops.Cksum.Type == cmn.PropInherit {
		nprops.Cksum.Type = cfg.Cksum.Type
	}
	targetCnt := p.owner.smap.Get().CountTargets()
	err = nprops.Validate(targetCnt, p.urlOutsideCluster)
	return
}
