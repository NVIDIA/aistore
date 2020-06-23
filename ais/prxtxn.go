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
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnServerCtx & prepTxnServer)
type txnClientCtx struct {
	uuid    string
	smap    *smapX
	msg     *aisMsg
	body    []byte
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

// create-bucket: { check non-existence -- begin -- create locally -- metasync -- commit }
func (p *proxyrunner) createBucket(msg *cmn.ActionMsg, bck *cluster.Bck, cloudHeader ...http.Header) error {
	var (
		bucketProps = cmn.DefaultBucketProps()
		nlp         = bck.GetNameLockPair()
	)

	if bck.Props != nil {
		bucketProps = bck.Props
	} else if len(cloudHeader) != 0 {
		bucketProps = cmn.CloudBucketProps(cloudHeader[0])
	}
	nlp.Lock()
	defer nlp.Unlock()

	// 1. try add
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); present {
		p.owner.bmd.Unlock()
		return cmn.NewErrorBucketAlreadyExists(bck.Bck, p.si.String())
	}
	p.owner.bmd.Unlock()

	// 3. begin
	var (
		c = p.prepTxnClient(msg, bck)
	)
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			return res.err
		}
	}

	// 3. lock & update BMD locally
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	added := clone.add(bck, bucketProps)
	cmn.Assert(added)
	p.owner.bmd.put(clone)

	// 4. metasync updated BMD & unlock BMD
	c.msg.BMDVersion = clone.version()
	wg := p.metasyncer.sync(revsPair{clone, c.msg})
	p.owner.bmd.Unlock()

	wg.Wait() // to synchronize prior to committing

	// 5. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	c.timeout = cmn.GCO.Get().Timeout.MaxKeepalive // making exception for this critical op
	c.req.Query.Set(cmn.URLParamTxnTimeout, cmn.UnixNano2S(int64(c.timeout)))
	results = p.bcastPost(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err) // commit must go thru
			p.undoCreateBucket(msg, bck)
			return res.err
		}
	}
	return nil
}

// destroy AIS bucket or evict Cloud bucket
func (p *proxyrunner) destroyBucket(msg *cmn.ActionMsg, bck *cluster.Bck) error {
	nlp := bck.GetNameLockPair()

	// TODO: try-lock
	nlp.Lock()
	defer nlp.Unlock()

	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()

	if _, present := bmd.Get(bck); !present {
		p.owner.bmd.Unlock()
		return cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
	}

	clone := bmd.clone()
	deled := clone.del(bck)
	cmn.Assert(deled)
	p.owner.bmd.put(clone)

	wg := p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})
	p.owner.bmd.Unlock()

	wg.Wait()
	return nil
}

// make-n-copies: { confirm existence -- begin -- update locally -- metasync -- commit }
func (p *proxyrunner) makeNCopies(msg *cmn.ActionMsg, bck *cluster.Bck) error {
	var (
		pname       = p.si.String()
		c           = p.prepTxnClient(msg, bck)
		nlp         = bck.GetNameLockPair()
		copies, err = p.parseNCopies(msg.Value)
	)
	if err != nil {
		return err
	}
	if !nlp.TryLock() {
		return cmn.NewErrorBucketIsBusy(bck.Bck, pname)
	}
	defer nlp.Unlock()

	// 1. confirm existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		p.owner.bmd.Unlock()
		return cmn.NewErrorBucketDoesNotExist(bck.Bck, pname)
	}
	p.owner.bmd.Unlock()

	// 2. begin
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			return res.err
		}
	}

	// 3. lock & update BMD locally
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present := clone.Get(bck)
	cmn.Assert(present)
	nprops := bprops.Clone()
	nprops.Mirror.Enabled = copies > 1
	nprops.Mirror.Copies = copies

	clone.set(bck, nprops)
	p.owner.bmd.put(clone)

	// 4. metasync updated BMD; unlock BMD
	c.msg.BMDVersion = clone.version()
	wg := p.metasyncer.sync(revsPair{clone, c.msg})
	p.owner.bmd.Unlock()

	wg.Wait()

	// 5. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	results = p.bcastPost(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err) // commit must go thru
			p.undoUpdateCopies(msg, bck, bprops.Mirror.Copies, bprops.Mirror.Enabled)
			return res.err
		}
	}

	return nil
}

// set-bucket-props: { confirm existence -- begin -- apply props -- metasync -- commit }
func (p *proxyrunner) setBucketProps(msg *cmn.ActionMsg, bck *cluster.Bck,
	propsToUpdate cmn.BucketPropsToUpdate) (err error) {
	var (
		c      *txnClientCtx
		nlp    = bck.GetNameLockPair()
		nprops *cmn.BucketProps   // complete version of bucket props containing propsToUpdate changes
		nmsg   = &cmn.ActionMsg{} // with nprops
	)

	// TODO: try-lock
	nlp.Lock()
	defer nlp.Unlock()

	// 1. confirm existence
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
	switch msg.Action {
	case cmn.ActSetBprops:
		if nprops, err = p.makeNprops(bck, propsToUpdate); err != nil {
			return
		}
	case cmn.ActResetBprops:
		nprops = cmn.DefaultBucketProps()
	default:
		cmn.Assert(false)
	}
	// msg{propsToUpdate} => nmsg{nprops} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = nprops
	c = p.prepTxnClient(nmsg, bck)

	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			return res.err
		}
	}

	// 3. lock and update BMD locally
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present = clone.Get(bck)
	cmn.Assert(present)
	if msg.Action == cmn.ActSetBprops {
		bck.Props = bprops
		nprops, err = p.makeNprops(bck, propsToUpdate)
		cmn.AssertNoErr(err)
	}
	clone.set(bck, nprops)
	p.owner.bmd.put(clone)

	// 4. metasync updated BMD; unlock BMD
	c.msg.BMDVersion = clone.version()
	wg := p.metasyncer.sync(revsPair{clone, c.msg})
	p.owner.bmd.Unlock()

	wg.Wait()

	// 5. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})

	return nil
}

// rename-bucket: { confirm existence -- begin -- RebID -- metasync -- commit -- wait for rebalance and unlock }
func (p *proxyrunner) renameBucket(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (err error) {
	var (
		c       *txnClientCtx
		nlpFrom = bckFrom.GetNameLockPair()
		nlpTo   = bckTo.GetNameLockPair()
		nmsg    = &cmn.ActionMsg{} // + bckTo
		pname   = p.si.String()
	)
	if err := p.canStartRebalance(); err != nil {
		return fmt.Errorf("bucket cannot be renamed: %w", err)
	}
	if !nlpFrom.TryLock() {
		return cmn.NewErrorBucketIsBusy(bckFrom.Bck, pname)
	}
	if !nlpTo.TryLock() {
		nlpFrom.Unlock()
		return cmn.NewErrorBucketIsBusy(bckTo.Bck, pname)
	}
	unlock := func() { nlpTo.Unlock(); nlpFrom.Unlock() }

	// 1. confirm existence & non-existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		p.owner.bmd.Unlock()
		unlock()
		return cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, pname)
	}
	if _, present := bmd.Get(bckTo); present {
		p.owner.bmd.Unlock()
		unlock()
		return cmn.NewErrorBucketAlreadyExists(bckTo.Bck, pname)
	}
	p.owner.bmd.Unlock()

	// msg{} => nmsg{bckTo} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = bckTo.Bck
	c = p.prepTxnClient(nmsg, bckFrom)

	// 2. begin
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			unlock()
			return res.err
		}
	}

	// 3. lock and update BMD locally
	p.owner.bmd.Lock()
	cloneBMD := p.owner.bmd.get().clone()
	bprops, present := cloneBMD.Get(bckFrom)
	cmn.Assert(present)

	bckFrom.Props = bprops.Clone()
	bckTo.Props = bprops.Clone()

	added := cloneBMD.add(bckTo, bckTo.Props)
	cmn.Assert(added)
	bckFrom.Props.Renamed = cmn.ActRenameLB
	cloneBMD.set(bckFrom, bckFrom.Props)

	p.owner.bmd.put(cloneBMD)

	// 4. metasync updated BMD; unlock BMD
	c.msg.BMDVersion = cloneBMD.version()
	wg := p.metasyncer.sync(revsPair{cloneBMD, c.msg})
	p.owner.bmd.Unlock()

	wg.Wait()

	_ = p.owner.rmd.modify(
		func(clone *rebMD) {
			clone.inc()
			clone.Resilver = true
		},
		func(clone *rebMD) {
			c.msg.RMDVersion = clone.version()

			// 5. commit
			c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
			c.body = cmn.MustMarshal(c.msg)
			c.req.Body = c.body

			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})

			// 6. start waiting for `finished` notifications
			nl := notifListenerFromTo{
				notifListenerBase: notifListenerBase{srcs: c.smap.Tmap.Clone(), f: p.nlBckRen},
				nlpFrom:           &nlpFrom,
				nlpTo:             &nlpTo,
			}
			rebUUID := strconv.FormatInt(clone.version(), 10)
			p.notifs.add(rebUUID, &nl)

			// 7. start rebalance and resilver
			wg = p.metasyncer.sync(revsPair{clone, c.msg})
		},
	)
	wg.Wait()
	return
}

// copy-bucket: { confirm existence -- begin -- conditional metasync -- start waiting for copy-done -- commit }
func (p *proxyrunner) copyBucket(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (err error) {
	var (
		c       *txnClientCtx
		nmsg    = &cmn.ActionMsg{} // + bckTo
		nlpFrom = bckFrom.GetNameLockPair()
		nlpTo   = bckTo.GetNameLockPair()
		pname   = p.si.String()
	)
	if !nlpFrom.TryRLock() {
		return cmn.NewErrorBucketIsBusy(bckFrom.Bck, pname)
	}
	if !nlpTo.TryLock() {
		nlpFrom.RUnlock()
		return cmn.NewErrorBucketIsBusy(bckTo.Bck, pname)
	}
	unlock := func() { nlpTo.Unlock(); nlpFrom.RUnlock() }

	// 1. confirm existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		p.owner.bmd.Unlock()
		unlock()
		return cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, pname)
	}
	p.owner.bmd.Unlock()

	// msg{} => nmsg{bckTo} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = bckTo.Bck
	c = p.prepTxnClient(nmsg, bckFrom)

	// 2. begin
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			unlock()
			return res.err
		}
	}

	// 3. lock and update BMD locally
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present := clone.Get(bckFrom)
	cmn.Assert(present)

	event := txnCommitEventNone

	// create destination bucket but only if it doesn't exist
	if _, present = clone.Get(bckTo); !present {
		bckFrom.Props = bprops.Clone()
		bckTo.Props = bprops.Clone()
		added := clone.add(bckTo, bckTo.Props)
		cmn.Assert(added)
		p.owner.bmd.put(clone)

		// 4. metasync updated BMD; unlock BMD
		c.msg.BMDVersion = clone.version()
		wg := p.metasyncer.sync(revsPair{clone, c.msg})
		p.owner.bmd.Unlock()

		wg.Wait()

		event = txnCommitEventMetasync
	} else {
		p.owner.bmd.Unlock()
	}

	// 5. start waiting for `finished` notifications
	nl := notifListenerFromTo{
		notifListenerBase: notifListenerBase{srcs: c.smap.Tmap.Clone(), f: p.nlBckCp},
		nlpFrom:           &nlpFrom,
		nlpTo:             &nlpTo,
	}
	p.notifs.add(c.uuid, &nl)

	// 6. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	c.req.Query.Set(cmn.URLParamTxnEvent, event)
	_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})

	return
}

// ec-encode: { confirm existence -- begin -- update locally -- metasync -- commit }
func (p *proxyrunner) ecEncode(bck *cluster.Bck, msg *cmn.ActionMsg) error {
	var (
		pname = p.si.String()
		c     = p.prepTxnClient(msg, bck)
		nlp   = bck.GetNameLockPair()
	)
	if !nlp.TryLock() {
		return cmn.NewErrorBucketIsBusy(bck.Bck, pname)
	}
	defer nlp.Unlock()

	// 1. confirm existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		p.owner.bmd.Unlock()
		return cmn.NewErrorBucketDoesNotExist(bck.Bck, pname)
	}
	p.owner.bmd.Unlock()

	// 2. begin
	results := p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastPost(bcastArgs{req: c.req, smap: c.smap})
			return res.err
		}
	}

	// TODO: BMD must be already (and separately) updated - see "add reEC" comment
	config := cmn.GCO.Get()

	// 3. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	results = p.bcastPost(bcastArgs{req: c.req, smap: c.smap, timeout: config.Timeout.CplaneOperation})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err)
			return res.err
		}
	}
	return nil
}

/////////////////////////////
// rollback & misc helpers //
/////////////////////////////

// txn client context
func (p *proxyrunner) prepTxnClient(msg *cmn.ActionMsg, bck *cluster.Bck) *txnClientCtx {
	var (
		query = make(url.Values)
		c     = &txnClientCtx{}
	)
	c.uuid = cmn.GenUUID()
	c.smap = p.owner.smap.get()

	c.msg = p.newAisMsg(msg, c.smap, nil, c.uuid)
	c.body = cmn.MustMarshal(c.msg)

	c.path = cmn.URLPath(cmn.Version, cmn.Txn, bck.Name)
	if bck != nil {
		_ = cmn.AddBckToQuery(query, bck.Bck)
	}
	c.timeout = cmn.GCO.Get().Timeout.CplaneOperation
	query.Set(cmn.URLParamTxnTimeout, cmn.UnixNano2S(int64(c.timeout)))

	c.req = cmn.ReqArgs{Path: cmn.URLPath(c.path, cmn.ActBegin), Query: query, Body: c.body}
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

	_ = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})

	p.owner.bmd.Unlock()
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

	_ = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})

	p.owner.bmd.Unlock()
}

func (p *proxyrunner) makeNprops(bck *cluster.Bck, propsToUpdate cmn.BucketPropsToUpdate) (nprops *cmn.BucketProps, err error) {
	const ers = "once enabled, EC configuration can be only disabled but cannot be changed"
	var (
		cfg    = cmn.GCO.Get()
		bprops = bck.Props
	)
	nprops = bprops.Clone()
	nprops.Apply(propsToUpdate)
	if bprops.EC.Enabled && nprops.EC.Enabled {
		if !reflect.DeepEqual(bprops.EC, nprops.EC) {
			err = errors.New(ers)
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

	targetCnt := p.owner.smap.Get().CountTargets()
	err = nprops.Validate(targetCnt)
	return
}

//
// notifications
//

// copy-bucket done
func (p *proxyrunner) nlBckCp(n notifListener, msg interface{}, uuid string, err error) {
	nl := n.(*notifListenerFromTo)
	nl.nlpTo.Unlock()
	nl.nlpFrom.RUnlock()
	if err != nil {
		glog.Errorf("%s(%q): failed to copy bucket: %+v, err: %v", p.si, uuid, msg, err)
	} else if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s(%q): %+v", p.si, uuid, msg)
	}
}

// rename-bucket done
func (p *proxyrunner) nlBckRen(n notifListener, msg interface{}, uuid string, err error) {
	nl := n.(*notifListenerFromTo)
	nl.nlpTo.Unlock()
	nl.nlpFrom.Unlock()
	if err != nil {
		glog.Errorf("%s(%q): failed to rename bucket: %+v, err: %v", p.si, uuid, msg, err)
	} else if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s(%q): %+v", p.si, uuid, msg)
	}
}
