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
	jsoniter "github.com/json-iterator/go"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnServerCtx & prepTxnServer)
type txnClientCtx struct {
	uuid    string
	smap    *smapX
	msg     *aisMsg
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
		c       = p.prepTxnClient(msg, bck)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
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
	results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
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
func (p *proxyrunner) makeNCopies(msg *cmn.ActionMsg, bck *cluster.Bck) (xactID string, err error) {
	copies, err := p.parseNCopies(msg.Value)
	if err != nil {
		return
	}
	// 1. confirm existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		p.owner.bmd.Unlock()
		err = cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
		return
	}
	p.owner.bmd.Unlock()

	// 2. begin
	var (
		c       = p.prepTxnClient(msg, bck)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
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

	// 5. IC
	nl := newNLB(c.uuid, c.smap, notifXact, msg.Action, bck.Bck)
	nl.setOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	for res := range results {
		if res.err != nil {
			glog.Error(res.err) // commit must go thru
			p.undoUpdateCopies(msg, bck, bprops.Mirror.Copies, bprops.Mirror.Enabled)
			err = res.err
			return
		}
	}
	xactID = c.uuid
	return
}

// set-bucket-props: { confirm existence -- begin -- apply props -- metasync -- commit }
func (p *proxyrunner) setBucketProps(msg *cmn.ActionMsg, bck *cluster.Bck,
	propsToUpdate cmn.BucketPropsToUpdate) (xactID string, err error) {
	var (
		nprops *cmn.BucketProps   // complete version of bucket props containing propsToUpdate changes
		nmsg   = &cmn.ActionMsg{} // with nprops
	)
	// 1. confirm existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	bprops, present := bmd.Get(bck)
	if !present {
		p.owner.bmd.Unlock()
		err = cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
		return
	}
	bck.Props = bprops
	p.owner.bmd.Unlock()

	// 2. begin
	switch msg.Action {
	case cmn.ActSetBprops:
		// make and validate new props
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
	var (
		c       = p.prepTxnClient(nmsg, bck)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. lock and update BMD locally
	var remirror, reec bool
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present = clone.Get(bck)
	cmn.Assert(present)
	if msg.Action == cmn.ActSetBprops {
		bck.Props = bprops
		nprops, err = p.makeNprops(bck, propsToUpdate) // under lock
		if err != nil {
			p.owner.bmd.Unlock()
			return
		}
	}

	remirror = reMirror(bprops, nprops)
	reec = reEC(bprops, nprops, bck)
	clone.set(bck, nprops)
	p.owner.bmd.put(clone)

	// 4. metasync updated BMD; unlock BMD
	c.msg.BMDVersion = clone.version()
	wg := p.metasyncer.sync(revsPair{clone, c.msg})
	p.owner.bmd.Unlock()

	wg.Wait()

	// 5. if remirror|re-EC|TBD-storage-svc
	if remirror || reec {
		nl := newNLB(c.uuid, c.smap, notifXact, msg.Action, bck.Bck)
		nl.setOwner(equalIC)
		p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})
		xactID = c.uuid
	}

	// 6. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	return
}

// rename-bucket: { confirm existence -- begin -- RebID -- metasync -- commit -- wait for rebalance and unlock }
func (p *proxyrunner) renameBucket(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (xactID string, err error) {
	var (
		nmsg = &cmn.ActionMsg{} // + bckTo
	)
	if rebErr := p.canStartRebalance(); rebErr != nil {
		err = fmt.Errorf("%s: bucket cannot be renamed: %w", p.si, rebErr)
		return
	}
	// 1. confirm existence & non-existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		p.owner.bmd.Unlock()
		err = cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, p.si.String())
		return
	}
	if _, present := bmd.Get(bckTo); present {
		p.owner.bmd.Unlock()
		err = cmn.NewErrorBucketAlreadyExists(bckTo.Bck, p.si.String())
		return
	}
	p.owner.bmd.Unlock()

	// msg{} => nmsg{bckTo} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = bckTo.Bck

	// 2. begin
	var (
		c       = p.prepTxnClient(nmsg, bckFrom)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
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

			// 5. IC
			nl := newNLB(c.uuid, c.smap, notifXact, msg.Action, bckFrom.Bck, bckTo.Bck)
			nl.setOwner(equalIC)
			p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

			// 6. commit
			xactID = c.uuid
			c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
			c.req.Body = cmn.MustMarshal(c.msg)

			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})

			// 7. start rebalance and resilver
			wg = p.metasyncer.sync(revsPair{clone, c.msg})
		},
	)
	wg.Wait()
	return
}

// copy-bucket: { confirm existence -- begin -- conditional metasync -- start waiting for copy-done -- commit }
func (p *proxyrunner) copyBucket(bckFrom, bckTo *cluster.Bck, msg *cmn.ActionMsg) (xactID string, err error) {
	var (
		nmsg = &cmn.ActionMsg{} // + bckTo
	)
	// 1. confirm existence
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		p.owner.bmd.Unlock()
		err = cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, p.si.String())
		return
	}
	p.owner.bmd.Unlock()

	// msg{} => nmsg{bckTo} and prep context(nmsg)
	*nmsg = *msg
	nmsg.Value = bckTo.Bck

	// 2. begin
	var (
		c       = p.prepTxnClient(nmsg, bckFrom)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. lock and update BMD locally
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present := clone.Get(bckFrom)
	cmn.Assert(present)

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

		c.req.Query.Set(cmn.URLParamWaitMetasync, "true")
	} else {
		p.owner.bmd.Unlock()
	}

	// 5. IC
	nl := newNLB(c.uuid, c.smap, notifXact, msg.Action, bckFrom.Bck, bckTo.Bck)
	nl.setOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	xactID = c.uuid
	return
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
		c          = p.prepTxnClient(msg, bck)
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
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	props, present := bmd.Get(bck)
	if !present {
		p.owner.bmd.Unlock()
		err = cmn.NewErrorBucketDoesNotExist(bck.Bck, pname)
		return
	}
	if props.EC.Enabled {
		// Changing data or parity slice count on the fly is unsupported yet
		p.owner.bmd.Unlock()
		err = fmt.Errorf("%s: EC is already enabled for bucket %s", p.si, bck)
		return
	}
	p.owner.bmd.Unlock()

	// 2. begin
	results := p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.URLPath(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. lock & update BMD locally
	p.owner.bmd.Lock()
	clone := p.owner.bmd.get().clone()
	bprops, present := clone.Get(bck)
	cmn.Assert(present)
	nprops := bprops.Clone()
	nprops.EC.Enabled = true
	nprops.EC.DataSlices = *ecConf.DataSlices
	nprops.EC.ParitySlices = *ecConf.ParitySlices

	clone.set(bck, nprops)
	p.owner.bmd.put(clone)

	// 4. metasync updated BMD; unlock BMD
	c.msg.BMDVersion = clone.version()
	wg := p.metasyncer.sync(revsPair{clone, c.msg})
	p.owner.bmd.Unlock()

	wg.Wait()

	// 5. IC
	nl := newNLB(c.uuid, c.smap, notifXact, msg.Action, bck.Bck)
	nl.setOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	unlockUpon = true
	config := cmn.GCO.Get()
	c.req.Path = cmn.URLPath(c.path, cmn.ActCommit)
	results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: config.Timeout.CplaneOperation})
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
	body := cmn.MustMarshal(c.msg)

	c.path = cmn.URLPath(cmn.Version, cmn.Txn, bck.Name)
	if bck != nil {
		_ = cmn.AddBckToQuery(query, bck.Bck)
	}
	c.timeout = cmn.GCO.Get().Timeout.CplaneOperation
	query.Set(cmn.URLParamTxnTimeout, cmn.UnixNano2S(int64(c.timeout)))

	c.req = cmn.ReqArgs{Method: http.MethodPost, Path: cmn.URLPath(c.path, cmn.ActBegin), Query: query, Body: body}
	return c
}

// rollback create-bucket
func (p *proxyrunner) undoCreateBucket(msg *cmn.ActionMsg, bck *cluster.Bck) {
	p.owner.bmd.Lock()
	defer p.owner.bmd.Unlock()

	clone := p.owner.bmd.get().clone()
	if !clone.del(bck) { // once-in-a-million
		return
	}
	p.owner.bmd.put(clone)

	_ = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})
}

// rollback make-n-copies
func (p *proxyrunner) undoUpdateCopies(msg *cmn.ActionMsg, bck *cluster.Bck, copies int64, enabled bool) {
	p.owner.bmd.Lock()
	defer p.owner.bmd.Unlock()

	clone := p.owner.bmd.get().clone()
	nprops, present := clone.Get(bck)
	if !present { // ditto
		return
	}
	bprops := nprops.Clone()
	bprops.Mirror.Enabled = enabled
	bprops.Mirror.Copies = copies
	clone.set(bck, bprops)
	p.owner.bmd.put(clone)

	_ = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})
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
	if bprops.EC.Enabled && nprops.EC.Enabled {
		if !reflect.DeepEqual(bprops.EC, nprops.EC) {
			err = fmt.Errorf("%s: once enabled, EC configuration can be only disabled but cannot change",
				p.si)
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

	targetCnt := p.owner.smap.Get().CountTargets()
	err = nprops.Validate(targetCnt)
	return
}
