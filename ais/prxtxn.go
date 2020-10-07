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
	"sync"
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
		timeout time.Duration
		req     cmn.ReqArgs
	}
	backendDoesNotExistErr struct {
		backend *cluster.Bck
		err     error
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

	// 3. begin
	var (
		c       = p.prepTxnClient(msg, bck)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	)
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			return res.err
		}
	}

	// 3. update BMD locally
	var wg *sync.WaitGroup
	_ = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		added := clone.add(bck, bucketProps) // TODO: Bucket could be added during begin.
		cmn.Assert(added)
		return true, nil
	}, func(clone *bucketMD) {
		// 4. metasync updated BMD
		c.msg.BMDVersion = clone.version()
		wg = p.metasyncer.sync(revsPair{clone, c.msg})
	})

	wg.Wait() // to synchronize prior to committing

	// 5. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
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

	var wg *sync.WaitGroup
	err := p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		if _, present := clone.Get(bck); !present {
			return false, cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
		}
		deleted := clone.del(bck)
		cmn.Assert(deleted)
		return true, nil
	}, func(clone *bucketMD) {
		wg = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})
	})
	if err != nil {
		return err
	}
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
	bmd := p.owner.bmd.get()
	if _, present := bmd.Get(bck); !present {
		err = cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
		return
	}

	// 2. begin
	var (
		c       = p.prepTxnClient(msg, bck)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
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

	// 3. update BMD locally
	var (
		wg      *sync.WaitGroup
		present bool
		bprops  *cmn.BucketProps
	)
	_ = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
		cmn.Assert(present)
		nprops := bprops.Clone()
		nprops.Mirror.Enabled = copies > 1
		nprops.Mirror.Copies = copies

		clone.set(bck, nprops)
		return true, nil
	}, func(clone *bucketMD) {
		// 4. metasync updated BMD
		c.msg.BMDVersion = clone.version()
		wg = p.metasyncer.sync(revsPair{clone, c.msg})
	})
	wg.Wait()

	// 5. IC
	nl := xaction.NewXactNL(c.uuid, &c.smap.Smap, c.smap.Tmap.Clone(), msg.Action, bck.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
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
		if err = p.checkBackendBck(bck, nprops); err != nil {
			return
		}
	case cmn.ActResetBprops:
		if bck.IsCloud() {
			if bck.HasBackendBck() {
				err = fmt.Errorf("%q has backend %q - detach it prior to resetting the props",
					bck.Bck, bck.BackendBck())
				return
			}
			cloudProps, err, _ := p.headCloudBck(bck.Bck, nil)
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
		c       = p.prepTxnClient(nmsg, bck)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
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

	// 3. update BMD locally
	var (
		wg           *sync.WaitGroup
		needReMirror bool
		needReEC     bool
	)
	err = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		bprops, present = clone.Get(bck) // TODO: Bucket could be deleted during begin.
		cmn.Assert(present)

		if msg.Action == cmn.ActSetBprops {
			bck.Props = bprops
			nprops, err = p.makeNprops(bck, propsToUpdate)
			if err != nil {
				return false, err
			}
			if err := p.checkBackendBck(bck, nprops); err != nil {
				return false, err
			}
		}

		needReMirror = reMirror(bprops, nprops)
		needReEC = reEC(bprops, nprops, bck)
		clone.set(bck, nprops)
		return true, nil
	}, func(clone *bucketMD) {
		// 4. metasync updated BMD
		c.msg.BMDVersion = clone.version()
		wg = p.metasyncer.sync(revsPair{clone, c.msg})
	})
	if err != nil {
		return "", err
	}
	wg.Wait()

	// 5. if remirror|re-EC|TBD-storage-svc
	if needReMirror || needReEC {
		action := cmn.ActMakeNCopies
		if needReEC {
			action = cmn.ActECEncode
		}
		nl := xaction.NewXactNL(c.uuid, &c.smap.Smap, c.smap.Tmap.Clone(), action, bck.Bck)
		nl.SetOwner(equalIC)
		p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})
		xactID = c.uuid
	}

	// 6. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
	_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})
	return
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
		c       = p.prepTxnClient(nmsg, bckFrom)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
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

	// 3. update BMD locally
	var wg *sync.WaitGroup
	_ = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		bprops, present := clone.Get(bckFrom) // TODO: Bucket could be deleted during begin.
		cmn.Assert(present)

		bckFrom.Props = bprops.Clone()
		bckTo.Props = bprops.Clone()

		added := clone.add(bckTo, bckTo.Props)
		cmn.Assert(added)
		bckFrom.Props.Renamed = cmn.ActRenameLB
		clone.set(bckFrom, bckFrom.Props)
		return true, nil
	}, func(clone *bucketMD) {
		// 4. metasync updated BMD
		c.msg.BMDVersion = clone.version()
		wg = p.metasyncer.sync(revsPair{clone, c.msg})
	})
	wg.Wait()

	_ = p.owner.rmd.modify(
		func(clone *rebMD) {
			clone.inc()
			clone.Resilver = true
		},
		func(clone *rebMD) {
			c.msg.RMDVersion = clone.version()

			// 5. IC
			nl := xaction.NewXactNL(c.uuid, &c.smap.Smap, c.smap.Tmap.Clone(),
				msg.Action, bckFrom.Bck, bckTo.Bck)
			nl.SetOwner(equalIC)
			p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

			// 6. commit
			xactID = c.uuid
			c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
			c.req.Body = cmn.MustMarshal(c.msg)

			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap, timeout: cmn.LongTimeout})

			// 7. start rebalance and resilver
			wg = p.metasyncer.sync(revsPair{clone, c.msg})
			nl = xaction.NewXactNL(xaction.RebID(clone.Version).String(), &c.smap.Smap,
				c.smap.Tmap.Clone(), cmn.ActRebalance)
			nl.SetOwner(equalIC)
			p.ic.registerEqual(regIC{smap: c.smap, nl: nl})
		},
	)
	wg.Wait()
	return
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
		c       = p.prepTxnClient(msg, bckFrom)
		results = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
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

	// 3. update BMD locally
	var wg *sync.WaitGroup
	_ = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		bprops, present := clone.Get(bckFrom) // TODO: Bucket could be removed during begin.
		cmn.Assert(present)

		// Skip destination bucket creation if it's dry run or it's already present.
		if _, present = clone.Get(bckTo); dryRun || present {
			return false, nil
		}

		cmn.Assert(!bckTo.IsRemote())
		bckFrom.Props = bprops.Clone()
		bckTo.Props = bprops.Clone()
		added := clone.add(bckTo, bckTo.Props)
		cmn.Assert(added)
		return true, nil
	}, func(clone *bucketMD) {
		// 4. metasync updated BMD
		c.msg.BMDVersion = clone.version()
		wg = p.metasyncer.sync(revsPair{clone, c.msg})
	})
	if wg != nil {
		wg.Wait()

		c.req.Query.Set(cmn.URLParamWaitMetasync, "true")
	}

	// 5. IC
	nl := xaction.NewXactNL(c.uuid, &c.smap.Smap, c.smap.Tmap.Clone(), msg.Action, bckFrom.Bck, bckTo.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
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
	results := p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
	for res := range results {
		if res.err != nil {
			// abort
			c.req.Path = cmn.JoinWords(c.path, cmn.ActAbort)
			_ = p.bcastToGroup(bcastArgs{req: c.req, smap: c.smap})
			err = res.err
			return
		}
	}

	// 3. update BMD locally
	var wg *sync.WaitGroup
	_ = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		bprops, present := clone.Get(bck) // TODO: Bucket could be deleted during begin.
		cmn.Assert(present)
		nprops := bprops.Clone()
		nprops.EC.Enabled = true
		nprops.EC.DataSlices = *ecConf.DataSlices
		nprops.EC.ParitySlices = *ecConf.ParitySlices

		clone.set(bck, nprops)
		return true, nil
	}, func(clone *bucketMD) {
		// 4. metasync updated BMD
		c.msg.BMDVersion = clone.version()
		wg = p.metasyncer.sync(revsPair{clone, c.msg})
	})
	wg.Wait()

	// 5. IC
	nl := xaction.NewXactNL(c.uuid, &c.smap.Smap, c.smap.Tmap.Clone(), msg.Action, bck.Bck)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: c.smap, query: c.req.Query})

	// 6. commit
	unlockUpon = true
	config := cmn.GCO.Get()
	c.req.Path = cmn.JoinWords(c.path, cmn.ActCommit)
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
	c := &txnClientCtx{}
	c.uuid = cmn.GenUUID()
	c.smap = p.owner.smap.get()

	c.msg = p.newAisMsg(msg, c.smap, nil, c.uuid)
	body := cmn.MustMarshal(c.msg)

	c.path = cmn.JoinWords(cmn.Version, cmn.Txn, bck.Name)
	c.timeout = cmn.GCO.Get().Timeout.CplaneOperation

	query := make(url.Values, 2)
	query = cmn.AddBckToQuery(query, bck.Bck)
	query.Set(cmn.URLParamTxnTimeout, cmn.UnixNano2S(int64(c.timeout)))

	c.req = cmn.ReqArgs{Method: http.MethodPost, Path: cmn.JoinWords(c.path, cmn.ActBegin), Query: query, Body: body}
	return c
}

// rollback create-bucket
func (p *proxyrunner) undoCreateBucket(msg *cmn.ActionMsg, bck *cluster.Bck) {
	_ = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		if !clone.del(bck) {
			return false, nil
		}
		p.owner.bmd.put(clone)
		return true, nil
	}, func(clone *bucketMD) {
		_ = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})
	})
}

// rollback make-n-copies
func (p *proxyrunner) undoUpdateCopies(msg *cmn.ActionMsg, bck *cluster.Bck, copies int64, enabled bool) {
	_ = p.owner.bmd.modify(func(clone *bucketMD) (bool, error) {
		nprops, present := clone.Get(bck)
		if !present {
			return false, nil
		}
		bprops := nprops.Clone()
		bprops.Mirror.Enabled = enabled
		bprops.Mirror.Copies = copies
		clone.set(bck, bprops)
		return true, nil
	}, func(clone *bucketMD) {
		_ = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, clone)})
	})
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

	targetCnt := p.owner.smap.Get().CountTargets()
	err = nprops.Validate(targetCnt)
	return
}

func _versioning(v bool) string {
	if v {
		return "enabled"
	}
	return "disabled"
}

func (p *proxyrunner) checkBackendBck(bck *cluster.Bck, nprops *cmn.BucketProps) (err error) {
	if nprops.BackendBck.IsEmpty() {
		return
	}
	backend := cluster.NewBckEmbed(nprops.BackendBck)
	if err = backend.InitNoBackend(p.owner.bmd, p.si); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			err = &backendDoesNotExistErr{
				backend,
				fmt.Errorf("failed to initialize backend %s for bucket %s", backend, bck),
			}
		}
		return
	}
	// NOTE: backend versioning override
	nprops.Versioning.Enabled = backend.Props.Versioning.Enabled
	return
}

func (e *backendDoesNotExistErr) Error() string { return e.err.Error() }
