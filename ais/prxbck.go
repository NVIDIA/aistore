// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/xact"
)

type bckInitArgs struct {
	w http.ResponseWriter
	r *http.Request

	p *proxy

	bck *cluster.Bck
	msg *apc.ActionMsg

	// URL query: the conventional/slow and
	// the fast alternative tailored exclusively for the datapath
	query url.Values
	dpq   *dpq

	origURLBck string

	reqBody []byte          // request body of original request
	perms   apc.AccessAttrs // apc.AceGET, apc.AcePATCH etc.

	// control flags
	skipBackend    bool // initialize bucket via `bck.InitNoBackend`
	createAIS      bool // create ais bucket on the fly
	dontAddRemote  bool // do not create (ie., add -> BMD) remote bucket on the fly
	dontHeadRemote bool // do not HEAD remote bucket (to find out whether it exists and/or get properties)
	tryHeadRemote  bool // when listing objects anonymously (via ListObjsMsg.Flags LsTryHeadRemote)
	isPresent      bool // the bucket is confirmed to be present (in the cluster's BMD)
	exists         bool // remote bucket is confirmed to be exist
}

////////////////
// ibargsPool //
////////////////

var (
	ibargsPool sync.Pool
	ib0        bckInitArgs
)

func allocInitBckArgs() (a *bckInitArgs) {
	if v := ibargsPool.Get(); v != nil {
		a = v.(*bckInitArgs)
		return
	}
	return &bckInitArgs{}
}

func freeInitBckArgs(a *bckInitArgs) {
	*a = ib0
	ibargsPool.Put(a)
}

//
// lookup and add buckets on the fly
//

// args.init initializes bucket and checks access permissions.
func (args *bckInitArgs) init(bckName string) (bck *cluster.Bck, errCode int, err error) {
	if args.bck == nil {
		args.bck, err = newBckFromQ(bckName, args.query, args.dpq)
		if err != nil {
			errCode = http.StatusBadRequest
			return
		}
	}

	bck = args.bck
	if err = args._checkRemoteBckPermissions(); err != nil {
		errCode = http.StatusBadRequest
		return
	}
	if args.skipBackend {
		err = bck.InitNoBackend(args.p.owner.bmd)
	} else {
		err = bck.Init(args.p.owner.bmd)
	}
	if err != nil {
		errCode = http.StatusBadRequest
		if cmn.IsErrBucketNought(err) {
			errCode = http.StatusNotFound
		}
		return
	}

	args.bck = bck
	args.isPresent = true

	// if permissions are not explicitly specified check the default (msg.Action => permissions)
	if args.perms == 0 && args.msg != nil {
		xactRecord, ok := xact.Table[args.msg.Action]
		if !ok || xactRecord.Access == 0 {
			return
		}
		args.perms = xactRecord.Access
	}
	errCode, err = args.access(bck)
	return
}

func (args *bckInitArgs) _checkRemoteBckPermissions() (err error) {
	var op string
	if !args.bck.IsRemote() {
		return
	}
	if args._requiresPermission(apc.AceMoveBucket) {
		op = "rename/move remote bucket"
		goto retErr
	}
	// HDFS buckets are allowed to be deleted.
	if args.bck.IsHDFS() {
		return
	}
	// HTTP buckets should fail on PUT and bucket rename operations
	if args.bck.IsHTTP() && args._requiresPermission(apc.AcePUT) {
		op = "PUT => HTTP bucket"
		goto retErr
	}
	// Destroy and Rename/Move are not permitted.
	if args.bck.IsCloud() && args._requiresPermission(apc.AceDestroyBucket) && args.msg.Action == apc.ActDestroyBck {
		op = "destroy " + args.bck.Provider + " (cloud) bucket"
		goto retErr
	}
	return
retErr:
	return cmn.NewErrUnsupp(op, args.bck.String())
}

func (args *bckInitArgs) _requiresPermission(perm apc.AccessAttrs) bool {
	return (args.perms & perm) == perm
}

func (args *bckInitArgs) access(bck *cluster.Bck) (errCode int, err error) {
	err = args.p.access(args.r.Header, bck, args.perms)
	errCode = aceErrToCode(err)
	return
}

// initAndTry initializes bucket and then _tries_ to add it if it doesn't exist.
// NOTE: on error the method calls `p.writeErr` - make sure _not_ to do the same in the caller
func (args *bckInitArgs) initAndTry(bucket string) (bck *cluster.Bck, err error) {
	var errCode int
	// 1. init bucket
	bck, errCode, err = args.init(bucket)
	if err == nil {
		return
	}
	if errCode != http.StatusNotFound {
		args.p.writeErr(args.w, args.r, err, errCode)
		return
	}
	// 2. handle two specific errors
	switch {
	case cmn.IsErrBckNotFound(err):
		debug.Assert(bck.IsAIS())
		if !args.createAIS {
			args.p.writeErr(args.w, args.r, err, errCode)
			return
		}
	case cmn.IsErrRemoteBckNotFound(err):
		debug.Assert(bck.IsRemote())
		// when remote-bucket lookup is not permitted
		if cmn.Features.IsSet(feat.DontHeadRemote) || args.dontHeadRemote {
			// - update global
			// - if `feat.DontHeadRemote` is globally enabled you could still use
			// `api.CreateBucket` to override (or disable it via `api.SetClusterConfig`)
			cmn.Features = cmn.GCO.Get().Features
			args.p.writeErrSilent(args.w, args.r, err, errCode)
			return
		}
	default:
		debug.Assertf(false, "%q: unexpected %v(%d)", args.bck, err, errCode)
		args.p.writeErr(args.w, args.r, err, errCode)
		return
	}

	// 3. go ahead to create as bucket OR
	// lookup and then maybe add remote bucket to BMD - on the fly
	bck, err = args.try()
	return
}

func (args *bckInitArgs) try() (bck *cluster.Bck, err error) {
	bck, errCode, err := args._try()
	if err != nil && err != errForwarded {
		if cmn.IsErrBucketAlreadyExists(err) {
			glog.Errorf("%s: %v - race, proceeding anyway...", args.p.si, err)
			err = nil
		} else {
			args.p.writeErr(args.w, args.r, err, errCode)
		}
	}
	return bck, err
}

//
// methods that are internal to this source
//

func (args *bckInitArgs) _try() (bck *cluster.Bck, errCode int, err error) {
	if err = args.bck.Validate(); err != nil {
		errCode = http.StatusBadRequest
		return
	}

	// if HDFS bucket is not present in the BMD there is no point
	// in checking if it exists remotely (in re: `ref_directory`)
	if args.bck.IsHDFS() {
		err = cmn.NewErrBckNotFound(args.bck.Bucket())
		errCode = http.StatusNotFound
		return
	}

	if args.p.forwardCP(args.w, args.r, args.msg, "add-bucket", args.reqBody) {
		err = errForwarded
		return
	}

	// am primary from this point on
	bck = args.bck
	var (
		action    = apc.ActCreateBck
		remoteHdr http.Header
	)
	if backend := bck.Backend(); backend != nil {
		bck = backend
	}
	if bck.IsAIS() {
		glog.Warningf("%s: %q doesn't exist, proceeding to create", args.p.si, args.bck)
		goto creadd
	}

	// lookup remote
	if bck.IsRemote() {
		action = apc.ActAddRemoteBck
		if remoteHdr, errCode, err = args.lookup(bck); err != nil {
			bck = nil
			return
		}
	}
	// validate ht://
	if bck.IsHTTP() {
		if args.origURLBck != "" {
			remoteHdr.Set(apc.HdrOrigURLBck, args.origURLBck)
		} else if origURL := args.getOrigURL(); origURL != "" {
			hbo, err := cmn.NewHTTPObjPath(origURL)
			if err != nil {
				errCode = http.StatusBadRequest
				return bck, errCode, err
			}
			remoteHdr.Set(apc.HdrOrigURLBck, hbo.OrigURLBck)
		} else {
			err = fmt.Errorf("failed to initialize bucket %q: missing HTTP URL", args.bck)
			errCode = http.StatusBadRequest
			return
		}
		debug.Assert(remoteHdr.Get(apc.HdrOrigURLBck) != "")
	}
	// return ok if explicitly asked not to add
	args.exists = true
	if args.dontAddRemote {
		bck.Props = defaultBckProps(bckPropsArgs{bck: bck, hdr: remoteHdr})
		return
	}

creadd: // add/create
	if err = args.p.createBucket(&apc.ActionMsg{Action: action}, bck, remoteHdr); err != nil {
		errCode = crerrStatus(err)
		return
	}
	// init the bucket after having successfully added it to the BMD
	if err = bck.Init(args.p.owner.bmd); err != nil {
		errCode = http.StatusInternalServerError
		err = cmn.NewErrFailedTo(args.p, "add-remote", bck, err, errCode)
	}
	bck = args.bck
	return
}

func (args *bckInitArgs) getOrigURL() (ourl string) {
	if args.query != nil {
		debug.Assert(args.dpq == nil)
		ourl = args.query.Get(apc.QparamOrigURL)
	} else {
		ourl = args.dpq.origURL
	}
	return
}

func (args *bckInitArgs) lookup(bck *cluster.Bck) (hdr http.Header, code int, err error) {
	q := url.Values{}
	if bck.IsHTTP() {
		origURL := args.getOrigURL()
		q.Set(apc.QparamOrigURL, origURL)
	}
	if args.tryHeadRemote {
		q.Set(apc.QparamSilent, "true")
	}
	hdr, code, err = args.p.headRemoteBck(bck.Bucket(), q)
	if (code == http.StatusUnauthorized || code == http.StatusForbidden) && args.tryHeadRemote {
		if args.dontAddRemote {
			return
		}
		// NOTE: assuming OK
		glog.Warningf("proceeding to add cloud bucket %s to the BMD after having: %v(%d)", bck, err, code)
		glog.Warningf("%s properties: using all cluster defaults", bck)
		hdr = make(http.Header, 2)
		hdr.Set(apc.HdrBackendProvider, bck.Provider)
		hdr.Set(apc.HdrBucketVerEnabled, "false")
		err = nil
	}
	return
}
