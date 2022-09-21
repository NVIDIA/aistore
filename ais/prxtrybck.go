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

	// flags
	fltPresence int  // (enum apc.Flt*)
	skipBackend bool // initialize bucket via `bck.InitNoBackend`
	createAIS   bool // create ais bucket on the fly
	noHeadRemB  bool // do not handle ErrRemoteBckNotFound by adding remote bucket on the fly
	tryHeadRemB bool // when listing objects anonymously (via ListObjsMsg.Flags LsTryHeadRemB)
	isPresent   bool // the bucket is confirmed to be present (in the cluster's BMD)
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
	bck, errCode, err = args.init(bucket)
	if err == nil {
		return
	}
	if errCode != http.StatusNotFound {
		args.p.writeErr(args.w, args.r, err, errCode)
		return
	}
	if !cmn.IsErrBucketNought(err) {
		args.p.writeErr(args.w, args.r, err, http.StatusBadRequest)
		return
	}
	// create ais bucket on the fly?
	if cmn.IsErrBckNotFound(err) /* ais bucket not found*/ && !args.createAIS {
		args.p.writeErr(args.w, args.r, err, errCode)
		return
	}
	// remote
	if cmn.IsErrRemoteBckNotFound(err) {
		// filtered
		if apc.IsFltPresent(args.fltPresence) {
			err = nil
			return
		}
		// NOTE: when and if `feat.NoHeadRemB` (feature) is globally enabled
		// use `api.CreateBucket`
		// (as in: explicit user request vs implied on-the-fly creation/addition)
		if cmn.Features.IsSet(feat.NoHeadRemB) || args.noHeadRemB {
			args.p.writeErrSilent(args.w, args.r, err, errCode)
			return
		}
	}
	// otherwise, try create remote bucket on the fly (ie., add to BMD)
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

	// In case of HDFS if the bucket does not exist in BMD there is no point
	// in checking if it exists remotely if we don't have `ref_directory`.
	if args.bck.IsHDFS() {
		err = cmn.NewErrBckNotFound(args.bck.Bucket())
		errCode = http.StatusNotFound
		return
	}

	if args.p.forwardCP(args.w, args.r, args.msg, "add-bucket", args.reqBody) {
		err = errForwarded
		return
	}

	// From this point on it's the primary - lookup via random target and try bucket add to BMD.
	bck = args.bck
	action := apc.ActCreateBck

	if backend := bck.Backend(); backend != nil {
		bck = backend
	}
	if bck.IsAIS() {
		glog.Warningf("%s: %q doesn't exist, proceeding to create", args.p.si, args.bck)
	}

	var remoteProps http.Header
	if bck.IsRemote() {
		action = apc.ActAddRemoteBck
		if remoteProps, errCode, err = args._lookup(bck); err != nil {
			bck = nil
			return
		}
	}

	if bck.IsHTTP() {
		if args.origURLBck != "" {
			remoteProps.Set(apc.HdrOrigURLBck, args.origURLBck)
		} else if origURL := args.getOrigURL(); origURL != "" {
			hbo, err := cmn.NewHTTPObjPath(origURL)
			if err != nil {
				errCode = http.StatusBadRequest
				return bck, errCode, err
			}
			remoteProps.Set(apc.HdrOrigURLBck, hbo.OrigURLBck)
		} else {
			err = fmt.Errorf("failed to initialize bucket %q: missing HTTP URL", args.bck)
			errCode = http.StatusBadRequest
			return
		}
		debug.Assert(remoteProps.Get(apc.HdrOrigURLBck) != "")
	}

	if err = args.p.createBucket(&apc.ActionMsg{Action: action}, bck, remoteProps); err != nil {
		errCode = crerrStatus(err)
		return
	}

	// Init the bucket after having successfully added it to the BMD.
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

// NOTE: alternatively, skip HEAD altogether when lsDontHeadRemoteBucket
func (args *bckInitArgs) _lookup(bck *cluster.Bck) (hdr http.Header, code int, err error) {
	q := url.Values{}
	if bck.IsHTTP() {
		origURL := args.getOrigURL()
		q.Set(apc.QparamOrigURL, origURL)
	}
	if args.tryHeadRemB {
		q.Set(apc.QparamSilent, "true")
	}
	hdr, code, err = args.p.headRemoteBck(bck.Bucket(), q)
	if (code == http.StatusUnauthorized || code == http.StatusForbidden) && args.tryHeadRemB {
		glog.Warningf("proceeding to add cloud bucket %s to the BMD after having failed HEAD request", bck)
		glog.Warningf("%s properties: using all defaults", bck)
		hdr = make(http.Header, 2)
		hdr.Set(apc.HdrBackendProvider, bck.Provider)
		hdr.Set(apc.HdrBucketVerEnabled, "false")
		err = nil
	}
	return
}
