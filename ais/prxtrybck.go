// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xaction"
)

type bckInitArgs struct {
	p       *proxyrunner
	w       http.ResponseWriter
	r       *http.Request
	reqBody []byte // request body of original request

	queryBck *cluster.Bck
	err      error
	msg      *cmn.ActionMsg

	perms cmn.AccessAttrs // cmn.AccessGET, cmn.AccessPATCH etc.

	skipBackend bool // initialize bucket `bck.InitNoBackend`
	tryOnlyRem  bool // try only creating remote bucket
	exists      bool // marks if bucket already exists
}

/////////////////////////////////////////////
// lookup and add bucket on the fly        //
/////////////////////////////////////////////

// init initializes bucket and checks access permissions.
func (args *bckInitArgs) init(bucket string) (bck *cluster.Bck, errCode int, err error) {
	if args.queryBck == nil {
		query := args.r.URL.Query()
		args.queryBck, err = newBckFromQuery(bucket, query)
		if err != nil {
			errCode = http.StatusBadRequest
			return
		}
	}

	bck = args.queryBck
	if err = args._checkRemoteBckPermissions(); err != nil {
		errCode = http.StatusBadRequest
		return
	}

	if args.skipBackend {
		err = bck.InitNoBackend(args.p.owner.bmd)
	} else {
		err = bck.Init(args.p.owner.bmd)
	}

	if err != nil && cmn.IsErrBucketNought(err) {
		errCode = http.StatusNotFound
		return
	}

	if err != nil {
		errCode = http.StatusBadRequest
		return
	}

	args.queryBck = bck
	args.exists = true

	// Check for msg.Action permission if permissions are not explicitly specified
	if args.perms == 0 && args.msg != nil {
		xactDtor, ok := xaction.XactsDtor[args.msg.Action]
		if !ok || xactDtor.Access == 0 {
			return
		}
		args.perms = xactDtor.Access
	}
	errCode, err = args._checkACL(bck)
	return
}

// TODO -- FIXME: A hack to check permissions specific to providers. Should automatically check based on providers.
func (args *bckInitArgs) _checkRemoteBckPermissions() (err error) {
	if args.queryBck.IsAIS() {
		return
	}

	bckType := "cloud"
	if args.queryBck.IsHTTP() {
		bckType = "http"
	}

	if args._requiresPermission(cmn.AccessMoveBucket) {
		goto retErr
	}

	// HDFS buckets are allowed to be deleted.
	if args.queryBck.IsHDFS() {
		return
	}

	// HTTP buckets should fail on PUT and bucket rename operations
	if args.queryBck.IsHTTP() && args._requiresPermission(cmn.AccessPUT) {
		goto retErr
	}

	// Destroy and Rename/Move are not permitted.
	if args.queryBck.IsCloud() && args._requiresPermission(cmn.AccessDestroyBucket) && args.msg.Action == cmn.ActDestroyBck {
		goto retErr
	}

	return
retErr:
	op := "operation"
	if args.msg != nil {
		op = fmt.Sprintf("operation %q", args.msg.Action)
	}
	err = fmt.Errorf("%s is not supported by %s buckets %q", op, bckType, args.queryBck)
	return
}

func (args *bckInitArgs) _requiresPermission(perm cmn.AccessAttrs) bool {
	return (args.perms & perm) == perm
}

func (args *bckInitArgs) _checkACL(bck *cluster.Bck) (errCode int, err error) {
	if err = args.p.checkACL(args.r.Header, bck, args.perms); err != nil {
		errCode = http.StatusForbidden
	}

	return
}

// initAndTry initializes bucket and tries to add it if doesn't exist.
// The method sets and returns err if was not successful and any point (if err
// is set then `p.invalmsghdlr` is called so caller doesn't need to).
func (args *bckInitArgs) initAndTry(bucket string, origURLBck ...string) (bck *cluster.Bck, err error) {
	var errCode int
	bck, errCode, err = args.init(bucket)
	if err == nil {
		return
	}
	if errCode != http.StatusNotFound {
		args.p.invalmsghdlr(args.w, args.r, err.Error(), errCode)
		return
	}

	// Should create only for remote bucket when `tryOnlyRem` flag is set.
	if _, ok := err.(*cmn.ErrorBucketDoesNotExist); ok && args.tryOnlyRem {
		args.p.invalmsghdlr(args.w, args.r, err.Error(), errCode)
		return
	}

	args.err = err
	bck, err = args.try(origURLBck...)
	return
}

func (args *bckInitArgs) try(origURLBck ...string) (bck *cluster.Bck, err error) {
	bck, errCode, err := args._try(origURLBck...)
	if err != nil && err != cmn.ErrForwarded {
		args.p.invalmsghdlr(args.w, args.r, err.Error(), errCode)
	}
	return bck, err
}

//
// methods that are internal to this source
//

func (args *bckInitArgs) _try(origURLBck ...string) (bck *cluster.Bck, errCode int, err error) {
	var cloudProps http.Header
	if args.err != nil && !cmn.IsErrBucketNought(args.err) {
		err = args.err
		errCode = http.StatusBadRequest
		return
	}

	if err = cmn.ValidateProvider(args.queryBck.Provider); err != nil {
		err = cmn.NewErrorInvalidBucketProvider(args.queryBck.Bck, err)
		errCode = http.StatusBadRequest
		return
	}

	// In case of HDFS if the bucket does not exist in BMD there is no point
	// in checking if it exists remotely if we don't have `ref_directory`.
	if args.queryBck.IsHDFS() {
		err = cmn.NewErrorBucketDoesNotExist(args.queryBck.Bck)
		errCode = http.StatusNotFound
		return
	}

	if args.p.forwardCP(args.w, args.r, args.msg, "add-bucket", args.reqBody) {
		err = cmn.ErrForwarded
		return
	}

	// From this point on it's the primary - lookup via random target and try bucket add to BMD.
	bck = args.queryBck
	action := cmn.ActCreateBck

	if bck.HasBackendBck() {
		bck = cluster.BackendBck(bck)
	}

	if bck.IsAIS() {
		glog.Warningf("%s: bucket %q doesn't exist, proceeding to create", args.p.si, args.queryBck.Bck)
	}

	if bck.IsCloud() || bck.IsHTTP() {
		action = cmn.ActRegisterCB
		if cloudProps, errCode, err = args._lookup(bck); err != nil {
			bck = nil
			return
		}
	}

	if bck.IsHTTP() {
		if len(origURLBck) > 0 {
			cloudProps.Set(cmn.HeaderOrigURLBck, origURLBck[0])
		} else if origURL := args.r.URL.Query().Get(cmn.URLParamOrigURL); origURL != "" {
			hbo, err := cmn.NewHTTPObjPath(origURL)
			if err != nil {
				errCode = http.StatusBadRequest
				return bck, errCode, err
			}
			cloudProps.Set(cmn.HeaderOrigURLBck, hbo.OrigURLBck)
		} else {
			err = fmt.Errorf("failed to initialize bucket %q: missing HTTP URL", args.queryBck)
			errCode = http.StatusBadRequest
			return
		}
		debug.Assert(cloudProps.Get(cmn.HeaderOrigURLBck) != "")
	}

	if err = args.p.createBucket(&cmn.ActionMsg{Action: action}, bck, cloudProps); err != nil {
		if _, ok := err.(*cmn.ErrorBucketAlreadyExists); !ok {
			errCode = http.StatusConflict
			return
		}
	}
	// init the bucket after having successfully added it to the BMD
	err = bck.Init(args.p.owner.bmd)
	if err != nil {
		err = fmt.Errorf("%s: unexpected failure to add remote %s, err: %v", args.p.si, bck, err)
		errCode = http.StatusInternalServerError
	}
	bck = args.queryBck
	return
}

func (args *bckInitArgs) _lookup(bck *cluster.Bck) (header http.Header, statusCode int, err error) {
	q := url.Values{}
	if bck.IsHTTP() {
		origURL := args.r.URL.Query().Get(cmn.URLParamOrigURL)
		q.Set(cmn.URLParamOrigURL, origURL)
	}
	return args.p.headRemoteBck(bck.Bck, q)
}
