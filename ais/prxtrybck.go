// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type remBckAddArgs struct {
	p        *proxyrunner
	w        http.ResponseWriter
	r        *http.Request
	query    url.Values
	queryBck *cluster.Bck
	err      error
	msg      *cmn.ActionMsg
	// bck.IsHTTP()
	origURLBck   string
	allowHTTPBck bool
	// If set then error is not returned when bucket does not exist.
	allowBckNotExist bool
}

/////////////////////////////////////////////
// lookup and add remote bucket on the fly //
/////////////////////////////////////////////

// initAndTry initializes bucket and tries to add it if doesn't exist.
// The method sets and returns err if was not successful and any point (if err
// is set then `p.invalmsghdlr` is called so caller doesn't need to).
func (args *remBckAddArgs) initAndTry(bucket string, origURLBck ...string) (bck *cluster.Bck, err error) {
	if len(origURLBck) > 0 {
		args.allowHTTPBck = true
	}
	if args.queryBck == nil {
		if args.query == nil {
			args.query = args.r.URL.Query()
		}
		bck, err = newBckFromQuery(bucket, args.query)
		if err != nil {
			args.p.invalmsghdlr(args.w, args.r, err.Error(), http.StatusBadRequest)
			return nil, err
		}
	} else {
		bck = args.queryBck
	}
	if err = bck.Init(args.p.owner.bmd, args.p.si); err == nil {
		return bck, nil
	}

	args.queryBck = bck
	args.err = err
	if args.allowHTTPBck {
		if len(origURLBck) > 0 {
			args.allowHTTPBck = true
			args.origURLBck = origURLBck[0]
		} else if origURL := args.query.Get(cmn.URLParamOrigURL); origURL != "" {
			hbo, err := cmn.NewHTTPObjPath(origURL)
			if err != nil {
				args.p.invalmsghdlr(args.w, args.r, err.Error(), http.StatusBadRequest)
				return nil, err
			}
			args.origURLBck = hbo.OrigURLBck
		}
	}
	bck, err = args.try()
	return
}

func (args *remBckAddArgs) try() (bck *cluster.Bck, err error) {
	bck, err, errCode := args._try()
	if err != nil && err != cmn.ErrForwarded {
		if _, ok := err.(*cmn.ErrorBucketDoesNotExist); ok && args.allowBckNotExist {
			err = nil
		} else {
			args.p.invalmsghdlr(args.w, args.r, err.Error(), errCode)
		}
	}
	return bck, err
}

//
// methods that are internal to this source
//

func (args *remBckAddArgs) _try() (bck *cluster.Bck, err error, errCode int) {
	var cloudProps http.Header
	if _, ok := args.err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok {
		err = args.err
		if _, ok := err.(*cmn.ErrorBucketDoesNotExist); ok {
			errCode = http.StatusNotFound
		} else {
			errCode = http.StatusBadRequest
		}
		return
	}
	if !cmn.IsValidProvider(args.queryBck.Provider) {
		err = cmn.NewErrorInvalidBucketProvider(args.queryBck.Bck, args.p.si.Name())
		errCode = http.StatusBadRequest
		return
	}

	if args.queryBck.IsHTTP() && !args.allowHTTPBck {
		op := "operation"
		if args.msg != nil {
			op = fmt.Sprintf("operation %q", args.msg.Action)
		}
		err = fmt.Errorf("%s does not support http buckets (bucket=%s)", op, args.queryBck)
		errCode = http.StatusBadRequest
		return
	}
	// forward maybe
	if args.p.forwardCP(args.w, args.r, args.msg, "add-remote-bucket") {
		err = cmn.ErrForwarded
		return
	}
	// from this point on it's the primary - lookup via random target and try bucket add to BMD
	bck = args.queryBck
	if bck.Props != nil && bck.HasBackendBck() {
		bck = cluster.NewBckEmbed(bck.Props.BackendBck)
	}
	if cloudProps, err, errCode = args._lookup(bck); err != nil {
		bck = nil
		return
	}
	if args.queryBck.IsHTTP() {
		debug.Assert(args.origURLBck != "")
		cloudProps.Set(cmn.HeaderOrigURLBck, args.origURLBck)
	}
	if err = args.p.createBucket(&cmn.ActionMsg{Action: cmn.ActRegisterCB}, bck, cloudProps); err != nil {
		if _, ok := err.(*cmn.ErrorBucketAlreadyExists); !ok {
			errCode = http.StatusConflict
			return
		}
	}
	// init the bucket after having successfully added it to the BMD
	err = bck.Init(args.p.owner.bmd, args.p.si)
	if err != nil {
		err = fmt.Errorf("%s: unexpected failure to add remote %s, err: %v", args.p.si, bck, err)
		errCode = http.StatusInternalServerError
	}
	return
}

func (args *remBckAddArgs) _lookup(bck *cluster.Bck) (header http.Header, err error, statusCode int) {
	q := url.Values{}
	if bck.IsHTTP() {
		origURL := args.r.URL.Query().Get(cmn.URLParamOrigURL)
		q.Set(cmn.URLParamOrigURL, origURL)
	}
	return args.p.headCloudBck(bck.Bck, q)
}
