// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
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

	// 5 user or caller-provided control flags followed by
	// 3 result flags
	skipBackend    bool // initialize bucket via `bck.InitNoBackend`
	createAIS      bool // create ais bucket on the fly
	dontAddRemote  bool // do not create (ie., add -> BMD) remote bucket on the fly
	dontHeadRemote bool // do not HEAD remote bucket (to find out whether it exists and/or get properties)
	tryHeadRemote  bool // when listing objects anonymously (via ListObjsMsg.Flags LsTryHeadRemote)
	isPresent      bool // the bucket is confirmed to be present (in the cluster's BMD)
	exists         bool // remote bucket is confirmed to be exist
	modified       bool // bucket-defining control structure got modified
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

func (p *proxy) a2u(aliasOrUUID string) string {
	p.remais.mu.RLock()
	for _, remais := range p.remais.A {
		if aliasOrUUID == remais.Alias || aliasOrUUID == remais.UUID {
			p.remais.mu.RUnlock()
			return remais.UUID
		}
	}
	l := len(p.remais.old)
	for i := l - 1; i >= 0; i-- {
		remais := p.remais.old[i]
		if aliasOrUUID == remais.Alias || aliasOrUUID == remais.UUID {
			p.remais.mu.RUnlock()
			return remais.UUID
		}
	}
	p.remais.mu.RUnlock()
	return aliasOrUUID
}

// args.init initializes bucket and checks access permissions.
func (args *bckInitArgs) init() (errCode int, err error) {
	debug.Assert(args.bck != nil)

	bck := args.bck

	// remote ais aliasing
	if bck.IsRemoteAIS() {
		if uuid := args.p.a2u(bck.Ns.UUID); uuid != bck.Ns.UUID {
			args.modified = true
			// care of targets
			query := args.query
			if query == nil {
				query = args.r.URL.Query()
			}
			bck.Ns.UUID = uuid
			query.Set(apc.QparamNamespace, bck.Ns.Uname())
			args.r.URL.RawQuery = query.Encode()
		}
	}

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

// initAndTry initializes the bucket (proxy-only, as the filename implies).
// The method _may_ try to add it to the BMD if the bucket doesn't exist.
// NOTE:
// - on error it calls `p.writeErr` and friends, so make sure _not_ to do the same in the caller
// - for remais buckets: user-provided alias(***)
func (args *bckInitArgs) initAndTry() (bck *cluster.Bck, err error) {
	var errCode int

	// 1. init bucket
	bck = args.bck
	if errCode, err = args.init(); err == nil {
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

	// 3. create ais bucket _or_ lookup and, if confirmed, add remote bucket to the BMD
	// (see also: "on the fly")
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
		glog.Warningf("%s: %q doesn't exist, proceeding to create", args.p, args.bck)
		goto creadd
	}

	// lookup remote
	debug.Assert(bck.IsRemote())
	action = apc.ActAddRemoteBck
	if remoteHdr, errCode, err = args.lookup(bck); err != nil {
		bck = nil
		return
	}

	// orig-url for the ht:// bucket
	if bck.IsHTTP() {
		if args.origURLBck != "" {
			remoteHdr.Set(apc.HdrOrigURLBck, args.origURLBck)
		} else {
			var (
				hbo     *cmn.HTTPBckObj
				origURL = args.getOrigURL()
			)
			if origURL == "" {
				err = cmn.NewErrFailedTo(args.p, "initialize", args.bck, errors.New("missing HTTP URL"))
				return
			}
			if hbo, err = cmn.NewHTTPObjPath(origURL); err != nil {
				return
			}
			remoteHdr.Set(apc.HdrOrigURLBck, hbo.OrigURLBck)
		}
	}

	// when explicitly asked _not_ to
	args.exists = true
	if args.dontAddRemote {
		if bck.IsRemoteAIS() {
			bck.Props, err = remoteBckProps(bckPropsArgs{bck: bck, hdr: remoteHdr})
		} else {
			// NOTE -- TODO (dilemma):
			// The bucket has no local representation. The best would be to return its metadata _as is_
			// but there's currently no control structure other than (AIS-only) `BucketProps`.
			// Therefore: return cluster defaults + assorted `props.Extra` fields.
			// See also: ais/backend for `HeadBucket`
			bck.Props = defaultBckProps(bckPropsArgs{bck: bck, hdr: remoteHdr})
		}
		return
	}

	// add/create
creadd:
	if err = args.p.createBucket(&apc.ActionMsg{Action: action}, bck, remoteHdr); err != nil {
		errCode = crerrStatus(err)
		return
	}
	// finally, initialize the newly added/created
	if err = bck.Init(args.p.owner.bmd); err != nil {
		debug.AssertNoErr(err)
		errCode = http.StatusInternalServerError
		err = cmn.NewErrFailedTo(args.p, "post create-bucket init", bck, err, errCode)
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
		glog.Warningf("Proceeding to add remote bucket %s to the BMD after getting err: %v(%d)", bck, err, code)
		glog.Warningf("Using all cluster defaults for %s property values", bck)
		hdr = make(http.Header, 2)
		hdr.Set(apc.HdrBackendProvider, bck.Provider)
		hdr.Set(apc.HdrBucketVerEnabled, "false")
		err = nil
	}
	return
}
