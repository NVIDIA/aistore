// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/xact"
)

type bckInitArgs struct {
	w http.ResponseWriter
	r *http.Request

	p *proxy

	bck *meta.Bck
	msg *apc.ActMsg

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
	exists         bool // remote bucket is confirmed to exist
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
		dtor, ok := xact.Table[args.msg.Action]
		if !ok || dtor.Access == 0 {
			return
		}
		args.perms = dtor.Access
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

func (args *bckInitArgs) access(bck *meta.Bck) (errCode int, err error) {
	err = args.p.access(args.r.Header, bck, args.perms)
	errCode = aceErrToCode(err)
	return
}

// initAndTry initializes the bucket (proxy-only, as the filename implies).
// The method _may_ try to add it to the BMD if the bucket doesn't exist.
// NOTE:
// - on error it calls `p.writeErr` and friends, so make sure _not_ to do the same in the caller
// - for remais buckets: user-provided alias(***)
func (args *bckInitArgs) initAndTry() (bck *meta.Bck, err error) {
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
			if args.perms == apc.AceBckHEAD {
				args.p.writeErr(args.w, args.r, err, errCode, Silent)
			} else {
				args.p.writeErr(args.w, args.r, err, errCode)
			}
			return
		}
	case cmn.IsErrRemoteBckNotFound(err):
		debug.Assert(bck.IsRemote())
		// when remote-bucket lookup is not permitted
		if args.dontHeadRemote {
			args.p.writeErr(args.w, args.r, err, errCode, Silent)
			return
		}
	default:
		debug.Assertf(false, "%q: unexpected %v(%d)", args.bck, err, errCode)
		args.p.writeErr(args.w, args.r, err, errCode)
		return
	}

	// 3. create ais bucket _or_ lookup and, *if* confirmed, add remote bucket to the BMD
	// (see also: "on the fly")
	bck, err = args.try()
	return
}

func (args *bckInitArgs) try() (bck *meta.Bck, err error) {
	bck, errCode, err := args._try()
	if err != nil && err != errForwarded {
		if cmn.IsErrBucketAlreadyExists(err) {
			// e.g., when (re)setting backend two times in a row
			nlog.Infoln(args.p.String()+":", err, " - nothing to do")
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

func (args *bckInitArgs) _try() (bck *meta.Bck, errCode int, err error) {
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
		if err = args.p.access(args.r.Header, nil /*bck*/, apc.AceCreateBucket); err != nil {
			errCode = aceErrToCode(err)
			return
		}
		nlog.Warningf("%s: %q doesn't exist, proceeding to create", args.p, args.bck)
		goto creadd
	}
	action = apc.ActAddRemoteBck // only if requested via args

	// lookup remote
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

	// when explicitly asked _not to_
	args.exists = true
	if args.dontAddRemote {
		if bck.IsRemoteAIS() {
			bck.Props, err = remoteBckProps(bckPropsArgs{bck: bck, hdr: remoteHdr})
		} else {
			// Background (#18995):
			//
			// The bucket is not in the BMD - has no local representation. The best we could do
			// is return remote metadata as is. But there's no control structure for that
			// other than (AIS-only) `BucketProps`.
			// Therefore: return the result of merging cluster defaults with the remote header
			// resulting from the backend.Head(bucket) call and containing actual
			// values (e.g. versioning).
			// The returned bucket props will have its BID == 0 (zero), which also means:
			// this bucket is not initialized and/or not present in BMD.

			bck.Props = defaultBckProps(bckPropsArgs{bck: bck, hdr: remoteHdr})
		}
		return
	}

	// add/create
creadd:
	if err = args.p.createBucket(&apc.ActMsg{Action: action}, bck, remoteHdr); err != nil {
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

func (args *bckInitArgs) lookup(bck *meta.Bck) (hdr http.Header, code int, err error) {
	var (
		q       = url.Values{}
		retried bool
	)
	if bck.IsHTTP() {
		origURL := args.getOrigURL()
		q.Set(apc.QparamOrigURL, origURL)
	}
	if args.tryHeadRemote {
		q.Set(apc.QparamSilent, "true")
	}
retry:
	hdr, code, err = args.p.headRemoteBck(bck.Bucket(), q)

	if (code == http.StatusUnauthorized || code == http.StatusForbidden) && args.tryHeadRemote {
		if args.dontAddRemote {
			return
		}
		// NOTE: assuming OK
		nlog.Warningf("Proceeding to add remote bucket %s to the BMD after getting err: %v(%d)", bck, err, code)
		nlog.Warningf("Using all cluster defaults for %s property values", bck)
		hdr = make(http.Header, 2)
		hdr.Set(apc.HdrBackendProvider, bck.Provider)
		hdr.Set(apc.HdrBucketVerEnabled, "false")
		err = nil
		return
	}
	// NOTE: retrying once (via random target)
	if err != nil && !retried && cos.IsErrClientURLTimeout(err) {
		nlog.Warningf("%s: HEAD(%s) timeout %q - retrying...", args.p, bck, errors.Unwrap(err))
		retried = true
		goto retry
	}
	return
}
