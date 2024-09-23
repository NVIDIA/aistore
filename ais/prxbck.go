// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"net/url"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
)

type bctx struct {
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
	ib0        bctx
)

func allocBctx() (a *bctx) {
	if v := ibargsPool.Get(); v != nil {
		a = v.(*bctx)
		return
	}
	return &bctx{}
}

func freeBctx(a *bctx) {
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

// initialize bucket and check access permissions
func (bctx *bctx) init() (_ int, err error) {
	debug.Assert(bctx.bck != nil)

	bck := bctx.bck

	// remote ais aliasing
	if bck.IsRemoteAIS() {
		if uuid := bctx.p.a2u(bck.Ns.UUID); uuid != bck.Ns.UUID {
			bctx.modified = true
			// care of targets
			query := bctx.query
			if query == nil {
				query = bctx.r.URL.Query()
			}
			bck.Ns.UUID = uuid
			query.Set(apc.QparamNamespace, bck.Ns.Uname())
			bctx.r.URL.RawQuery = query.Encode()
		}
	}

	if err = bctx.accessSupported(); err != nil {
		return http.StatusMethodNotAllowed, err
	}
	if bctx.skipBackend {
		err = bck.InitNoBackend(bctx.p.owner.bmd)
	} else {
		err = bck.Init(bctx.p.owner.bmd)
	}
	if err != nil {
		if cmn.IsErrBucketNought(err) {
			return http.StatusNotFound, err
		}
		return http.StatusBadRequest, err
	}

	bctx.isPresent = true

	// if permissions are not explicitly specified check the default (msg.Action => permissions)
	if bctx.perms == 0 && bctx.msg != nil {
		dtor, ok := xact.Table[bctx.msg.Action]
		if !ok || dtor.Access == 0 {
			return
		}
		bctx.perms = dtor.Access
	}
	return bctx.accessAllowed(bck)
}

// returns true when operation requires the 'perm' type access
func (bctx *bctx) _perm(perm apc.AccessAttrs) bool { return (bctx.perms & perm) == perm }

// (compare w/ accessAllowed)
func (bctx *bctx) accessSupported() error {
	if !bctx.bck.IsRemote() {
		return nil
	}

	var op string
	if bctx._perm(apc.AceMoveBucket) {
		op = "rename/move remote bucket"
		goto rerr
	}
	// HTTP buckets are not writeable
	if bctx.bck.IsHT() && bctx._perm(apc.AcePUT) {
		op = "write to HTTP bucket"
		goto rerr
	}
	// Cloud bucket: destroy op. not allowed, and not supported yet
	// (have no separate perm for eviction, that's why an extra check)
	if rmb := bctx.bck.IsCloud() && bctx._perm(apc.AceDestroyBucket) && bctx.msg.Action == apc.ActDestroyBck; !rmb {
		return nil
	}
	op = "destroy cloud bucket"
rerr:
	return cmn.NewErrUnsupp(op, bctx.bck.Cname(""))
}

// (compare w/ accessSupported)
func (bctx *bctx) accessAllowed(bck *meta.Bck) (ecode int, err error) {
	err = bctx.p.access(bctx.r.Header, bck, bctx.perms)
	ecode = aceErrToCode(err)
	return ecode, err
}

// initAndTry initializes the bucket (proxy-only, as the filename implies).
// The method _may_ try to add it to the BMD if the bucket doesn't exist.
// NOTE:
// - on error it calls `p.writeErr` and friends, so make sure _not_ to do the same in the caller
// - for remais buckets: user-provided alias(***)
func (bctx *bctx) initAndTry() (bck *meta.Bck, err error) {
	var ecode int

	// 1. init bucket
	bck = bctx.bck
	if ecode, err = bctx.init(); err == nil {
		return
	}
	if ecode != http.StatusNotFound {
		// user GET and PUT requests: making a _silent_ exception for assorted error codes
		// (counting them via stats.IncErr though)
		if bctx.perms == apc.AceGET || bctx.perms == apc.AcePUT {
			if ecode == http.StatusUnauthorized || ecode == http.StatusForbidden {
				bctx.p.writeErr(bctx.w, bctx.r, err, ecode, Silent)
				return
			}
		}
		bctx.p.writeErr(bctx.w, bctx.r, err, ecode)
		return
	}
	// 2. handle two specific errors
	switch {
	case cmn.IsErrBckNotFound(err):
		debug.Assert(bck.IsAIS())
		if !bctx.createAIS {
			if bctx.perms == apc.AceBckHEAD {
				bctx.p.writeErr(bctx.w, bctx.r, err, ecode, Silent)
			} else {
				bctx.p.writeErr(bctx.w, bctx.r, err, ecode)
			}
			return
		}
	case cmn.IsErrRemoteBckNotFound(err):
		debug.Assert(bck.IsRemote())
		// when remote-bucket lookup is not permitted
		if bctx.dontHeadRemote {
			bctx.p.writeErr(bctx.w, bctx.r, err, ecode, Silent)
			return
		}
	default:
		debug.Assertf(false, "%q: unexpected %v(%d)", bctx.bck, err, ecode)
		bctx.p.writeErr(bctx.w, bctx.r, err, ecode)
		return
	}

	// 3. create ais bucket _or_ lookup and, *if* confirmed, add remote bucket to the BMD
	// (see also: "on the fly")
	return bctx.try()
}

func (bctx *bctx) try() (bck *meta.Bck, err error) {
	bck, ecode, err := bctx._try()
	switch {
	case err == nil || err == errForwarded:
		return bck, err
	case cmn.IsErrBucketAlreadyExists(err):
		// e.g., when (re)setting backend two times in a row
		// TODO: return http.StatusNoContent
		nlog.Infoln(bctx.p.String(), err, " - nothing to do")
		return bck, nil
	default:
		if bctx.perms == apc.AceBckHEAD {
			bctx.p.writeErr(bctx.w, bctx.r, err, ecode, Silent)
		} else {
			bctx.p.writeErr(bctx.w, bctx.r, err, ecode)
		}
		return bck, err
	}
}

//
// methods that are internal to this source
//

func (bctx *bctx) _try() (bck *meta.Bck, ecode int, err error) {
	if err = bctx.bck.Validate(); err != nil {
		return bck, http.StatusBadRequest, err
	}
	if bctx.p.forwardCP(bctx.w, bctx.r, bctx.msg, "add-bucket", bctx.reqBody) {
		return bck, 0, errForwarded
	}

	// am primary from this point on
	bck = bctx.bck
	var (
		action    = apc.ActCreateBck
		remoteHdr http.Header
	)
	if backend := bck.Backend(); backend != nil {
		bck = backend
	}
	if bck.IsAIS() {
		if err = bctx.p.access(bctx.r.Header, nil /*bck*/, apc.AceCreateBucket); err != nil {
			return bck, aceErrToCode(err), err
		}
		nlog.Warningf("%s: %q doesn't exist, proceeding to create", bctx.p, bctx.bck)
		goto creadd
	}
	action = apc.ActAddRemoteBck // only if requested via bctx

	// lookup remote
	if remoteHdr, ecode, err = bctx.lookup(bck); err != nil {
		return nil, ecode, err
	}

	// orig-url for the ht:// bucket
	if bck.IsHT() {
		if bctx.origURLBck != "" {
			remoteHdr.Set(apc.HdrOrigURLBck, bctx.origURLBck)
		} else {
			var (
				hbo     *cmn.HTTPBckObj
				origURL = bctx.getOrigURL()
			)
			if origURL == "" {
				return bck, 0, cmn.NewErrFailedTo(bctx.p, "initialize", bctx.bck, errors.New("missing HTTP URL"))
			}
			if hbo, err = cmn.NewHTTPObjPath(origURL); err != nil {
				return
			}
			remoteHdr.Set(apc.HdrOrigURLBck, hbo.OrigURLBck)
		}
	}

	// when explicitly asked _not to_
	bctx.exists = true
	if bctx.dontAddRemote {
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
	if err = bctx.p.createBucket(&apc.ActMsg{Action: action}, bck, remoteHdr); err != nil {
		return bck, crerrStatus(err), err
	}

	// finally, initialize the newly added/created
	if err = bck.Init(bctx.p.owner.bmd); err != nil {
		debug.AssertNoErr(err)
		return bck, http.StatusInternalServerError,
			cmn.NewErrFailedTo(bctx.p, "post create-bucket init", bck, err, ecode)
	}

	err = bctx.p.onEC(bck)

	bck = bctx.bck
	return
}

func (bctx *bctx) getOrigURL() (ourl string) {
	if bctx.query != nil {
		debug.Assert(bctx.dpq == nil)
		ourl = bctx.query.Get(apc.QparamOrigURL)
	} else {
		ourl = bctx.dpq.origURL
	}
	return
}

func (bctx *bctx) lookup(bck *meta.Bck) (hdr http.Header, code int, err error) {
	var (
		q       = url.Values{}
		retried bool
	)
	if bck.IsHT() {
		origURL := bctx.getOrigURL()
		q.Set(apc.QparamOrigURL, origURL)
	}
	if bctx.tryHeadRemote {
		q.Set(apc.QparamSilent, "true")
	}
retry:
	hdr, code, err = bctx.p.headRemoteBck(bck.Bucket(), q)

	if (code == http.StatusUnauthorized || code == http.StatusForbidden) && bctx.tryHeadRemote {
		if bctx.dontAddRemote {
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
		nlog.Warningf("%s: HEAD(%s) timeout %q - retrying...", bctx.p, bck, errors.Unwrap(err))
		retried = true
		goto retry
	}
	return
}
