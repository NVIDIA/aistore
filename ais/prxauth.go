// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/authn"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func (p *proxyrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &tokenList{}
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathTokens.L); err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "revoke token") {
		return
	}
	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
		return
	}
	p.authn.updateRevokedList(tokenList)
	if p.owner.smap.get().isPrimary(p.si) {
		msg := p.newAmsgStr(cmn.ActNewPrimary, nil)
		_ = p.metasyncer.sync(revsPair{p.authn.revokedTokenList(), msg})
	}
}

// Read a token from request header and validates it
// Header format:
//		'Authorization: Bearer <token>'
// Returns: is auth enabled, decoded token, error
func (p *proxyrunner) validateToken(hdr http.Header) (*authn.Token, error) {
	authToken := hdr.Get(cmn.HdrAuthorization)
	if authToken == "" {
		return nil, authn.ErrNoToken
	}
	idx := strings.Index(authToken, " ")
	if idx == -1 || authToken[:idx] != cmn.AuthenticationTypeBearer {
		return nil, authn.ErrNoToken
	}

	auth, err := p.authn.validateToken(authToken[idx+1:])
	if err != nil {
		glog.Errorf("invalid token: %v", err)
		return nil, err
	}

	return auth, nil
}

// When AuthN is on, accessing a bucket requires two permissions:
//   - access to the bucket is granted to a user
//   - bucket ACL allows the required operation
//   Exception: a superuser can always PATCH the bucket/Set ACL
// If AuthN is off, only bucket permissions are checked.
//   Exceptions:
//   - read-only access to a bucket is always granted
//   - PATCH cannot be forbidden
func (p *proxyrunner) checkACL(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, ace cmn.AccessAttrs) error {
	err := p._checkACL(r.Header, bck, ace)
	if err == nil {
		return nil
	}
	p.writeErr(w, r, err, p.aclErrToCode(err))
	return err
}

func (*proxyrunner) aclErrToCode(err error) int {
	switch err {
	case nil:
		return http.StatusOK
	case authn.ErrNoToken:
		return http.StatusUnauthorized
	default:
		return http.StatusForbidden
	}
}

func (p *proxyrunner) _checkACL(hdr http.Header, bck *cluster.Bck, ace cmn.AccessAttrs) error {
	if p.isIntraCall(hdr) {
		return nil
	}
	var (
		token  *authn.Token
		cfg    = cmn.GCO.Get()
		bucket *cmn.Bck
		err    error
	)
	if cfg.Auth.Enabled {
		token, err = p.validateToken(hdr)
		if err != nil {
			return err
		}
		uid := p.owner.smap.Get().UUID

		if bck != nil {
			bucket = &bck.Bck
		}
		if err := token.CheckPermissions(uid, bucket, ace); err != nil {
			return err
		}
	}
	if bck == nil {
		// cluster ACL like create/list buckets, node management etc
		return nil
	}
	if !cfg.Auth.Enabled || token.IsAdmin {
		// PATCH and ACL are always allowed in two cases:
		// - a user is a superuser
		// - AuthN is disabled
		ace &^= (cmn.AccessPATCH | cmn.AccessBckSetACL)
	}
	if ace == 0 {
		return nil
	}
	if !cfg.Auth.Enabled {
		// Without AuthN, read-only access is always OK
		ace &^= cmn.AccessRO
	}
	return bck.Allow(ace)
}
