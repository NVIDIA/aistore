// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

var errInvalidToken = errors.New("invalid token")

func (p *proxyrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
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
		msg := p.newAisMsgStr(cmn.ActNewPrimary, nil, nil)
		_ = p.metasyncer.sync(revsPair{p.authn.revokedTokenList(), msg})
	}
}

// Read a token from request header and validates it
// Header format:
//		'Authorization: Bearer <token>'
// Returns: is auth enabled, decoded token, error
func (p *proxyrunner) validateToken(hdr http.Header) (*cmn.AuthToken, error) {
	authToken := hdr.Get(cmn.HeaderAuthorization)
	idx := strings.Index(authToken, " ")
	if idx == -1 || authToken[:idx] != cmn.HeaderBearer {
		return nil, errInvalidToken
	}

	auth, err := p.authn.validateToken(authToken[idx+1:])
	if err != nil {
		glog.Errorf("invalid token: %v", err)
		return nil, errInvalidToken
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
func (p *proxyrunner) checkACL(hdr http.Header, bck *cluster.Bck, ace cmn.AccessAttrs) error {
	if isIntraCall(hdr) {
		return nil
	}
	var (
		token *cmn.AuthToken
		cfg   = cmn.GCO.Get()
	)
	if cfg.Auth.Enabled {
		token, err := p.validateToken(hdr)
		if err != nil {
			return err
		}
		uid := p.owner.smap.Get().UUID
		if err := token.CheckPermissions(uid, &bck.Bck, ace); err != nil {
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
