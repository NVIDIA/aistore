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
	"github.com/NVIDIA/aistore/cmn"
)

var errInvalidToken = errors.New("invalid token")

func (p *proxyrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Tokens); err != nil {
		return
	}
	if p.forwardCP(w, r, &cmn.ActionMsg{Action: cmn.ActRevokeToken}, "revoke token", nil) {
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

func (p *proxyrunner) checkPermissions(hdr http.Header, bck *cmn.Bck, perms cmn.AccessAttrs) error {
	if isIntraCall(hdr) {
		return nil
	}
	cfg := cmn.GCO.Get()
	if !cfg.Auth.Enabled {
		return nil
	}
	token, err := p.validateToken(hdr)
	if err != nil {
		return err
	}
	uid := p.owner.smap.Get().UUID
	return token.CheckPermissions(uid, bck, perms)
}
