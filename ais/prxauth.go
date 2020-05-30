// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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

// TODO: better check for internal request - nid & pid are too insecure
func (p *proxyrunner) isInternalReq(r *http.Request) bool {
	pid := r.URL.Query().Get(cmn.URLParamProxyID)
	if pid != "" {
		return true
	}
	nid := r.Header.Get(cmn.HeaderNodeID)
	return nid != ""
}

// Read a token from request header and validates it
// Header format:
//		'Authorization: Bearer <token>'
// Returns: is auth enabled, decoded token, error
func (p *proxyrunner) validateToken(r *http.Request, cfg *cmn.Config) (bool, *cmn.AuthToken, error) {
	if p.isInternalReq(r) {
		return false, nil, nil
	}
	if cfg == nil {
		cfg = cmn.GCO.Get()
	}
	if !cfg.Auth.Enabled {
		return false, nil, nil
	}
	authToken := r.Header.Get(cmn.HeaderAuthorization)
	idx := strings.Index(authToken, " ")
	if idx == -1 || authToken[:idx] != cmn.HeaderBearer {
		return false, nil, errInvalidToken
	}

	auth, err := p.authn.validateToken(authToken[idx+1:])
	if err != nil {
		glog.Errorf("invalid token: %v", err)
		return false, nil, errInvalidToken
	}

	return true, auth, nil
}

func (p *proxyrunner) checkPermissions(r *http.Request, bck *cmn.Bck, perms uint64, cfg *cmn.Config) error {
	authOn, token, err := p.validateToken(r, cfg)
	if err != nil || !authOn {
		return nil
	}
	uid := p.owner.smap.Get().UUID
	if granted := token.HasPermissions(uid, bck, perms); granted {
		return nil
	}
	return cmn.ErrNoPermissions
}
