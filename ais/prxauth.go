// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/authnsrv"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	tokenList   authn.TokenList            // token strings
	tkList      map[string]*authnsrv.Token // tk structs
	authManager struct {
		sync.Mutex
		// cache of decrypted tokens
		tkList tkList
		// list of invalid tokens(revoked or of deleted users)
		// Authn sends these tokens to primary for broadcasting
		revokedTokens map[string]bool
		version       int64
	}
)

/////////////////
// authManager //
/////////////////

func newAuthManager() *authManager {
	return &authManager{tkList: make(tkList), revokedTokens: make(map[string]bool), version: 1}
}

// Add tokens to list of invalid ones. After that it cleans up the list
// from expired tokens
func (a *authManager) updateRevokedList(newRevoked *tokenList) (allRevoked *tokenList) {
	a.Lock()
	if newRevoked.Version == 0 {
		// manually revoked
		a.version++
	} else if newRevoked.Version > a.version {
		a.version = newRevoked.Version
	} else {
		glog.Errorf("Current token list v%d is greater than received v%d", a.version, newRevoked.Version)
		a.Unlock()
		return
	}
	// add new
	for _, token := range newRevoked.Tokens {
		a.revokedTokens[token] = true
		delete(a.tkList, token)
	}
	allRevoked = &tokenList{
		Tokens:  make([]string, 0, len(a.revokedTokens)),
		Version: a.version,
	}
	var (
		now    = time.Now()
		secret = cmn.GCO.Get().Auth.Secret
	)
	for token := range a.revokedTokens {
		tk, err := authnsrv.DecryptToken(token, secret)
		debug.AssertNoErr(err)
		if tk.Expires.Before(now) {
			delete(a.revokedTokens, token)
		} else {
			allRevoked.Tokens = append(allRevoked.Tokens, token)
		}
	}
	a.Unlock()
	if len(allRevoked.Tokens) == 0 {
		allRevoked = nil
	}
	return
}

func (a *authManager) revokedTokenList() (allRevoked *tokenList) {
	a.Lock()
	l := len(a.revokedTokens)
	if l == 0 {
		a.Unlock()
		return
	}
	allRevoked = &tokenList{Tokens: make([]string, 0, l), Version: a.version}
	for token := range a.revokedTokens {
		allRevoked.Tokens = append(allRevoked.Tokens, token)
	}
	a.Unlock()
	return
}

// Checks if a token is valid:
//   - must not be revoked one
//   - must not be expired
//   - must have all mandatory fields: userID, creds, issued, expires
// Returns decrypted token information if it is valid
func (a *authManager) validateToken(token string) (tk *authnsrv.Token, err error) {
	a.Lock()
	if _, ok := a.revokedTokens[token]; ok {
		tk, err = nil, fmt.Errorf("%v: %s", authnsrv.ErrTokenRevoked, tk)
	} else {
		tk, err = a.validateAddRm(token, time.Now())
	}
	a.Unlock()
	return
}

// Decrypts and validates token. Adds it to authManager.token if not found. Removes if expired.
// Must be called under lock.
func (a *authManager) validateAddRm(token string, now time.Time) (*authnsrv.Token, error) {
	tk, ok := a.tkList[token]
	if !ok || tk == nil {
		var (
			err    error
			secret = cmn.GCO.Get().Auth.Secret
		)
		if tk, err = authnsrv.DecryptToken(token, secret); err != nil {
			err = fmt.Errorf("%v: %v", authnsrv.ErrInvalidToken, err)
			return nil, err
		}
		a.tkList[token] = tk
	}
	debug.Assert(tk != nil)
	if tk.Expires.Before(now) {
		delete(a.tkList, token)
		return nil, fmt.Errorf("%v: %s", authnsrv.ErrTokenExpired, tk)
	}
	return tk, nil
}

///////////////
// tokenList //
///////////////

// interface guard
var _ revs = (*tokenList)(nil)

func (*tokenList) tag() string         { return revsTokenTag }
func (t *tokenList) version() int64    { return t.Version } // no versioning: receivers keep adding tokens to their lists
func (t *tokenList) marshal() []byte   { return cos.MustMarshal(t) }
func (t *tokenList) jit(_ *proxy) revs { return t }
func (*tokenList) sgl() *memsys.SGL    { return nil }
func (t *tokenList) String() string    { return fmt.Sprintf("TokenList v%d", t.Version) }

//
// proxy cont-ed
//

func (p *proxy) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &tokenList{}
	if _, err := p.checkRESTItems(w, r, 0, false, apc.URLPathTokens.L); err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "revoke token") {
		return
	}
	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
		return
	}
	allRevoked := p.authn.updateRevokedList(tokenList)
	if allRevoked != nil && p.owner.smap.get().isPrimary(p.si) {
		msg := p.newAmsgStr(apc.ActNewPrimary, nil)
		_ = p.metasyncer.sync(revsPair{allRevoked, msg})
	}
}

// Read a token from request header and validates it
// Header format:
//		'Authorization: Bearer <token>'
// Returns: is auth enabled, decoded token, error
func (p *proxy) validateToken(hdr http.Header) (*authnsrv.Token, error) {
	authToken := hdr.Get(apc.HdrAuthorization)
	if authToken == "" {
		return nil, authnsrv.ErrNoToken
	}
	idx := strings.Index(authToken, " ")
	if idx == -1 || authToken[:idx] != apc.AuthenticationTypeBearer {
		return nil, authnsrv.ErrNoToken
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
func (p *proxy) checkACL(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, ace apc.AccessAttrs) error {
	err := p._checkACL(r.Header, bck, ace)
	if err == nil {
		return nil
	}
	p.writeErr(w, r, err, p.aclErrToCode(err))
	return err
}

func (*proxy) aclErrToCode(err error) int {
	switch err {
	case nil:
		return http.StatusOK
	case authnsrv.ErrNoToken:
		return http.StatusUnauthorized
	default:
		return http.StatusForbidden
	}
}

func (p *proxy) _checkACL(hdr http.Header, bck *cluster.Bck, ace apc.AccessAttrs) error {
	var (
		tk     *authnsrv.Token
		bucket *cmn.Bck
		err    error
		cfg    = cmn.GCO.Get()
	)
	if p.isIntraCall(hdr, false /*from primary*/) == nil {
		return nil
	}
	if cfg.Auth.Enabled {
		tk, err = p.validateToken(hdr)
		if err != nil {
			return err
		}
		uid := p.owner.smap.Get().UUID

		if bck != nil {
			bucket = (*cmn.Bck)(bck)
		}
		if err := tk.CheckPermissions(uid, bucket, ace); err != nil {
			return err
		}
	}
	if bck == nil {
		// cluster ACL like create/list buckets, node management etc
		return nil
	}
	if !cfg.Auth.Enabled || tk.IsAdmin {
		// PATCH and ACL are always allowed in two cases:
		// - a user is a superuser
		// - AuthN is disabled
		ace &^= (apc.AcePATCH | apc.AceBckSetACL)
	}
	if ace == 0 {
		return nil
	}
	if !cfg.Auth.Enabled {
		// Without AuthN, read-only access is always OK
		ace &^= apc.AccessRO
	}
	return bck.Allow(ace)
}
