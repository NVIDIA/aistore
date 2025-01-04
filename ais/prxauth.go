// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018=2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	tokenList   authn.TokenList       // token strings
	tkList      map[string]*tok.Token // tk structs
	authManager struct {
		// cache of decrypted tokens
		tkList tkList
		// list of invalid tokens(revoked or of deleted users)
		// Authn sends these tokens to primary for broadcasting
		revokedTokens map[string]bool
		version       int64
		// signing key secret
		secret string
		// lock
		sync.Mutex
	}
)

/////////////////
// authManager //
/////////////////

func newAuthManager(config *cmn.Config) *authManager {
	return &authManager{
		tkList:        make(tkList),
		revokedTokens: make(map[string]bool), // TODO: preallocate
		version:       1,
		secret:        cos.Right(config.Auth.Secret, os.Getenv(env.AisAuthSecretKey)), // environment override
	}
}

// Add tokens to the list of invalid ones and clean up the list from expired tokens.
func (a *authManager) updateRevokedList(newRevoked *tokenList) (allRevoked *tokenList) {
	a.Lock()
	defer a.Unlock()

	switch {
	case newRevoked.Version == 0: // Manually revoked tokens
		a.version++
	case newRevoked.Version > a.version:
		a.version = newRevoked.Version
	default:
		nlog.Errorf("Current token list v%d is greater than received v%d", a.version, newRevoked.Version)
		return
	}

	// Add new revoked tokens and remove them from the valid token list.
	for _, token := range newRevoked.Tokens {
		a.revokedTokens[token] = true
		delete(a.tkList, token)
	}

	allRevoked = &tokenList{
		Tokens:  make([]string, 0, len(a.revokedTokens)),
		Version: a.version,
	}

	// Clean up expired tokens from the revoked list.
	now := time.Now()

	for token := range a.revokedTokens {
		tk, err := tok.DecryptToken(token, a.secret)
		debug.AssertNoErr(err)
		if tk.Expires.Before(now) {
			delete(a.revokedTokens, token)
		} else {
			allRevoked.Tokens = append(allRevoked.Tokens, token)
		}
	}
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
//
// Returns decrypted token information if it is valid
func (a *authManager) validateToken(token string) (tk *tok.Token, err error) {
	a.Lock()
	if _, ok := a.revokedTokens[token]; ok {
		tk, err = nil, fmt.Errorf("%v: %s", tok.ErrTokenRevoked, tk)
	} else {
		tk, err = a.validateAddRm(token, time.Now())
	}
	a.Unlock()
	return
}

// Decrypts and validates token. Adds it to authManager.token if not found. Removes if expired.
// Must be called under lock.
func (a *authManager) validateAddRm(token string, now time.Time) (*tok.Token, error) {
	tk, ok := a.tkList[token]
	if !ok || tk == nil {
		var err error
		if tk, err = tok.DecryptToken(token, a.secret); err != nil {
			nlog.Errorln(err)
			return nil, tok.ErrInvalidToken
		}
		a.tkList[token] = tk
	}
	if tk.Expires.Before(now) {
		delete(a.tkList, token)
		return nil, fmt.Errorf("%v: %s", tok.ErrTokenExpired, tk)
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
func (*tokenList) uuid() string        { return "" }        // TODO: add
func (t *tokenList) marshal() []byte   { return cos.MustMarshal(t) }
func (t *tokenList) jit(_ *proxy) revs { return t }
func (*tokenList) sgl() *memsys.SGL    { return nil }
func (t *tokenList) String() string    { return fmt.Sprintf("TokenList v%d", t.Version) }

//
// proxy cont-ed
//

// [METHOD] /v1/tokens
func (p *proxy) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.validateSecret(w, r)
	case http.MethodDelete:
		p.delToken(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete)
	}
}

func (p *proxy) validateSecret(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathTokens.L, 0, false); err != nil {
		return
	}
	cksum := cos.NewCksumHash(cos.ChecksumSHA256)
	cksum.H.Write([]byte(p.authn.secret))
	cksum.Finalize()

	cluConf := &authn.ServerConf{}
	if err := cmn.ReadJSON(w, r, cluConf); err != nil {
		return
	}
	if cksum.Val() != cluConf.Secret {
		p.writeErrf(w, r, "%s: invalid secret sha256(%q)", p, cos.SHead(cluConf.Secret))
	}
}

func (p *proxy) delToken(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathTokens.L, 0, false); err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "revoke token") {
		return
	}
	tokenList := &tokenList{}
	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
		return
	}
	allRevoked := p.authn.updateRevokedList(tokenList)
	if allRevoked != nil && p.owner.smap.get().isPrimary(p.si) {
		msg := p.newAmsgStr(apc.ActNewPrimary, nil)
		_ = p.metasyncer.sync(revsPair{allRevoked, msg})
	}
}

// Validates a token from the request header
func (p *proxy) validateToken(hdr http.Header) (*tok.Token, error) {
	token, err := tok.ExtractToken(hdr)
	if err != nil {
		return nil, err
	}
	tk, err := p.authn.validateToken(token)
	if err != nil {
		nlog.Errorf("invalid token: %v", err)
		return nil, err
	}
	return tk, nil
}

// When AuthN is on, accessing a bucket requires two permissions:
//   - access to the bucket is granted to a user
//   - bucket ACL allows the required operation
//     Exception: a superuser can always PATCH the bucket/Set ACL
//
// If AuthN is off, only bucket permissions are checked.
//
//	Exceptions:
//	- read-only access to a bucket is always granted
//	- PATCH cannot be forbidden
func (p *proxy) checkAccess(w http.ResponseWriter, r *http.Request, bck *meta.Bck, ace apc.AccessAttrs) (err error) {
	if err = p.access(r.Header, bck, ace); err != nil {
		p.writeErr(w, r, err, aceErrToCode(err))
	}
	return
}

func aceErrToCode(err error) (status int) {
	switch err {
	case nil:
	case tok.ErrNoToken, tok.ErrInvalidToken:
		status = http.StatusUnauthorized
	default:
		status = http.StatusForbidden
	}
	return status
}

func (p *proxy) access(hdr http.Header, bck *meta.Bck, ace apc.AccessAttrs) (err error) {
	var (
		tk     *tok.Token
		bucket *cmn.Bck
	)
	if p.checkIntraCall(hdr, false /*from primary*/) == nil {
		return nil
	}
	if cmn.Rom.AuthEnabled() { // config.Auth.Enabled
		tk, err = p.validateToken(hdr)
		if err != nil {
			// NOTE: making exception to allow 3rd party clients read remote ht://bucket
			if err == tok.ErrNoToken && bck != nil && bck.IsHT() {
				err = nil
			}
			return err
		}
		uid := p.owner.smap.Get().UUID
		if bck != nil {
			bucket = bck.Bucket()
		}
		if err := tk.CheckPermissions(uid, bucket, ace); err != nil {
			return err
		}
	}
	if bck == nil {
		// cluster ACL: create/list buckets, node management, etc.
		return nil
	}

	// bucket access conventions:
	// - without AuthN: read-only access, PATCH, and ACL
	// - with AuthN:    superuser can PATCH and change ACL
	if !cmn.Rom.AuthEnabled() {
		ace &^= (apc.AcePATCH | apc.AceBckSetACL | apc.AccessRO)
	} else if tk.IsAdmin {
		ace &^= (apc.AcePATCH | apc.AceBckSetACL)
	}
	if ace == 0 {
		return nil
	}
	return bck.Allow(ace)
}
