// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"errors"
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
	tokenList authn.TokenList       // token strings
	tkList    map[string]*tok.Token // tk structs

	authManager struct {
		// cache of decrypted tokens
		tkList tkList
		// list of invalid tokens(revoked or of deleted users)
		// Authn sends these tokens to primary for broadcasting
		revokedTokens map[string]bool
		// signing key secret
		secret string
		// public key for validating tokens
		rsaPubKey *rsa.PublicKey
		// latest revoked version
		version int64
		// lock
		sync.Mutex
	}
)

/////////////////
// authManager //
/////////////////

func newAuthManager(config *cmn.Config) *authManager {
	rsaPubKey, err := parsePubKey(cos.Right(config.Auth.PubKey, os.Getenv(env.AisAuthPublicKey)))
	if err != nil {
		cos.ExitLogf("Failed to parse RSA public key: %v", err)
	}
	return &authManager{
		tkList:        make(tkList),
		revokedTokens: make(map[string]bool), // TODO: preallocate
		version:       1,
		secret:        cos.Right(config.Auth.Secret, os.Getenv(env.AisAuthSecretKey)), // environment override
		rsaPubKey:     rsaPubKey,
	}
}

func parsePubKey(str string) (*rsa.PublicKey, error) {
	if str == "" {
		return nil, nil
	}
	// Parse b64-encoded RSA public key from string
	derBytes, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, err
	}
	pub, err := x509.ParsePKIXPublicKey(derBytes)
	if err != nil {
		return nil, err
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("not an RSA public key")
	}
	return rsaPub, nil
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
		return nil
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
		tk, err := tok.ValidateToken(token, a.secret, a.rsaPubKey)
		debug.AssertNoErr(err)
		if tk.IsExpired(&now) {
			delete(a.revokedTokens, token)
		} else {
			allRevoked.Tokens = append(allRevoked.Tokens, token)
		}
	}
	if len(allRevoked.Tokens) == 0 {
		allRevoked = nil
	}
	return allRevoked
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
		tk, err = a.validateAddRm(token)
	}
	a.Unlock()
	return
}

// Decrypts and validates token. Adds it to authManager.token if not found. Removes if expired.
// Must be called under lock.
func (a *authManager) validateAddRm(token string) (*tok.Token, error) {
	tk, ok := a.tkList[token]
	debug.Assert(!ok || tk != nil)
	if !ok || tk == nil {
		var err error
		if tk, err = tok.ValidateToken(token, a.secret, a.rsaPubKey); err != nil {
			nlog.Errorln(err)
			return nil, tok.ErrInvalidToken
		}
		a.tkList[token] = tk
	}
	if tk.IsExpired(nil) {
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
		p.validateKey(w, r)
	case http.MethodDelete:
		p.delToken(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodPost, http.MethodDelete)
	}
}

// Given a secret key or a public key, validate if that key is valid for requests to this cluster
func (p *proxy) validateKey(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathTokens.L, 0, false); err != nil {
		return
	}

	reqConf := &authn.ServerConf{}
	if err := cmn.ReadJSON(w, r, reqConf); err != nil {
		return
	}

	if reqConf.Secret == "" && reqConf.PubKey == nil {
		p.writeErrf(w, r, "no secret or public key provided to validate")
		return
	}
	if reqConf.Secret != "" {
		cksumVal := cos.ChecksumB2S(cos.UnsafeB(p.authn.secret), cos.ChecksumSHA256)
		if cksumVal != reqConf.Secret {
			p.writeErrf(w, r, "%s: invalid secret sha256(%q)", p, cos.SHead(reqConf.Secret))
		}
	}
	if reqConf.PubKey != nil {
		reqKey, err := parsePubKey(*reqConf.PubKey)
		if err != nil {
			p.writeErrf(w, r, "%s: invalid public key (%q)", p, cos.SHead(*reqConf.PubKey))
			return
		}
		if !reqKey.Equal(p.authn.rsaPubKey) {
			p.writeErrf(w, r, "%s: provided public key (%q) does not match cluster's public key", p, cos.SHead(*reqConf.PubKey))
			return
		}
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
	switch {
	case err == nil:
	case errors.Is(err, tok.ErrNoToken) || errors.Is(err, tok.ErrInvalidToken):
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
