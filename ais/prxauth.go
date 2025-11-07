// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

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

	onexxh "github.com/OneOfOne/xxhash"
)

type (
	tokenList   authn.TokenList // token strings
	authManager struct {
		// used for parsing and validating claims from token strings
		tokenParser tok.Parser
		// provides thread-safe access to a cache of decrypted token claims
		tokenMap *shardedTokenMap
		// provides thread-safe access to an underlying map of tokens
		revokedTokens *RevokedTokensMap
	}

	RevokedTokensMap struct {
		// list of invalid tokens(revoked or of deleted users)
		// Authn sends these tokens to primary for broadcasting
		revokedTokens map[string]bool
		// latest revoked version
		version int64
		// lock
		sync.RWMutex
	}

	shardedTokenMap struct {
		// Shard tokens into separate maps for improved locking
		shards []*tokenMap
	}

	tokenMap struct {
		tokens map[string]*tok.AISClaims
		sync.RWMutex
	}
)

// TokenMapShardExponent is used to define the number of maps used for parallel locking of token -> claims
// The actual number of shards will be equal to 2^TokenMapShardExponent
const TokenMapShardExponent = 4

/////////////////
// authManager //
/////////////////

func newAuthManager(config *cmn.Config) *authManager {
	var requiredClaims *tok.RequiredClaims
	if config.Auth.RequiredClaims != nil {
		requiredClaims = &tok.RequiredClaims{
			Aud: config.Auth.RequiredClaims.Aud,
		}
	}
	return &authManager{
		tokenMap:      newShardedTokenMap(TokenMapShardExponent),
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   tok.NewTokenParser(newSigConfig(&config.Auth), requiredClaims),
	}
}

// Parse config and environment variables to determine keys for validating JWT signatures
func newSigConfig(authConf *cmn.AuthConf) *tok.SigConfig {
	// First check for env vars as they take precedence
	if pubKeyEnvStr := os.Getenv(env.AisAuthPublicKey); pubKeyEnvStr != "" {
		pubKey, err := tok.ParsePubKey(pubKeyEnvStr)
		if err != nil {
			cos.ExitLogf("Failed to parse RSA public key: %v", err)
		}
		return &tok.SigConfig{RSAPublicKey: pubKey}
	}
	if hmacEnvStr := os.Getenv(env.AisAuthSecretKey); hmacEnvStr != "" {
		return &tok.SigConfig{HMACSecret: hmacEnvStr}
	}
	// Empty config is valid with enabled auth as OIDC issuer lookup may be used
	if authConf.Signature == nil || authConf.Signature.Method == "" {
		return &tok.SigConfig{}
	}

	// Finally check config -- parse according to provided method
	m := strings.ToUpper(authConf.Signature.Method)
	switch {
	case authConf.Signature.IsHMAC():
		return &tok.SigConfig{HMACSecret: authConf.Signature.Key}
	case authConf.Signature.IsRSA():
		pubKey, err := tok.ParsePubKey(authConf.Signature.Key)
		if err != nil {
			cos.ExitLogf("Failed to parse RSA public key: %v", err)
		}
		return &tok.SigConfig{RSAPublicKey: pubKey}
	default:
		cos.ExitLogf("Auth enabled with invalid key signature: %q. Supported values are: %s", m, authConf.Signature.ValidMethods())
		return nil
	}
}

// Add tokens to the list of invalid ones and clean up the list from expired tokens.
func (a *authManager) updateRevokedList(newRevoked *tokenList) (allRevoked *tokenList) {
	// Add new revoked tokens -- error if invalid version
	err := a.revokedTokens.update(newRevoked)
	if err != nil {
		return nil
	}
	// Remove revoked tokens from the token cache
	a.tokenMap.deleteMultiple(newRevoked.Tokens)
	// Clean up any expired tokens from the revoked list
	return a.revokedTokens.cleanup(a.tokenParser)
}

func (a *authManager) revokedTokenList() (allRevoked *tokenList) {
	return a.revokedTokens.getAll()
}

// Checks if a token is valid:
//   - must not be revoked one
//   - must not be expired
//   - must have valid JWT claims or equivalent: sub, iss, aud, exp
//
// Caches and returns decoded token claims if it is valid
func (a *authManager) validateToken(token string) (*tok.AISClaims, error) {
	if a.revokedTokens.contains(token) {
		return nil, fmt.Errorf("%w [err: %w]: %s", tok.ErrInvalidToken, tok.ErrTokenRevoked, token)
	}
	claims, ok := a.tokenMap.getClaims(token)
	debug.Assert(!ok || claims != nil)
	// If token string exists in cache, only need to check claim expiry
	if ok && claims != nil {
		if claims.IsExpired() {
			// This takes a separate write lock than the previous read, but:
			// - claims are immutable once cached
			// - we only delete expired claims, never modify them
			a.tokenMap.delete(token)
			return nil, fmt.Errorf("%w [err: %w]: %s", tok.ErrInvalidToken, tok.ErrTokenExpired, token)
		}
		return claims, nil
	}
	return a.cacheNewToken(token)
}

// Take a token string, validate the signature and claims, and add to the authManager cache
func (a *authManager) cacheNewToken(token string) (claims *tok.AISClaims, err error) {
	// Validate token signature and extract
	claims, err = a.tokenParser.ValidateToken(token)
	if err != nil {
		nlog.Infof("Received invalid token, error: %v", err)
		return nil, err
	}
	// We aren't guaranteed someone else didn't do this, but worth a separate attempt to validate outside of lock
	a.tokenMap.set(token, claims)
	return claims, nil
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
		if !p.authn.tokenParser.IsSecretCksumValid(reqConf.Secret) {
			p.writeErrf(w, r, "%s: invalid secret sha256(%q)", p, cos.SHead(reqConf.Secret))
		}
	}
	if reqConf.PubKey != nil {
		valid, err := p.authn.tokenParser.IsPublicKeyValid(*reqConf.PubKey)
		if err != nil {
			p.writeErrf(w, r, "%s: invalid public key (%q)", p, cos.SHead(*reqConf.PubKey))
			return
		}
		if !valid {
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

// Validates a token from the request header.
// Supports both standard Bearer tokens and X-Amz-Security-Token
// as fallback for AWS SDK compatibility.
func (p *proxy) validateToken(hdr http.Header) (*tok.AISClaims, error) {
	tokenHdr, err := tok.ExtractToken(hdr)
	if err != nil {
		return nil, err
	}
	claims, err := p.authn.validateToken(tokenHdr.Token)
	if err != nil {
		nlog.Errorf("invalid token from header %q: %v ", tokenHdr.Header, err)
		return nil, err
	}
	return claims, nil
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
		claims *tok.AISClaims
		bucket *cmn.Bck
	)
	if p.checkIntraCall(hdr, false /*from primary*/) == nil {
		return nil
	}
	if cmn.Rom.AuthEnabled() { // config.Auth.Enabled
		claims, err = p.validateToken(hdr)
		if err != nil {
			// NOTE: making exception to allow 3rd party clients read remote ht://bucket
			if errors.Is(err, tok.ErrNoToken) && bck != nil && bck.IsHT() {
				err = nil
			}
			return err
		}
		uid := p.owner.smap.Get().UUID
		if bck != nil {
			bucket = bck.Bucket()
		}
		if err = claims.CheckPermissions(uid, bucket, ace); err != nil {
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
	} else if claims != nil && claims.IsAdmin {
		ace &^= (apc.AcePATCH | apc.AceBckSetACL)
	}
	if ace == 0 {
		return nil
	}
	return bck.Allow(ace)
}

/////////////////////
// shardedTokenMap //
/////////////////////

func newShardedTokenMap(exponent int) *shardedTokenMap {
	// Force size to be a power of 2 so we can use bitmask
	size := 1 << exponent
	shardedMap := &shardedTokenMap{
		shards: make([]*tokenMap, size),
	}
	for i := range size {
		shardedMap.shards[i] = newTokenMap()
	}
	return shardedMap
}

func (sm *shardedTokenMap) getClaims(token string) (*tok.AISClaims, bool) {
	m := sm.shards[sm.getShardIndex(token)]
	m.RLock()
	claims, ok := m.tokens[token]
	m.RUnlock()
	return claims, ok
}

func (sm *shardedTokenMap) set(token string, claims *tok.AISClaims) {
	m := sm.shards[sm.getShardIndex(token)]
	m.Lock()
	m.tokens[token] = claims
	m.Unlock()
}

func (sm *shardedTokenMap) delete(token string) {
	m := sm.shards[sm.getShardIndex(token)]
	m.Lock()
	delete(m.tokens, token)
	m.Unlock()
}

func (sm *shardedTokenMap) deleteMultiple(tokens []string) {
	if len(tokens) == 0 {
		return
	}
	// Map from shard index to slice of tokens to delete in that shard
	shardTokens := make(map[int][]string)
	for _, token := range tokens {
		idx := sm.getShardIndex(token)
		shardTokens[idx] = append(shardTokens[idx], token)
	}

	// Delete from each touched shard
	for idx, tkList := range shardTokens {
		tm := sm.shards[idx]
		tm.Lock()
		for _, token := range tkList {
			delete(tm.tokens, token)
		}
		tm.Unlock()
	}
}

func (sm *shardedTokenMap) getShardIndex(token string) int {
	tkHash := onexxh.ChecksumString64(token)
	// Pick a shard by bitmasking against the total number (power of 2)
	return int(tkHash & uint64(len(sm.shards)-1))
}

//////////////
// tokenMap //
//////////////

func newTokenMap() *tokenMap {
	return &tokenMap{
		tokens: make(map[string]*tok.AISClaims),
	}
}

//////////////////////
// RevokedTokensMap //
//////////////////////

func newRevokedTokensMap() *RevokedTokensMap {
	return &RevokedTokensMap{
		revokedTokens: make(map[string]bool),
		version:       1,
	}
}

func (r *RevokedTokensMap) update(newRevoked *tokenList) error {
	// Lock over the whole operation as we must verify the final updated version matches the version number
	r.Lock()
	defer r.Unlock()
	err := r.updateVersion(newRevoked)
	if err != nil {
		return err
	}
	for _, token := range newRevoked.Tokens {
		r.revokedTokens[token] = true
	}
	return nil
}

// Must be called under lock
func (r *RevokedTokensMap) updateVersion(newRevoked *tokenList) error {
	currentVersion := r.version
	switch {
	case newRevoked.Version == 0:
		r.version++
		return nil
	case newRevoked.Version > currentVersion:
		r.version = newRevoked.Version
		return nil
	case newRevoked.Version == currentVersion:
		nlog.Warningf("received token list v%d equal to current token list v%d, ignoring", newRevoked.Version, currentVersion)
	default:
		nlog.Errorf("received token list v%d less than current token list v%d", newRevoked.Version, currentVersion)
	}
	return fmt.Errorf("received invalid token list version v%d compared to current token list v%d", newRevoked.Version, currentVersion)
}

func (r *RevokedTokensMap) cleanup(tkParser tok.Parser) (allRevoked *tokenList) {
	r.Lock()
	defer r.Unlock()
	allRevoked = &tokenList{
		Tokens:  make([]string, 0, len(r.revokedTokens)),
		Version: r.version,
	}

	// Clean up expired tokens from the revoked list.
	for token := range r.revokedTokens {
		_, err := tkParser.ValidateToken(token)
		switch {
		case errors.Is(err, tok.ErrTokenExpired):
			delete(r.revokedTokens, token)
		case err == nil:
			allRevoked.Tokens = append(allRevoked.Tokens, token)
		default:
			// Keep tokens if error validating that's not expiration
			// We don't want to un-revoke this token in case expected claims revert to previous
			allRevoked.Tokens = append(allRevoked.Tokens, token)
			nlog.Errorf("Unexpected token validation error: %v (token: %s)", err, token)
		}
	}
	if len(allRevoked.Tokens) == 0 {
		allRevoked = nil
	}
	return allRevoked
}

func (r *RevokedTokensMap) contains(token string) bool {
	r.RLock()
	_, ok := r.revokedTokens[token]
	r.RUnlock()
	return ok
}

func (r *RevokedTokensMap) getAll() *tokenList {
	r.RLock()
	defer r.RUnlock()
	l := len(r.revokedTokens)
	if l == 0 {
		return nil
	}
	allRevoked := &tokenList{Tokens: make([]string, 0, l), Version: r.version}
	for token := range r.revokedTokens {
		allRevoked.Tokens = append(allRevoked.Tokens, token)
	}
	return allRevoked
}
