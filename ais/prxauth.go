// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"

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
		// for canceling internal long-lived context
		cancelCtx context.CancelFunc
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
const (
	TokenMapShardExponent  = 4
	TokenParserInitTimeout = 10 * time.Second
)

// Client defaults for issuer requests
// Used for JWKS URL discovery and fetching
// See cmn/client.go
const (
	// KeyCacheDialTimeout and KeyCacheTimeout are set for faster proxy startup in case of an unresponsive issuer service
	KeyCacheDialTimeout = 5 * time.Second
	KeyCacheTimeout     = 10 * time.Second

	// KeyCacheIdleConnsPerHost overrides AIS client settings to match http.Transport.DefaultMaxIdleConnsPerHost
	// Because of cached key sets, we don't expect to need idle connections to the same issuer host
	KeyCacheIdleConnsPerHost = 2
	// KeyCacheMaxIdleConnsLimit caps the maximum idle conns based on number of allowed issuers
	KeyCacheMaxIdleConnsLimit = 16
)

/////////////////
// authManager //
/////////////////

func newAuthManager(config *cmn.Config, statsT stats.Tracker) *authManager {
	debug.Assert(g.netServ.pub != nil)

	rootCtx, rootCancel := context.WithCancel(context.Background())
	keyCacheClient := newKeyCacheClient(config, statsT)
	keyCacheManager := tok.NewKeyCacheManager(config.Auth.OIDC, keyCacheClient, nil, statsT)
	keyCacheManager.Init(rootCtx)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), TokenParserInitTimeout)
	defer cancel()
	if err := keyCacheManager.PopulateJWKSCache(timeoutCtx); err != nil {
		nlog.Errorf("Errors occurred while pre-populating JWKS key cache: %v", err)
	}
	return &authManager{
		tokenParser:   tok.NewTokenParser(&config.Auth, keyCacheManager),
		tokenMap:      newShardedTokenMap(TokenMapShardExponent),
		revokedTokens: newRevokedTokensMap(),
		cancelCtx:     rootCancel,
	}
}

// Define the client used by the key cache manager for contacting token issuers
func newKeyCacheClient(config *cmn.Config, statsT stats.Tracker) *http.Client {
	var tls cmn.TLSArgs
	// Set our own certificate, key, and verification settings from AIS network config
	if config.Net.HTTP.UseHTTPS {
		tls = config.Net.HTTP.ToTLS()
	} else {
		tls = cmn.TLSArgs{
			SkipVerify: false,
		}
	}
	// Override the trusted CA for issuers if location provided
	if config.Auth.OIDC != nil && config.Auth.OIDC.IssuerCA != "" {
		tls.ClientCA = config.Auth.OIDC.IssuerCA
	}
	// Key cache client should never need more idle connections than configured issuers
	// Set MaxIdleConns to limit the idle connections kept to issuers when we don't expect any subsequent requests
	maxIdleConns := KeyCacheIdleConnsPerHost
	if config.Auth.OIDC != nil && len(config.Auth.OIDC.AllowedIssuers) > maxIdleConns {
		maxIdleConns = min(len(config.Auth.OIDC.AllowedIssuers), KeyCacheMaxIdleConnsLimit)
	}

	transport := cmn.TransportArgs{
		DialTimeout:      KeyCacheDialTimeout,
		Timeout:          KeyCacheTimeout,
		IdleConnsPerHost: KeyCacheIdleConnsPerHost,
		MaxIdleConns:     maxIdleConns,
		PreferIPv6:       g.netServ.pub.useIPv6,
	}
	client := cmn.NewClientTLS(transport, tls, false)

	// Wrap the transport with a custom type to inject stats tracking
	if statsT != nil {
		client.Transport = tok.NewJWKSRoundTripper(client.Transport, statsT)
	}
	return client
}

func (a *authManager) stop() {
	a.cancelCtx()
}

// Add tokens to the list of invalid ones and clean up the list from expired tokens.
func (a *authManager) updateRevokedList(ctx context.Context, newRevoked *tokenList) (allRevoked *tokenList) {
	// Add new revoked tokens -- error if invalid version
	err := a.revokedTokens.update(newRevoked)
	if err != nil {
		return nil
	}
	// Remove revoked tokens from the token cache
	a.tokenMap.deleteMultiple(newRevoked.Tokens)
	// Clean up any expired tokens from the revoked list
	return a.revokedTokens.cleanup(ctx, a.tokenParser)
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
func (a *authManager) validateToken(ctx context.Context, token string) (*tok.AISClaims, error) {
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
	return a.cacheNewToken(ctx, token)
}

// Take a token string, validate the signature and claims, and add to the authManager cache
func (a *authManager) cacheNewToken(ctx context.Context, token string) (claims *tok.AISClaims, err error) {
	// Validate token signature and extract
	claims, err = a.tokenParser.ValidateToken(ctx, token)
	if err != nil {
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

// is called when authentication is being enabled at runtime to guard the transition
//
//	auth.enabled: false -> true
//
// - if the caller has a valid token under the current config, enabling auth is allowed unconditionally
// - otherwise, we fail with one of the specific reasons (below)
// - note that enabling cluster-key signing (auth.cluster_key.*) is handled separately

func (p *proxy) validateEnableAuth(r *http.Request, clone *cmn.AuthConf, toUpdate *cmn.AuthConfToSet) (int, error) {
	if clone.Enabled || toUpdate == nil || toUpdate.Enabled == nil || !*toUpdate.Enabled {
		debug.Assertf(false, "%v %+v", clone.Enabled, toUpdate)
		return 0, nil
	}

	claims, err := p.validateToken(r.Context(), r.Header)
	if err != nil {
		return http.StatusUnauthorized, fmt.Errorf("enabling JWT/OIDC auth requires a valid token [err: %v]", err)
	}
	if !claims.IsAdmin {
		return http.StatusUnauthorized, errors.New("enabling JWT/OIDC auth requires a token with an 'admin' claim")
	}

	// apply and check
	if e := cmn.CopyProps(toUpdate, clone, apc.Cluster); e != nil {
		return 0, e
	}

	// validate config
	err = clone.Validate()
	if err != nil {
		return http.StatusUnauthorized, fmt.Errorf("enabling JWT/OIDC auth requires valid config (%s)", err)
	}
	return 0, nil
}

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
	allRevoked := p.authn.updateRevokedList(r.Context(), tokenList)
	if allRevoked != nil && p.owner.smap.get().isPrimary(p.si) {
		msg := p.newAmsgStr(apc.ActNewPrimary, nil)
		_ = p.metasyncer.sync(revsPair{allRevoked, msg})
	}
}

// Validate the token found in the given header, return claims, and update metrics
// Returned claims will always be non-nil if no error is returned
func (p *proxy) validateToken(ctx context.Context, hdr http.Header) (*tok.AISClaims, error) {
	claims, err := p.extractAndValidate(ctx, hdr)
	p.statsT.Inc(stats.AuthTotalCount)

	if err != nil {
		p.statsT.Inc(stats.AuthFailCount)
		switch {
		case errors.Is(err, tok.ErrNoToken):
			p.statsT.Inc(stats.AuthNoTokenCount)
		case errors.Is(err, tok.ErrInvalidToken):
			p.statsT.Inc(stats.AuthInvalidTokenCount)
		case errors.Is(err, tok.ErrTokenExpired):
			p.statsT.Inc(stats.AuthExpiredTokenCount)
		}
	} else {
		p.statsT.Inc(stats.AuthSuccessCount)
	}
	return claims, err
}

// Validates a token from the request header.
// Supports both standard Bearer tokens and X-Amz-Security-Token
// as fallback for AWS SDK compatibility.
func (p *proxy) extractAndValidate(ctx context.Context, hdr http.Header) (*tok.AISClaims, error) {
	tokenHdr, err := tok.ExtractToken(hdr)
	if err != nil {
		return nil, err
	}
	claims, err := p.authn.validateToken(ctx, tokenHdr.Token)
	if err != nil {
		return nil, fmt.Errorf("invalid token from header %q: %w", tokenHdr.Header, err)
	}
	if claims == nil {
		return nil, fmt.Errorf("token from header %q has no claims: %w", tokenHdr.Header, tok.ErrInvalidToken)
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
	if err = p.access(r.Context(), r.Header, bck, ace); err != nil {
		// Use writeErrMsg (with the combined message from wrapped errors) instead of writeErr
		// aceErrToCode parses code from the type so additional status code parsing is not necessary
		p.writeErrMsg(w, r, err.Error(), aceErrToCode(err))
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

// Validate the given header contains a token allowing access to the given bucket with the requested permissions
// All failures must be logged at this level
func (p *proxy) access(ctx context.Context, hdr http.Header, bck *meta.Bck, ace apc.AccessAttrs) (err error) {
	// Skip internal calls
	if p.checkIntraCall(hdr, false /*from primary*/) == nil {
		return nil
	}

	// If auth is NOT enabled, only check bucket properties
	if !cmn.Rom.AuthEnabled() {
		if bck == nil || bck.Props == nil {
			return nil
		}
		// With Auth disabled, always allow read-only access, PATCH, and ACL
		ace &^= apc.AcePATCH | apc.AceBckSetACL | apc.AccessRO
		err = bck.Allow(ace)
		if err != nil {
			nlog.Warningln("bucket access check failed:", err)
		}
		return err
	}

	// System buckets: require admin
	// - note that majority of control flows that operate on bucket(s) validate perm-s via bckArgs.initAndTry() => p.access()
	// - the rest that pass `bck == nil` MUST explicitly add apc.AceAdmin
	if bck != nil && bck.Bucket().IsSystem() {
		ace |= apc.AceAdmin
	}

	// Validate token and parse claims ONCE
	claims, err := p.validateToken(ctx, hdr)
	if err != nil {
		// NOTE: making exception to allow 3rd party clients read remote ht://bucket
		if errors.Is(err, tok.ErrNoToken) && bck != nil && bck.IsHT() {
			err = nil
		} else {
			nlog.Warningln("token validation failed:", err)
		}
		return err
	}
	return p.checkTokenAccess(claims, bck, ace)
}

func (p *proxy) checkTokenAccess(claims *tok.AISClaims, bck *meta.Bck, ace apc.AccessAttrs) (err error) {
	if bck == nil {
		err = p.checkClaimPermissions(claims, nil, ace)
		if err != nil {
			nlog.Warningln("cluster access check failed:", err)
		}
	} else {
		err = p.checkBucketAccess(claims, bck, ace)
		if err != nil {
			nlog.Warningln("bucket access check failed:", err)
		}
	}
	p.statsT.Inc(stats.ACLTotalCount)
	if err != nil {
		p.statsT.Inc(stats.ACLDeniedCount)
	}
	return err
}

// checkClaimPermissions validates claims have the required permissions
func (p *proxy) checkClaimPermissions(claims *tok.AISClaims, bucket *cmn.Bck, ace apc.AccessAttrs) error {
	if claims == nil {
		return tok.ErrInvalidToken
	}
	uid := p.owner.smap.Get().UUID
	return claims.CheckPermissions(uid, bucket, ace)
}

func (p *proxy) checkBucketAccess(claims *tok.AISClaims, bck *meta.Bck, ace apc.AccessAttrs) error {
	err := p.checkClaimPermissions(claims, bck.Bucket(), ace)
	if err != nil {
		return err
	}
	// If an admin, bucket properties for access still apply, but admin can always patch and set ACL
	if claims.IsAdmin {
		ace &^= apc.AcePATCH | apc.AceBckSetACL
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

func (r *RevokedTokensMap) cleanup(ctx context.Context, tkParser tok.Parser) (allRevoked *tokenList) {
	r.Lock()
	defer r.Unlock()
	allRevoked = &tokenList{
		Tokens:  make([]string, 0, len(r.revokedTokens)),
		Version: r.version,
	}

	// Clean up expired tokens from the revoked list.
	for token := range r.revokedTokens {
		_, err := tkParser.ValidateToken(ctx, token)
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
