// Package tok provides AuthN token (structure and methods)
// for validation by AIS gateways
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tok

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/stats"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

type (
	KeyCacheManager struct {
		// Configured allowed issuers
		allowedIss map[string]struct{}
		// Manages all cached JWKS
		keyCache *keyCache
		// Client for looking up JWKS URI and caching key sets
		jwksClient *http.Client
		// Config options for the JWK client cache
		cacheConfig *cacheConfig
		// Optional stats tracker when used by AIS nodes
		statsT stats.Tracker
	}

	KCMConfig struct {
		// User-exposed config for key validation with OIDC issuer lookup
		cmn.OIDCConf
		// Configuration for issuer discovery URL used to find JWKS URL
		Discovery *DiscoveryConf
	}

	DiscoveryConf struct {
		// Number of retry attempts for the discovery URL of an allowed issuer
		Retries *int
		// Base for exponential backoff when retrying issuer discovery
		BaseDelay *time.Duration
	}

	keyCache struct {
		// Per-issuer state keyed by issuer URL
		issuers map[string]*issuerEntry
		// Cache for key sets
		jwksCache *jwk.Cache
		// Minimum interval between explicit refreshes per issuer
		minRotationRefresh time.Duration
		// Lock to allow for dynamic updates
		sync.RWMutex
	}

	issuerEntry struct {
		sync.Mutex
		jwksURL            string
		lastRefreshAttempt time.Time
	}

	cacheConfig struct {
		discoveryRetries     int
		discoveryBaseDelay   time.Duration
		minBackgroundRefresh time.Duration // floor for background auto-refresh polling
		minRotationRefresh   time.Duration // throttle for explicit refresh on kid miss
	}

	discoverResp struct {
		JWKSURI string `json:"jwks_uri"`
	}

	// JWKSRoundTripper wraps http.RoundTripper to track latency of JWKS fetches
	JWKSRoundTripper struct {
		base   http.RoundTripper
		statsT stats.Tracker
	}
)

var (
	ErrNoJWKSForIssuer           = errors.New("no JWKS entry exists for issuer")
	ErrJWKSCacheNotInitialized   = errors.New("JWKS cache not initialized")
	ErrJWKSCacheRefreshThrottled = errors.New("JWKS cache refresh throttled")
)

const (
	defaultMinBackgroundRefresh    = 15 * time.Minute
	defaultMinRotationRefresh      = 30 * time.Second
	defaultOIDCDiscoveryRetries    = 3
	defaultOIDCDiscoveryRetryDelay = cos.PollSleepShort
	oidcDiscoveryEndpoint          = "/.well-known/openid-configuration"
	maxKidLength                   = 256
	keyFetchURLJWK                 = "/jwk"
	keyFetchURLKey                 = "/key"
	keyFetchURLCert                = "/cert"
)

// NewKeyCacheManager creates an instance of KeyCacheManager with an unpopulated cache
// After creating, call Init with a long-lived context to create a key cache
// Optionally, also pre-populate the cache to register and preload the allowed issuers
func NewKeyCacheManager(conf *KCMConfig, client *http.Client, statsT stats.Tracker) *KeyCacheManager {
	return &KeyCacheManager{
		allowedIss:  newAllowSet(conf),
		jwksClient:  client,
		cacheConfig: newCacheConfig(conf),
		statsT:      statsT,
	}
}

func newAllowSet(conf *KCMConfig) map[string]struct{} {
	if conf == nil {
		return make(map[string]struct{})
	}
	return conf.GetAllowedIssSet()
}

func newCacheConfig(conf *KCMConfig) *cacheConfig {
	cc := &cacheConfig{
		discoveryRetries:     defaultOIDCDiscoveryRetries,
		discoveryBaseDelay:   defaultOIDCDiscoveryRetryDelay,
		minBackgroundRefresh: defaultMinBackgroundRefresh,
		minRotationRefresh:   defaultMinRotationRefresh,
	}
	if conf == nil {
		return cc
	}
	if conf.JWKSCacheConf != nil {
		if conf.JWKSCacheConf.MinBackgroundRefresh != 0 {
			cc.minBackgroundRefresh = conf.JWKSCacheConf.MinBackgroundRefresh.D()
		}
		if conf.JWKSCacheConf.MinRotationRefresh != 0 {
			cc.minRotationRefresh = conf.JWKSCacheConf.MinRotationRefresh.D()
		}
	}
	if conf.Discovery != nil {
		if conf.Discovery.Retries != nil {
			cc.discoveryRetries = *conf.Discovery.Retries
		}
		if conf.Discovery.BaseDelay != nil {
			cc.discoveryBaseDelay = *conf.Discovery.BaseDelay
		}
	}
	return cc
}

func NewJWKSRoundTripper(base http.RoundTripper, statsT stats.Tracker) *JWKSRoundTripper {
	return &JWKSRoundTripper{
		base:   base,
		statsT: statsT,
	}
}

// RoundTrip implements http.RoundTripper with additional stats wrapping
func (jrt *JWKSRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	started := time.Now()
	resp, err := jrt.base.RoundTrip(req)
	elapsed := time.Since(started)
	// Specific histogram for key fetches.
	// Not comprehensive but includes commonly used `.well-known/jwks.json`,`/oauth2/v1/keys`,`/openid-connect/certs`
	if strings.Contains(req.URL.Path, keyFetchURLJWK) ||
		strings.Contains(req.URL.Path, keyFetchURLKey) ||
		strings.Contains(req.URL.Path, keyFetchURLCert) {
		jrt.statsT.Observe(stats.AuthJWKSHist, elapsed.Seconds())
	} else {
		jrt.statsT.Observe(stats.AuthIssHist, elapsed.Seconds())
	}
	return resp, err
}

func (km *KeyCacheManager) isInitialized() bool {
	return km.keyCache != nil
}

// Init prepares a key cache manager to provide to a token parser
// The provided context must be valid for the life of the cache for automatic refresh
func (km *KeyCacheManager) Init(rootCtx context.Context) {
	km.keyCache = &keyCache{
		jwksCache:          jwk.NewCache(rootCtx),
		issuers:            make(map[string]*issuerEntry, len(km.allowedIss)),
		minRotationRefresh: km.cacheConfig.minRotationRefresh,
	}
}

func (km *KeyCacheManager) IncCounter(metric string) {
	if km.statsT != nil {
		km.statsT.Inc(metric)
	}
}

// PopulateJWKSCache looks up JWKS URLs, adds them to the cache, and preloads JWKS
// Returns error only on context cancellation or invalid config
func (km *KeyCacheManager) PopulateJWKSCache(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(km.allowedIss))

	for iss := range km.allowedIss {
		wg.Add(1)
		go func(iss string) {
			defer wg.Done()
			select {
			// Check for context cancellation
			case <-ctx.Done():
				nlog.Errorf("key cache initialization canceled for issuer %s", iss)
				return
			default:
				// Issuer URL is invalid, so record error as we never expect it to work
				discoveryURL, err := getDiscoveryURL(iss)
				if err != nil {
					errCh <- fmt.Errorf("failed to get discovery URL for issuer %s: %w", iss, err)
					return
				}
				// Ignore errors from individual issuers with valid URLs
				// Errors are logged internally and registration will retry later on request
				_ = km.registerIssWithCache(ctx, iss, discoveryURL)
			}
		}(iss)
	}
	wg.Wait()
	close(errCh)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var errs []error
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}

	km.keyCache.preLoadAll(ctx)
	return errors.Join(errs...)
}

// Register a given issuer to the cache and map the issuer to the JWKS url
func (km *KeyCacheManager) registerIssWithCache(ctx context.Context, iss, discoveryURL string) error {
	jwksURL, err := km.discoverJWKSURI(ctx, iss, discoveryURL)
	if err != nil {
		err = fmt.Errorf("failed to discover JWKS URI: %w", err)
		nlog.Errorf("iss %s err: %v", iss, err)
		return err
	}
	regOpts := []jwk.RegisterOption{
		jwk.WithHTTPClient(km.jwksClient),
		jwk.WithMinRefreshInterval(km.cacheConfig.minBackgroundRefresh),
	}
	err = km.keyCache.registerIss(iss, jwksURL, regOpts)
	if err != nil {
		nlog.Errorf("iss %s err: %v", iss, err)
		return err
	}
	return nil
}

func (km *KeyCacheManager) ResolveKey(ctx context.Context, tok *jwt.Token) (any, error) {
	if !km.isInitialized() {
		return nil, errors.New("cannot validate signature by issuer lookup: jwks cache not initialized")
	}
	if len(km.allowedIss) == 0 {
		return nil, errors.New("no public key or allowed issuers configured to validate token")
	}
	// At this point jwt ParseWithClaims has already parsed the claims, just not verified signature
	claims, ok := tok.Claims.(*AISClaims)
	if !ok {
		return nil, fmt.Errorf("cannot determine issuer due to invalid token claims: %v", tok.Claims)
	}
	iss, err := claims.GetIssuer()
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'iss' claim: %w", err)
	}
	if iss == "" {
		return nil, errors.New("missing 'iss' claim")
	}

	// Allowed issuer check
	_, ok = km.allowedIss[iss]
	if !ok {
		return nil, errors.New("provided 'iss' claim not in configured allowed list")
	}

	// Get a valid key id from token
	kid, err := GetKeyID(tok)
	if err != nil {
		return nil, fmt.Errorf("invalid 'kid' header -- required for fetching public key from issuer: %w", err)
	}
	return km.getPubKey(ctx, iss, kid)
}

// ValidateKey checks if the public key provided in the request struct exists with any configured issuers
func (km *KeyCacheManager) ValidateKey(ctx context.Context, reqConf *authn.ServerConf) (int, error) {
	if reqConf.Secret != "" {
		return http.StatusBadRequest, errors.New("cannot validate provided HMAC secret checksum: AIS cluster is configured for OIDC issuer-based key validation")
	}
	if reqConf.PubKey == nil {
		return http.StatusBadRequest, errors.New("no public key provided to validate")
	}
	reqKey, parseErr := parsePubKey(*reqConf.PubKey)
	if parseErr != nil {
		return http.StatusBadRequest, fmt.Errorf("invalid public key: %w", parseErr)
	}
	if !km.isInitialized() {
		return http.StatusInternalServerError, errors.New("JWKS cache not initialized")
	}
	for iss := range km.allowedIss {
		keySet, err := km.getKeySetFromCache(ctx, iss)
		if err != nil {
			nlog.Warningf("OIDC public key validation skipping issuer %s: %v", iss, err)
			continue
		}
		for it := keySet.Keys(ctx); it.Next(ctx); {
			key := it.Pair().Value.(jwk.Key)
			var rawKey any
			if keyErr := key.Raw(&rawKey); keyErr != nil {
				continue
			}
			if rsaPub, ok := rawKey.(*rsa.PublicKey); ok && rsaPub.Equal(reqKey) {
				return 0, nil
			}
		}
	}
	return http.StatusForbidden, errors.New("provided public key not found in any allowed issuer's JWKS")
}

func (km *KeyCacheManager) getPubKey(ctx context.Context, iss, kid string) (any, error) {
	keySet, err := km.getKeySetFromCache(ctx, iss)
	if err != nil {
		km.IncCounter(stats.AuthInvalidIssCount)
		return nil, fmt.Errorf("failed to get keyset for issuer %s: %v", iss, err)
	}
	jwKey, err := km.lookupKey(ctx, keySet, iss, kid)
	if err != nil {
		return nil, fmt.Errorf("unrecognized key id %s for issuer %s: %v", kid, iss, err)
	}
	var pubKey any
	if err = jwKey.Raw(&pubKey); err != nil {
		return nil, fmt.Errorf("failed to parse public key [iss: %s, kid: %s, err: %w]", iss, kid, err)
	}
	return pubKey, nil
}

func (km *KeyCacheManager) lookupKey(ctx context.Context, keySet jwk.Set, iss, kid string) (jwk.Key, error) {
	jwKey, found := keySet.LookupKeyID(kid)
	if found {
		return jwKey, nil
	}
	// Key not found — attempt to refresh the issuer's key set
	jwKey, err := km.refreshAndLookup(ctx, iss, kid)
	if err != nil {
		km.IncCounter(stats.AuthInvalidKidCount)
	}
	return jwKey, err
}

func (km *KeyCacheManager) refreshAndLookup(ctx context.Context, iss, kid string) (jwk.Key, error) {
	lookupSet, err := km.keyCache.refreshKeySetForIss(ctx, iss)
	if err != nil && !errors.Is(err, ErrJWKSCacheRefreshThrottled) {
		return nil, err
	}
	// If throttled, retry get in case another routine refreshed between check and lock acquisition
	if errors.Is(err, ErrJWKSCacheRefreshThrottled) {
		var getErr error
		lookupSet, getErr = km.getKeySetFromCache(ctx, iss)
		if getErr != nil {
			return nil, errors.Join(err, getErr)
		}
	}
	if jwKey, found := lookupSet.LookupKeyID(kid); found {
		return jwKey, nil
	}
	return nil, errors.Join(err, errors.New("kid not found after JWKS refresh attempt"))
}

func (km *KeyCacheManager) getKeySetFromCache(ctx context.Context, iss string) (jwk.Set, error) {
	keySet, err := km.keyCache.getKeySetForIss(ctx, iss)
	if err == nil {
		return keySet, nil
	}
	// Only continue to dynamic registration and retry if the issuer has not been registered
	if !errors.Is(err, ErrNoJWKSForIssuer) {
		return nil, err
	}
	return km.getUnregisteredKeySet(ctx, iss)
}

// Attempt dynamic registration of an issuer that's already confirmed to be in the allowed list
// Note this is susceptible to races if functionality is added to allow updating the allowed list
func (km *KeyCacheManager) getUnregisteredKeySet(ctx context.Context, iss string) (jwk.Set, error) {
	nlog.Infof("Performing dynamic auth issuer registration for issuer %s", iss)
	// Fail if the URL is invalid
	discoveryURL, err := getDiscoveryURL(iss)
	if err != nil {
		return nil, err
	}
	regErr := km.registerIssWithCache(ctx, iss, discoveryURL)
	if regErr != nil {
		return nil, fmt.Errorf("failed dynamic issuer registration: %w", regErr)
	}
	// Retry fetching the keySet once initialized
	return km.keyCache.getKeySetForIss(ctx, iss)
}

func getDiscoveryURL(issuer string) (string, error) {
	// Parse and validate the issuer URL
	err := cmn.ValidateIssuerURL(issuer)
	if err != nil {
		return "", fmt.Errorf("invalid issuer URL %s: %w", issuer, err)
	}
	base := strings.TrimSuffix(issuer, "/")
	return base + oidcDiscoveryEndpoint, nil
}

func (km *KeyCacheManager) discoverJWKSURI(ctx context.Context, issuer, discoveryURL string) (string, error) {
	var (
		attempts  = km.cacheConfig.discoveryRetries + 1
		baseDelay = km.cacheConfig.discoveryBaseDelay
		lastErr   error
	)
	for attempt := range attempts {
		if attempt > 0 {
			expDelay := baseDelay * time.Duration(1<<attempt)
			backoff := applyJitter(expDelay)
			nlog.Infof("JWKS discovery retry %d for issuer %s: backing off for %v", attempt+1, issuer, backoff)
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("context canceled during JWKS discovery: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}
		jwksURL, discoveryErr := km.makeDiscoveryRequest(ctx, discoveryURL, issuer)
		if discoveryErr != nil {
			nlog.Warningf("Failed to discover JWKS URI for issuer %s: %v", issuer, discoveryErr)
			lastErr = discoveryErr
			continue
		}
		urlErr := cmn.ValidateIssuerURL(jwksURL)
		if urlErr != nil {
			return "", fmt.Errorf("invalid JWKS URL received from discovery for issuer %s: %w", issuer, urlErr)
		}
		nlog.Infof("Registered JWKS URL %q for configured issuer %s", jwksURL, issuer)
		return jwksURL, nil
	}
	return "", fmt.Errorf("unable to discover jwks_uri for issuer %s after %d attempts: %w", issuer, attempts, lastErr)
}

func applyJitter(d time.Duration) time.Duration {
	// AND current timestamp with 111 and subtract 4 to get a pseudo-random int from -4 to 3
	n := (time.Now().UnixNano() & 0x7) - 4
	// Divide the delay by 16 and multiply it by our random number to get some random jitter -25 < x < 18.75%
	jitter := int64(d>>4) * n
	return d + time.Duration(jitter)
}

func (km *KeyCacheManager) makeDiscoveryRequest(ctx context.Context, reqURL, issuer string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("failed to create jwks discovery request for issuer %s: %w", issuer, err)
	}
	resp, err := km.jwksClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("unexpected status code from OIDC discovery for issuer %s: %d", issuer, resp.StatusCode)
	}
	var meta discoverResp
	err = json.NewDecoder(resp.Body).Decode(&meta)
	if err != nil {
		return "", err
	}
	if meta.JWKSURI == "" {
		return "", fmt.Errorf("missing jwks_uri in issuer %s metadata", issuer)
	}
	return meta.JWKSURI, nil
}

//////////////
// keyCache //
//////////////

// Best-effort attempt to refresh all cached JWKS
func (kc *keyCache) preLoadAll(ctx context.Context) {
	kc.RLock()
	snapshot := make(map[string]string, len(kc.issuers))
	for iss, entry := range kc.issuers {
		snapshot[iss] = entry.jwksURL
	}
	kc.RUnlock()

	var wg sync.WaitGroup
	for iss, jwksURL := range snapshot {
		wg.Add(1)
		go func(issuer, url string) {
			defer wg.Done()
			if _, err := kc.jwksCache.Refresh(ctx, url); err != nil {
				nlog.Errorf("failed to preload jwks cache for issuer %s: %v", issuer, err)
			}
		}(iss, jwksURL)
	}
	wg.Wait()
}

// Register a new issuer with the internal cache and update the map from issuer to JWKS url
func (kc *keyCache) registerIss(iss, jwksURL string, regOpts []jwk.RegisterOption) error {
	kc.Lock()
	defer kc.Unlock()
	err := kc.jwksCache.Register(jwksURL, regOpts...)
	if err != nil {
		return fmt.Errorf("failed to register jwksURL in cache: %w", err)
	}
	kc.issuers[iss] = &issuerEntry{jwksURL: jwksURL}
	return nil
}

func (kc *keyCache) getIssEntry(iss string) (*issuerEntry, error) {
	if kc.jwksCache == nil {
		return nil, ErrJWKSCacheNotInitialized
	}
	kc.RLock()
	defer kc.RUnlock()
	entry, ok := kc.issuers[iss]
	if !ok {
		return nil, ErrNoJWKSForIssuer
	}
	return entry, nil
}

func (kc *keyCache) getKeySetForIss(ctx context.Context, iss string) (jwk.Set, error) {
	entry, err := kc.getIssEntry(iss)
	if err != nil {
		return nil, err
	}
	return kc.jwksCache.Get(ctx, entry.jwksURL)
}

func (kc *keyCache) refreshKeySetForIss(ctx context.Context, iss string) (jwk.Set, error) {
	entry, err := kc.getIssEntry(iss)
	if err != nil {
		return nil, err
	}
	// For a given issuer, only one routine should refresh
	entry.Lock()
	defer entry.Unlock()
	now := time.Now()
	if now.Sub(entry.lastRefreshAttempt) < kc.minRotationRefresh {
		return nil, fmt.Errorf("%w: (last attempt %s ago)", ErrJWKSCacheRefreshThrottled, now.Sub(entry.lastRefreshAttempt).Round(time.Second))
	}
	entry.lastRefreshAttempt = now

	keySet, err := kc.jwksCache.Refresh(ctx, entry.jwksURL)
	if err != nil {
		nlog.Warningf("JWKS refresh failed for issuer %s, err: %v", iss, err)
		return nil, err
	}
	return keySet, err
}
