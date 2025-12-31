// Package tok provides AuthN token (structure and methods)
// for validation by AIS gateways
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package tok

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
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
		// Exposed config options for the JWK client and cache
		cacheConfig *CacheConfig
		// Optional stats tracker when used by AIS nodes
		statsT stats.Tracker
	}

	keyCache struct {
		// Map of iss URL to JWKS URL
		issuerToJWKS map[string]string
		// Cache for key sets
		jwksCache *jwk.Cache
		// Lock to allow for dynamic updates
		// All key cache fields should be updated within the same lock
		sync.RWMutex
	}

	CacheConfig struct {
		DiscoveryConf           *DiscoveryConf
		MinCacheRefreshInterval *time.Duration
	}

	DiscoveryConf struct {
		Retries   int
		BaseDelay time.Duration
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

var ErrNoJWKSForIssuer = errors.New("no JWKS entry exists for issuer")

const (
	defaultMinCacheRefreshInterval = 15 * time.Minute
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
func NewKeyCacheManager(oidc *cmn.OIDCConf, client *http.Client, cacheConf *CacheConfig, statsT stats.Tracker) *KeyCacheManager {
	return &KeyCacheManager{
		allowedIss:  newAllowSet(oidc),
		jwksClient:  client,
		cacheConfig: newCacheConfig(cacheConf),
		statsT:      statsT,
	}
}

func newAllowSet(oidc *cmn.OIDCConf) map[string]struct{} {
	if oidc == nil {
		return make(map[string]struct{})
	}
	return oidc.GetAllowedIssSet()
}

func newCacheConfig(conf *CacheConfig) *CacheConfig {
	if conf == nil {
		conf = &CacheConfig{}
	}
	if conf.DiscoveryConf == nil {
		conf.DiscoveryConf = &DiscoveryConf{
			Retries:   defaultOIDCDiscoveryRetries,
			BaseDelay: defaultOIDCDiscoveryRetryDelay,
		}
	}
	if conf.MinCacheRefreshInterval == nil {
		conf.MinCacheRefreshInterval = apc.Ptr(defaultMinCacheRefreshInterval)
	}
	return conf
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
		jwksCache:    jwk.NewCache(rootCtx),
		issuerToJWKS: make(map[string]string, len(km.allowedIss)),
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
		jwk.WithMinRefreshInterval(*km.cacheConfig.MinCacheRefreshInterval),
	}
	err = km.keyCache.registerIss(iss, jwksURL, regOpts)
	if err != nil {
		nlog.Errorf("iss %s err: %v", iss, err)
		return err
	}
	return nil
}

func (km *KeyCacheManager) getKeyForToken(ctx context.Context, tok *jwt.Token) (any, error) {
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
	kid, err := getKeyID(tok)
	if err != nil {
		return nil, fmt.Errorf("invalid 'kid' header -- required for fetching public key from issuer: %w", err)
	}
	return km.getPubKey(ctx, iss, kid)
}

func getKeyID(tok *jwt.Token) (string, error) {
	kid, ok := tok.Header["kid"].(string)
	if !ok || kid == "" {
		return "", errors.New("header 'kid' missing")
	}
	// Validate kid to prevent injection attacks
	if strings.ContainsAny(kid, "/\\|;&$<>`\"'()") {
		return "", errors.New("invalid characters in 'kid' header")
	}
	if len(kid) > maxKidLength {
		return "", errors.New("'kid' header too long")
	}
	return kid, nil
}

func (km *KeyCacheManager) getPubKey(ctx context.Context, iss, kid string) (any, error) {
	keySet, err := km.getKeySetFromCache(ctx, iss)
	if err != nil {
		km.IncCounter(stats.AuthInvalidIssCount)
		return nil, fmt.Errorf("failed to get keyset for issuer %s: %v", iss, err)
	}
	jwKey, found := keySet.LookupKeyID(kid)
	if !found {
		km.IncCounter(stats.AuthInvalidKidCount)
		return nil, fmt.Errorf("key with kid %s not found for issuer %s", kid, iss)
	}
	var pubKey any
	if err = jwKey.Raw(&pubKey); err != nil {
		return nil, fmt.Errorf("failed to parse public key [iss: %s, kid: %s, err: %w]", iss, kid, err)
	}
	return pubKey, nil
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
		attempts  = km.cacheConfig.DiscoveryConf.Retries + 1
		baseDelay = km.cacheConfig.DiscoveryConf.BaseDelay
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
			return "", fmt.Errorf("invalid issuer URL received from discovery %s: %w", issuer, urlErr)
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
		return "", fmt.Errorf("jwks_uri not found in metadata for issuer %s", issuer)
	}
	return meta.JWKSURI, nil
}

//////////////
// keyCache //
//////////////

// Best-effort attempt to refresh all cached JWKS
func (kc *keyCache) preLoadAll(ctx context.Context) {
	// Snapshot issuerToJWKS under lock
	kc.RLock()
	issuerToJWKS := maps.Clone(kc.issuerToJWKS)
	kc.RUnlock()

	// Refresh all JWKS in parallel
	var wg sync.WaitGroup
	for iss, jwksURL := range issuerToJWKS {
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
	kc.issuerToJWKS[iss] = jwksURL
	return nil
}

func (kc *keyCache) getKeySetForIss(ctx context.Context, iss string) (jwk.Set, error) {
	if kc.jwksCache == nil {
		return nil, errors.New("jwks cache not initialized")
	}
	kc.RLock()
	defer kc.RUnlock()
	jwksURL, ok := kc.issuerToJWKS[iss]
	if !ok {
		return nil, ErrNoJWKSForIssuer
	}
	return kc.jwksCache.Get(ctx, jwksURL)
}
