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
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

type (
	KeyCacheManager struct {
		// Configured allowed issuers
		allowedIss map[string]struct{}
		// Map of iss URL to JWKS URL -- Written once on init, read only after
		issuerToJWKS map[string]string
		// Cache for key sets -- Written once on init, read only after
		jwksCache *jwk.Cache
		// Client for looking up JWKS URI and caching key sets
		jwksClient *http.Client
		// Exposed config options for the JWK client and cache
		cacheConfig *CacheConfig

		// For locking during initialization
		once sync.Once
	}

	JWKSClientConf struct {
		TransportArgs *cmn.TransportArgs
		TLSArgs       *cmn.TLSArgs
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

	IssuerCacheError struct {
		Failed map[string]error
	}
)

const (
	defaultMinCacheRefreshInterval = 15 * time.Minute
	defaultOIDCDiscoveryRetries    = 3
	defaultOIDCDiscoveryRetryDelay = 200 * time.Millisecond
	oidcDiscoveryEndpoint          = "/.well-known/openid-configuration"
	maxKidLength                   = 256
)

// NewKeyCacheManager creates an instance of KeyCacheManager with an unpopulated cache
// After creating, call initJWKSCache to register and preload the allowed issuers
func NewKeyCacheManager(oidc *cmn.OIDCConf, clientConf *JWKSClientConf, cacheConf *CacheConfig) *KeyCacheManager {
	return &KeyCacheManager{
		allowedIss:  newAllowSet(oidc),
		jwksClient:  newClient(clientConf),
		cacheConfig: newCacheConfig(cacheConf),
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

func newClient(clientConf *JWKSClientConf) *http.Client {
	if clientConf == nil {
		clientConf = &JWKSClientConf{}
	}
	if clientConf.TransportArgs == nil {
		// Client defaults for looking up the JWKS endpoint from the issuer discovery endpoint
		// See cmn/client.go
		clientConf.TransportArgs = &cmn.TransportArgs{
			DialTimeout: 3 * time.Second,
			Timeout:     5 * time.Second,
			// No need for idle connections to the same issuer host because of cached key sets
			IdleConnsPerHost: 0,
			MaxIdleConns:     1,
		}
	}
	if clientConf.TLSArgs == nil {
		clientConf.TLSArgs = &cmn.TLSArgs{
			SkipVerify: false,
		}
	}
	return cmn.NewClientTLS(*clientConf.TransportArgs, *clientConf.TLSArgs, false)
}

func (km *KeyCacheManager) init(ctx context.Context) error {
	var initErr error
	km.once.Do(func() {
		initErr = km.initJWKSCache(ctx)
	})
	return initErr
}

// Look up JWKS URLs, add them to the cache, and preload JWKS
// Returned error specifically means no issuers succeeded
func (km *KeyCacheManager) initJWKSCache(ctx context.Context) error {
	km.jwksCache = jwk.NewCache(ctx)
	// TODO populate this in parallel with dynamic updates if initial registration fails
	km.issuerToJWKS = make(map[string]string, len(km.allowedIss))
	anySucceeded := false
	// Aggregate errors to allow some issuers to succeed if others fail
	failed := make(map[string]error)
	for iss := range km.allowedIss {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			failed["__context__"] = ctx.Err()
			return &IssuerCacheError{Failed: failed}
		default:
		}
		jwksURL, err := km.discoverJWKSURI(ctx, iss)
		if err != nil {
			err = fmt.Errorf("failed to discover JWKS URI: %w", err)
			nlog.Errorf("iss %s err: %v", iss, err)
			failed[iss] = err
			continue
		}
		km.issuerToJWKS[iss] = jwksURL
		err = km.jwksCache.Register(jwksURL, jwk.WithHTTPClient(km.jwksClient), jwk.WithMinRefreshInterval(*km.cacheConfig.MinCacheRefreshInterval))
		if err != nil {
			err = fmt.Errorf("failed to register jwksURL in cache: %w", err)
			nlog.Errorf("iss %s err: %v", iss, err)
			failed[iss] = err
			continue
		}
		anySucceeded = true
		// Preload -- best effort
		if _, err = km.jwksCache.Refresh(ctx, jwksURL); err != nil {
			nlog.Errorf("failed to preload jwks cache for issuer %s: %v", iss, err)
			continue
		}
	}
	if len(failed) > 0 && !anySucceeded {
		return &IssuerCacheError{Failed: failed}
	}
	return nil
}

func (km *KeyCacheManager) getKeyForToken(ctx context.Context, tok *jwt.Token) (any, error) {
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
	if km.jwksCache == nil {
		return nil, errors.New("jwks cache not initialized, cannot get public key")
	}
	jwksCache := km.jwksCache
	jwksURL, ok := km.issuerToJWKS[iss]
	if !ok {
		return nil, fmt.Errorf("failed to get keyset; no JWKS entry exists for issuer %s", iss)
	}
	keySet, err := jwksCache.Get(ctx, jwksURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get keyset for issuer %s: %v", iss, err)
	}
	jwKey, found := keySet.LookupKeyID(kid)
	if !found {
		return nil, fmt.Errorf("key with kid %s not found for issuer %s", kid, iss)
	}
	var pubKey any
	if err = jwKey.Raw(&pubKey); err != nil {
		return nil, fmt.Errorf("failed to parse public key [iss: %s, kid: %s, err: %w]", iss, kid, err)
	}
	return pubKey, nil
}

// Parse and validate key issuer URLs used for JWKS discovery and fetching
func validateIssuerURL(urlString string) error {
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return fmt.Errorf("invalid URL %s: %w", urlString, err)
	}

	if parsedURL.Scheme != "https" {
		return fmt.Errorf("must use HTTPS: %s", urlString)
	}

	if parsedURL.Host == "" {
		return fmt.Errorf("must have a host: %s", urlString)
	}

	if !parsedURL.IsAbs() {
		return fmt.Errorf("must be an absolute URL: %s", urlString)
	}
	return nil
}

func getDiscoveryURL(issuer string) (string, error) {
	// Parse and validate the issuer URL
	err := validateIssuerURL(issuer)
	if err != nil {
		return "", fmt.Errorf("invalid issuer URL %s: %w", issuer, err)
	}
	base := strings.TrimSuffix(issuer, "/")
	return base + oidcDiscoveryEndpoint, nil
}

func (km *KeyCacheManager) discoverJWKSURI(ctx context.Context, issuer string) (string, error) {
	discoveryURL, err := getDiscoveryURL(issuer)
	if err != nil {
		return "", err
	}
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
		urlErr := validateIssuerURL(jwksURL)
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

func (e *IssuerCacheError) Error() string {
	var b strings.Builder
	b.WriteString("JWKS cache creation failed for issuers:\n")
	for iss, err := range e.Failed {
		fmt.Fprintf(&b, "  %s: %v\n", iss, err)
	}
	return b.String()
}
