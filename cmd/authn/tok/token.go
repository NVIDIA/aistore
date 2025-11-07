// Package tok provides AuthN token (structure and methods)
// for validation by AIS gateways
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tok

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/golang-jwt/jwt/v5"
)

type (
	AISClaims struct {
		// Deprecated: Use RegisteredClaims.Subject instead, mapped to 'sub' claim.
		UserID string `json:"username"`
		// Deprecated: Use RegisteredClaims.ExpiresAt instead, mapped to 'exp' claim.
		Expires     time.Time       `json:"expires"`
		ClusterACLs []*authn.CluACL `json:"clusters"`
		BucketACLs  []*authn.BckACL `json:"buckets,omitempty"`
		IsAdmin     bool            `json:"admin"`
		jwt.RegisteredClaims
	}

	TokenParser struct {
		// used for validating JWT signature with e.g. a public key
		sigConfig *SigConfig
		// options for the jwt parser to use
		parseOpts []jwt.ParserOption
	}

	TokenHdr struct {
		// Request header containing token string
		Header string
		// Raw token string from request
		Token string
	}

	SigConfig struct {
		HMACSecret   string
		RSAPublicKey *rsa.PublicKey
	}

	Parser interface {
		// ValidateToken verifies JWT signature and extracts token claims.
		ValidateToken(tokenStr string) (*AISClaims, error)
		// IsSecretCksumValid checks if a provided secret checksum is valid.
		IsSecretCksumValid(cksumVal string) bool
		// IsPublicKeyValid checks if a provided public key matches the parser's key.
		IsPublicKeyValid(pubKeyStr string) (bool, error)
	}

	RequiredClaims struct {
		Aud []string
	}
)

var (
	ErrNoPermissions        = errors.New("insufficient permissions")
	ErrInvalidToken         = errors.New("invalid token")
	ErrNoSubject            = errors.New("missing 'sub' or 'username' claims")
	ErrNoToken              = errors.New("token required")
	ErrNoBearerToken        = errors.New("invalid token: no bearer")
	ErrTokenExpired         = errors.New("token expired")
	ErrTokenRevoked         = errors.New("token revoked")
	supportedSigningMethods = []string{jwt.SigningMethodRS256.Name, jwt.SigningMethodRS384.Name, jwt.SigningMethodRS512.Name, jwt.SigningMethodHS256.Name}
)

// TODO: cos.Unsafe* and other micro-optimization and refactoring

func CreateHMACTokenStr(c jwt.Claims, secret string) (string, error) {
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	return t.SignedString([]byte(secret))
}

func CreateRSATokenStr(c jwt.Claims, rsaKey *rsa.PrivateKey) (string, error) {
	t := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	return t.SignedString(rsaKey)
}

func StandardClaims(expires time.Time, userID, aud string, bucketACLs []*authn.BckACL, clusterACLs []*authn.CluACL) *AISClaims {
	return &AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expires),
			Subject:   userID,
			Audience:  []string{aud},
		},
		BucketACLs:  bucketACLs,
		ClusterACLs: clusterACLs,
	}
}

func AdminClaims(expires time.Time, userID, aud string) *AISClaims {
	return &AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expires),
			Subject:   userID,
			Audience:  []string{aud},
		},
		IsAdmin: true,
	}
}

// extractBearerToken extracts a bearer token from the Authorization header.
// Header format: 'Authorization: Bearer <token>'
func extractBearerToken(hdr http.Header) (*TokenHdr, error) {
	s := hdr.Get(apc.HdrAuthorization)
	if s == "" {
		return nil, ErrNoToken
	}
	idx := strings.Index(s, " ")
	if idx == -1 || s[:idx] != apc.AuthenticationTypeBearer {
		return nil, ErrNoBearerToken
	}
	return &TokenHdr{Header: apc.HdrAuthorization, Token: s[idx+1:]}, nil
}

// ExtractToken extracts JWT token from either Authorization header (Bearer token)
// or X-Amz-Security-Token header with the following priority:
//  1. Authorization: Bearer <token> (standard JWT auth)
//  2. X-Amz-Security-Token: enables native AWS SDK clients to authenticate using AIS-compatible JWT tokens passed when
//     using SigV4 authentication.
func ExtractToken(hdr http.Header) (*TokenHdr, error) {
	// First, try standard Bearer token from Authorization header
	t, err := extractBearerToken(hdr)
	if err == nil {
		return t, nil
	}

	// Fallback to X-Amz-Security-Token for AWS SDK compatibility
	s := hdr.Get(s3.HeaderSecurityToken)
	if s != "" {
		return &TokenHdr{Header: s3.HeaderSecurityToken, Token: s}, nil
	}
	return nil, ErrNoToken
}

/////////////////
// TokenParser //
/////////////////

func NewTokenParser(sigConf *SigConfig, reqClaims *RequiredClaims) *TokenParser {
	return &TokenParser{
		sigConfig: sigConf,
		parseOpts: buildParseOptions(reqClaims),
	}
}

func buildParseOptions(reqClaims *RequiredClaims) []jwt.ParserOption {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(supportedSigningMethods),
	}
	if reqClaims != nil && len(reqClaims.Aud) > 0 {
		opts = append(opts, jwt.WithAudience(reqClaims.Aud...))
	}
	return opts
}

// Based on the token provided, return the key for the jwt library to use to verify the signature
func (tm *TokenParser) parseJWTKey(tok *jwt.Token) (any, error) {
	switch tok.Method.(type) {
	case *jwt.SigningMethodHMAC:
		return []byte(tm.getHMACSecret()), nil
	case *jwt.SigningMethodRSA:
		return tm.getRSAPublicKey(), nil
	default:
		return nil, fmt.Errorf("unsupported signing method %v, header specified %s", tok.Method, tok.Header["alg"])
	}
}

func ParsePubKey(str string) (*rsa.PublicKey, error) {
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

// ValidateToken verifies JWT signature and extracts token claims
// (supporting both HMAC (HS256) and RSA (RS256) signing methods)
// - HS256: validates with secret (symmetric)
// - RS256: validates with pubKey (asymmetric)
func (tm *TokenParser) ValidateToken(tokenStr string) (*AISClaims, error) {
	jwtToken, err := jwt.ParseWithClaims(
		tokenStr,
		&AISClaims{},
		func(t *jwt.Token) (any, error) {
			return tm.parseJWTKey(t)
		},
		tm.parseOpts...,
	)
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, fmt.Errorf("%w [err: %w]", ErrInvalidToken, ErrTokenExpired)
		}
		return nil, fmt.Errorf("%w [err: %w]", ErrInvalidToken, err)
	}
	claims, ok := jwtToken.Claims.(*AISClaims)
	if !ok || !jwtToken.Valid {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// IsSecretCksumValid Checks if a provided secret checksum is valid for signing requests to be parsed by this cluster
func (tm *TokenParser) IsSecretCksumValid(cksumVal string) bool {
	return cos.ChecksumB2S(cos.UnsafeB(tm.getHMACSecret()), cos.ChecksumSHA256) == cksumVal
}

// IsPublicKeyValid Checks if a provided public key matches what this cluster will use to validate tokens
func (tm *TokenParser) IsPublicKeyValid(pubKeyStr string) (bool, error) {
	reqKey, err := ParsePubKey(pubKeyStr)
	if err != nil {
		return false, err
	}
	return reqKey.Equal(tm.getRSAPublicKey()), nil
}

func (tm *TokenParser) getRSAPublicKey() *rsa.PublicKey {
	if tm.sigConfig == nil {
		return nil
	}
	return tm.sigConfig.RSAPublicKey
}

func (tm *TokenParser) getHMACSecret() string {
	if tm.sigConfig == nil {
		return ""
	}
	return tm.sigConfig.HMACSecret
}

///////////////
// AISClaims //
///////////////

// Validate implements Claims interface to add extra claims validation after parsing a token
func (c *AISClaims) Validate() error {
	if sub, err := c.GetSubject(); err != nil || sub == "" {
		return ErrNoSubject
	}
	return nil
}

// GetExpirationTime implements Claims interface with backwards-compatible support for 'expires'
func (c *AISClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	if c.ExpiresAt != nil && !c.ExpiresAt.IsZero() {
		return c.ExpiresAt, nil
	}
	return jwt.NewNumericDate(c.Expires), nil
}

// GetSubject implements Claims interface with backwards-compatible support for 'username'
func (c *AISClaims) GetSubject() (string, error) {
	if c.Subject != "" {
		return c.Subject, nil
	}
	return c.UserID, nil
}

func (c *AISClaims) String() string {
	sub, _ := c.GetSubject()
	return fmt.Sprintf("user %s, %s", sub, expiresIn(c.getExpiry()))
}

// Supports both our old `expires` and standard `exp` fields, with `exp` taking precedence
func (c *AISClaims) getExpiry() time.Time {
	if c.ExpiresAt != nil && !c.ExpiresAt.IsZero() {
		return c.ExpiresAt.UTC()
	}
	return c.Expires
}

func (c *AISClaims) IsExpired() bool {
	return c.getExpiry().Before(time.Now())
}

func (c *AISClaims) IsUser(user string) bool {
	sub, _ := c.GetSubject()
	return sub == user
}

// A user has two-level permissions: cluster-wide and on per bucket basis.
// To be able to access data, a user must have either permission. This
// allows creating users, e.g, with read-only access to the entire cluster,
// and read-write access to a single bucket.
// Per-bucket ACL overrides cluster-wide one.
// Permissions for a cluster with empty ID are used as default ones when
// a user do not have permissions for the given `clusterID`.
//
// ACL rules are checked in the following order (from highest to the lowest priority):
//  1. A user's role is an admin.
//  2. User's permissions for the given bucket
//  3. User's permissions for the given cluster
//  4. User's default cluster permissions (ACL for a cluster with empty clusterID)
//
// If there are no defined ACL found at any step, any access is denied.

func (c *AISClaims) CheckPermissions(clusterID string, bck *cmn.Bck, perms apc.AccessAttrs) error {
	if c.IsAdmin {
		return nil
	}
	if perms == 0 {
		return errors.New("empty permissions requested")
	}
	sub, _ := c.GetSubject()
	cluPerms := perms & apc.ClusterAccessRW
	objPerms := perms & apc.AccessRW
	cluACL, cluOk := c.aclForCluster(clusterID)
	if cluPerms != 0 {
		// Cluster-wide permissions requested
		if !cluOk {
			return fmt.Errorf("user `%s` has %v", sub, ErrNoPermissions)
		}
		if clusterID == "" {
			return errors.New("requested cluster permissions without cluster ID")
		}
		if !cluACL.Has(cluPerms) {
			return fmt.Errorf("user `%s` has %v: [cluster %s, %s, granted(%s)]", sub,
				ErrNoPermissions, clusterID, c, cluACL.Describe(false /*include all*/))
		}
	}
	if objPerms == 0 {
		return nil
	}

	// Check only bucket specific permissions.
	if bck == nil {
		return errors.New("requested bucket permissions without a bucket")
	}
	bckACL, bckOk := c.aclForBucket(clusterID, bck)
	if bckOk {
		if bckACL.Has(objPerms) {
			return nil
		}
		return fmt.Errorf("user `%s` has %v: [%s, bucket %s, granted(%s)]", sub,
			ErrNoPermissions, c, bck.String(), bckACL.Describe(false /*include all*/))
	}
	if !cluOk || !cluACL.Has(objPerms) {
		return fmt.Errorf("user `%s` has %v: [%s, granted(%s)]", sub, ErrNoPermissions, c, cluACL.Describe(false /*include all*/))
	}
	return nil
}

//
// private
//

func expiresIn(tm time.Time) string {
	dur := time.Until(tm)
	if dur <= 0 {
		return ErrTokenExpired.Error()
	}
	// round up
	dur = dur.Round(time.Second)
	return "token expires in " + dur.String()
}

func (c *AISClaims) aclForCluster(clusterID string) (perms apc.AccessAttrs, ok bool) {
	var defaultCluster *authn.CluACL
	for _, pm := range c.ClusterACLs {
		if pm.ID == clusterID {
			return pm.Access, true
		}
		if pm.ID == "" {
			defaultCluster = pm
		}
	}
	if defaultCluster != nil {
		return defaultCluster.Access, true
	}
	return 0, false
}

func (c *AISClaims) aclForBucket(clusterID string, bck *cmn.Bck) (perms apc.AccessAttrs, ok bool) {
	for _, b := range c.BucketACLs {
		tbBck := b.Bck
		if tbBck.Ns.UUID != clusterID {
			continue
		}
		// For AuthN all buckets are external: they have UUIDs of the respective AIS clusters.
		// To correctly compare with the caller's `bck` we construct tokenBck from the token.
		tokenBck := cmn.Bck{Name: tbBck.Name, Provider: tbBck.Provider}
		if tokenBck.Equal(bck) {
			return b.Access, true
		}
	}
	return 0, false
}
