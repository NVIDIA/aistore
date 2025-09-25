// Package authn
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package tok_test

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/golang-jwt/jwt/v5"
)

// Helper for dummy permission structs
func makeBckACL(access apc.AccessAttrs, clusterID, name string) *authn.BckACL {
	return &authn.BckACL{
		Bck:    cmn.Bck{Name: name, Provider: "ais", Ns: cmn.Ns{UUID: clusterID}},
		Access: access,
	}
}

func makeCluACL(access apc.AccessAttrs, clusterID string) *authn.CluACL {
	return &authn.CluACL{
		ID:     clusterID,
		Access: access,
	}
}

func TestAdminJWTAndValidate(t *testing.T) {
	secret := "supersecret"
	expires := time.Now().Add(1 * time.Hour)
	user := "adminUser"

	tokenStr, err := tok.AdminJWT(expires, user, secret)
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	tk, err := tok.ValidateToken(tokenStr, secret, nil)
	tassert.Fatalf(t, err == nil, "ValidateToken failed: %v", err)
	tassert.Error(t, tk.IsAdmin, "Expected IsAdmin to be true")
	tassert.Errorf(t, tk.UserID == user, "Expected UserID %q, got %q", user, tk.UserID)
	// Bucket and cluster don't matter if token has full admin access
	err = tk.CheckPermissions("any", &cmn.Bck{Name: "b1", Provider: "ais"}, apc.ClusterAccessRW&apc.AccessRW)
	tassert.Errorf(t, err == nil, "Failed to get full cluster RW access, error: %v", err)
}

func TestJWTBucket(t *testing.T) {
	secret := "secret"
	expires := time.Now().Add(2 * time.Hour)
	user := "normalUser"

	bckACL := makeBckACL(apc.AccessRO, "cid1", "b1")

	tokenStr, err := tok.JWT(expires, user, []*authn.BckACL{bckACL}, nil, secret)
	tassert.Fatalf(t, err == nil, "JWT generation failed: %v", err)
	tk, err := tok.ValidateToken(tokenStr, secret, nil)
	tassert.Fatalf(t, err == nil, "ValidateToken failed: %v", err)

	// When comparing against bucket, don't include the cluster id in the namespace
	bck := &cmn.Bck{Name: "b1", Provider: "ais"}
	err = tk.CheckPermissions("cid1", bck, apc.AccessRO)
	tassert.Errorf(t, err == nil, "Expected RO access on b1, got:: %v", err)

	err = tk.CheckPermissions("cid1", bck, apc.AccessRW)
	tassert.Error(t, err != nil, "Expected validation error requesting RW access on b1")
	err = tk.CheckPermissions("cid2", bck, apc.AccessRO)
	tassert.Error(t, err != nil, "Expected validation error when accessing cluster cid2")

	bck2 := &cmn.Bck{Name: "b2", Provider: "ais", Ns: cmn.Ns{UUID: "cid1"}}
	err = tk.CheckPermissions("cid1", bck2, apc.AccessRO)
	tassert.Error(t, err != nil, "Expected write to b2 to be denied")
}

func TestJWTCluster(t *testing.T) {
	secret := "secret"
	expires := time.Now().Add(2 * time.Hour)
	user := "normalUser"

	cluACL := makeCluACL(apc.ClusterAccessRO, "cid1")

	tokenStr, err := tok.JWT(expires, user, nil, []*authn.CluACL{cluACL}, secret)
	tassert.Fatalf(t, err == nil, "JWT generation failed: %v", err)
	tk, err := tok.ValidateToken(tokenStr, secret, nil)
	tassert.Fatalf(t, err == nil, "ValidateToken failed: %v", err)

	err = tk.CheckPermissions("cid1", nil, apc.ClusterAccessRO)
	tassert.Errorf(t, err == nil, "Expected RO access on cid1, got: %v", err)

	err = tk.CheckPermissions("cid1", nil, apc.ClusterAccessRW)
	tassert.Error(t, err != nil, "Expected  validation error when requesting RW access on cid1")

	err = tk.CheckPermissions("cid2", nil, apc.ClusterAccessRO)
	tassert.Error(t, err != nil, "Expected  validation error when requesting RO access on cid2")
}

func TestExtractToken(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer sometoken")
	token, err := tok.ExtractToken(hdr)
	tassert.Fatalf(t, err == nil, "ExtractToken failed: %v", err)
	tassert.Errorf(t, token == "sometoken", "Expected 'sometoken', got %q", token)
	hdr.Set("Authorization", "sometoken")
	_, err = tok.ExtractToken(hdr)
	tassert.Error(t, err != nil, "Expected failure due to missing 'Bearer' prefix")
}

func TestTokenExpiration(t *testing.T) {
	tk := &tok.Token{Expires: time.Now().Add(-time.Second)}
	tassert.Error(t, tk.IsExpired(nil), "Token should be expired")
	tk2 := &tok.Token{Expires: time.Now().Add(1 * time.Hour)}
	tassert.Error(t, !tk2.IsExpired(nil), "Token should not be expired")
}

func TestRSAJWTValidate(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA keys: %v", err)
	}

	expires := time.Now().Add(30 * time.Minute)
	user := "rsauser"
	claims := map[string]any{
		"expires":  expires,
		"username": user,
		"admin":    true,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims(claims))
	tokenStr, err := token.SignedString(rsaKey)
	tassert.Fatalf(t, err == nil, "RSA signing failed: %v", err)

	tk, err := tok.ValidateToken(tokenStr, "", &rsaKey.PublicKey)
	tassert.Fatalf(t, err == nil, "ValidateToken failed with RSA: %v", err)
	tassert.Errorf(t, tk.UserID == user, "Expected username %q, got %q", user, tk.UserID)
	tassert.Errorf(t, tk.IsAdmin, "Expected IsAdmin to be true")
}
