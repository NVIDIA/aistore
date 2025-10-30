// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/golang-jwt/jwt/v5"
)

type mockTokenParser struct {
	validateMap map[string]error
	claimsMap   map[string]*tok.AISClaims
}

var (
	validClaim   = &tok.AISClaims{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour))}}
	expiredClaim = &tok.AISClaims{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Second))}}
)

// Validate tokens in the validateMap, return claims from claimsMap, otherwise fail
func (m *mockTokenParser) ValidateToken(token string) (*tok.AISClaims, error) {
	if err, ok := m.validateMap[token]; ok {
		return nil, err
	}
	if claims, ok := m.claimsMap[token]; ok {
		return claims, nil
	}
	return nil, errors.New("invalid token")
}

func (*mockTokenParser) IsSecretCksumValid(_ string) bool {
	return true
}

func (*mockTokenParser) IsPublicKeyValid(_ string) (bool, error) {
	return true, nil
}

func newMockTokenParser() *mockTokenParser {
	return &mockTokenParser{
		validateMap: make(map[string]error),
		claimsMap:   make(map[string]*tok.AISClaims),
	}
}

func TestAuth_Manager_UpdateRevokedList_AddsRevokedTokens(t *testing.T) {
	mockParser := newMockTokenParser()
	// Token claims must be non-expired to be added to revoked list
	tokens := []string{"tok1", "tok2"}
	// Tell the mock parser to consider these tokens valid
	for _, token := range tokens {
		mockParser.claimsMap[token] = validClaim
	}
	am := &authManager{
		tokenMap:      newTokenClaimMap(),
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   mockParser,
	}
	revoked := &tokenList{Tokens: tokens, Version: 2}

	res := am.updateRevokedList(revoked)

	tassert.Fatal(t, res != nil, "Expect a non-nil result from revocation call")
	tassert.Error(t, len(res.Tokens) == 2, "Expected only 2 entries")
	tassert.Error(t, res.Version == revoked.Version, "Expected version to match")
	tassert.Error(t, slices.Contains(res.Tokens, "tok1"), "Expected to contain 'tok1'")
	tassert.Error(t, slices.Contains(res.Tokens, "tok2"), "Expected to contain 'tok2'")
}

func TestAuth_Manager_UpdateRevokedList_CleansExpiredTokens(t *testing.T) {
	am := &authManager{
		tokenMap:      newTokenClaimMap(),
		revokedTokens: newRevokedTokensMap(),
		tokenParser: &mockTokenParser{
			validateMap: map[string]error{"expiredToken": tok.ErrTokenExpired},
		},
	}
	revoked := &tokenList{Tokens: []string{"expiredToken"}, Version: 1}
	allRevoked := am.updateRevokedList(revoked)
	tassert.Error(t, allRevoked == nil, "Expected allRevoked to be nil after cleanup of expired tokens")
}

func TestAuth_Manager_RevokedTokenList(t *testing.T) {
	am := &authManager{
		tokenMap:      newTokenClaimMap(),
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   newMockTokenParser(),
	}
	err := am.revokedTokens.update(&tokenList{Tokens: []string{"tok1"}})
	tassert.Error(t, err == nil, "expected manual token update to succeed")
	list := am.revokedTokenList()
	tassert.Error(t, len(list.Tokens) == 1, "Expected only 1 entry after update")
	tassert.Error(t, list.Tokens[0] == "tok1", "Expected 'tok1' to be revoked after update")
}

func TestAuth_Manager_ValidateToken_Revoked(t *testing.T) {
	// Set up a token with valid claims but revoke it, should still fail
	token := "revoked"
	tcMap := newTokenClaimMap()
	tcMap.set(token, validClaim)
	am := &authManager{
		tokenMap:      newTokenClaimMap(),
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   &mockTokenParser{},
	}
	am.updateRevokedList(&tokenList{Tokens: []string{token}})
	_, err := am.validateToken(token)
	tassert.Error(t, err != nil, "Revoked token should not be valid")
}

func TestAuth_Manager_ValidateToken_CachedValid(t *testing.T) {
	token := "goodtoken"
	tcMap := newTokenClaimMap()
	tcMap.set(token, validClaim)
	am := &authManager{
		tokenMap:      tcMap,
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   &mockTokenParser{},
	}
	_, err := am.validateToken(token)
	tassert.Errorf(t, err == nil, "Expected token to be valid, but got: %v", err)
}

func TestAuth_Manager_ValidateToken_CachedExpired(t *testing.T) {
	token := "expiredtoken"
	tcMap := newTokenClaimMap()
	tcMap.set(token, expiredClaim)
	am := &authManager{
		tokenMap:      tcMap,
		revokedTokens: newRevokedTokensMap(),
		tokenParser:   &mockTokenParser{},
	}
	_, err := am.validateToken(token)
	tassert.Error(t, err != nil, "Expected error when token is expired")
}

func TestAuth_RevokedTokensMap_Initialize(t *testing.T) {
	r := newRevokedTokensMap()
	tassert.Errorf(t, r.version == 1, "expected version 1, got %d", r.version)
	tassert.Error(t, len(r.revokedTokens) == 0, "expected empty revokedTokens map at start")
}

func TestAuth_RevokedTokensMap_UpdateAndContains(t *testing.T) {
	r := newRevokedTokensMap()
	tl := &tokenList{Tokens: []string{"a", "b"}}

	err := r.update(tl)
	tassert.Error(t, err == nil, "expected manual token update to succeed")
	tassert.Error(t, r.contains("a"), "expected token 'a' in revokedTokens map")
	tassert.Error(t, r.contains("b"), "expected token 'b' in revokedTokens map")
	tassert.Error(t, !r.contains("c"), "unexpected token 'c' found in revokedTokens map")
}

func TestAuth_RevokedTokensMap_GetAll(t *testing.T) {
	r := newRevokedTokensMap()
	tassert.Error(t, r.getAll() == nil, "expected nil for empty map from getAll()")
	err := r.update(&tokenList{Tokens: []string{"tok1", "tok2"}})
	tassert.Error(t, err == nil, "expected manual token update to succeed")
	tl := r.getAll()
	tassert.Error(t, tl != nil && len(tl.Tokens) == 2, "expected two tokens returned from getAll")
}

func TestAuth_RevokedTokensMap_Update_ManualRevoke(t *testing.T) {
	r := newRevokedTokensMap()
	err := r.update(&tokenList{Version: 0})
	tassert.Error(t, err == nil, "expected nil error for manual revoke")
	tassert.Error(t, r.version == 2, "expected version to increment to 2 after manual revoke")
}

func TestAuth_RevokedTokensMap_CheckVersion(t *testing.T) {
	r := newRevokedTokensMap()
	r.version = 5
	err := r.update(&tokenList{Version: 6})
	tassert.Errorf(t, err == nil, "expected success with higher version, got: %v", err)
	tassert.Errorf(t, r.version == 6, "expected version updated to 6, got %d", r.version)

	r.version = 10
	err = r.update(&tokenList{Version: 8})
	tassert.Error(t, err != nil, "expected error for lower version")
	tassert.Errorf(t, r.version == 10, "version should remain 10 after error, received %d", r.version)
}

func TestAuth_RevokedTokensMap_Cleanup(t *testing.T) {
	// Currently only removing revoked tokens that have expired
	r := newRevokedTokensMap()
	err := r.update(&tokenList{Tokens: []string{"good", "expired", "bad"}})
	tassert.Error(t, err == nil, "expected manual token update to succeed")
	tassert.Fatalf(t, len(r.revokedTokens) == 3, "expected only the 3 new entries in the revoked list")
	parser := &mockTokenParser{
		validateMap: map[string]error{
			"expired": tok.ErrTokenExpired,
			"bad":     errors.New("parse error"),
			"good":    nil,
		},
	}
	result := r.cleanup(parser)
	tassert.Error(t, len(result.Tokens) == 2, "expected both valid and invalid tokens after cleanup")
	tassert.Error(t, !r.contains("expired"), "expired token should have been removed")
}

func TestAuth_RevokedTokensMap_Concurrency(t *testing.T) {
	r := newRevokedTokensMap()
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			err := r.update(&tokenList{Tokens: []string{fmt.Sprintf("tok-%d", i)}})
			tassert.Error(t, err == nil, "expected manual token update to succeed")
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			_ = r.contains(fmt.Sprintf("tok-%d", i))
			_ = r.getAll()
		}
	}()
	wg.Wait()
	// No assertion here as success is absence of data race or panic.
}

func TestAuth_TokenClaimMap_Initialize(t *testing.T) {
	m := newTokenClaimMap()
	tassert.Error(t, m != nil, "expected newTokenClaimMap to return non-nil")
	tassert.Error(t, len(m.tokenMap) == 0, "expected empty tokenMap on creation")
}

func TestAuth_TokenClaimMap_SetAndGet(t *testing.T) {
	m := newTokenClaimMap()
	c := &tok.AISClaims{IsAdmin: true}
	m.set("foo", c)
	val, ok := m.get("foo")
	tassert.Error(t, ok, "expected token 'foo' to be present after set")
	tassert.Error(t, val == c, "retrieved claims did not match the inserted claims")
}

func TestAuth_TokenClaimMap_Get_NonExistent(t *testing.T) {
	m := newTokenClaimMap()
	val, ok := m.get("bar")
	tassert.Error(t, !ok, "expected get on missing token to return false")
	tassert.Error(t, val == nil, "expected nil claims for missing token")
}

func TestAuth_TokenClaimMap_Delete(t *testing.T) {
	m := newTokenClaimMap()
	c := &tok.AISClaims{}
	m.set("tok", c)
	m.delete("tok")
	val, ok := m.get("tok")
	tassert.Error(t, !ok, "expected token to be gone after delete")
	tassert.Error(t, val == nil, "expected nil claims after delete")
}

func TestAuth_TokenClaimMap_DeleteMultiple(t *testing.T) {
	m := newTokenClaimMap()
	c1, c2 := &tok.AISClaims{}, &tok.AISClaims{}
	m.set("a", c1)
	m.set("b", c2)
	m.deleteMultiple([]string{"a", "b", "nonexistent"})
	_, ok1 := m.get("a")
	_, ok2 := m.get("b")
	tassert.Error(t, !ok1 && !ok2, "expected all specified tokens to be deleted")
}

func TestAuth_TokenClaimMap_Concurrency(_ *testing.T) {
	m := newTokenClaimMap()
	wg := sync.WaitGroup{}
	c := &tok.AISClaims{}
	// Set tokens concurrently
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.set(string(rune('A'+i)), c)
			m.get(string(rune('A' + i)))
		}(i)
	}
	wg.Wait()
	// Delete concurrently
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.delete(string(rune('A' + i)))
		}(i)
	}
	wg.Wait()
}
