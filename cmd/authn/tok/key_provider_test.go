// Package tok_test includes tests for tok pkg
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tok_test

import (
	"errors"
	"testing"

	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestValidateToken_InvalidKey(t *testing.T) {
	tokenStr, err := hmacSigner.SignToken(newAdminClaims())
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	authConf := &cmn.AuthConf{Signature: &cmn.AuthSignatureConf{Method: "hmac", Key: "invalid-secret"}}
	invalidKeyProvider, err := tok.NewStaticKeyProvider(authConf)
	tassert.CheckFatal(t, err)
	invalidParser := tok.NewTokenParser(invalidKeyProvider, nil)
	_, err = invalidParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with wrong key to fail")
}
