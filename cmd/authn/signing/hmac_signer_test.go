// Package signing manages keys the auth service uses for signing and verification
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package signing_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmd/authn/signing"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestGetSigConf(t *testing.T) {
	testSecret := "myTestSecret"
	signer := signing.NewHMACSigner(cmn.Censored(testSecret))
	sigConf := signer.GetSigConf()
	tassert.Fatalf(t, sigConf.Method == cmn.SigMethodHMAC, "expected HMAC method, got %v", sigConf.Method)
	tassert.Fatalf(t, string(sigConf.Key) == testSecret, "expected key %q, got %q", testSecret, sigConf.Key)
}

func TestValidationConf(t *testing.T) {
	testSecret := "myTestSecret"
	signer := signing.NewHMACSigner(cmn.Censored(testSecret))

	serverConf := signer.ValidationConf()

	// For hmac, validation conf must not be the plaintext secret
	tassert.Fatalf(t, serverConf.Secret != testSecret, "expected hashed secret, got plaintext")
	tassert.Fatalf(t, serverConf.Secret != "", "expected non-empty secret checksum")

	// A new signer must generate a consistent checksum
	signer2 := signing.NewHMACSigner(cmn.Censored(testSecret))
	serverConf2 := signer2.ValidationConf()
	tassert.Fatalf(t, serverConf.Secret == serverConf2.Secret,
		"expected consistent checksum, got %q vs %q", serverConf.Secret, serverConf2.Secret)

	// If the secret changes, the validation conf must change
	signer3 := signing.NewHMACSigner("differentSecret")
	serverConf3 := signer3.ValidationConf()
	tassert.Fatalf(t, serverConf.Secret != serverConf3.Secret,
		"expected different checksum for different secret")
}
