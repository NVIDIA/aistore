// Package cos_test: unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"crypto/rsa"
	"encoding/pem"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestRSAKeyRoundTrip(t *testing.T) {
	key, err := cos.GenerateRSAKey(cos.RSAKeyMinBits)
	tassert.CheckFatal(t, err)

	privPEM, err := cos.EncodeRSAPrivateKeyPEM(key)
	tassert.CheckFatal(t, err)
	pubPEM, err := cos.EncodeRSAPublicKeyPEM(&key.PublicKey)
	tassert.CheckFatal(t, err)

	priv, err := cos.ParseRSAPrivateKeyPEM(privPEM)
	tassert.CheckFatal(t, err)

	tassert.Errorf(t, priv.N.Cmp(key.N) == 0, "modulus mismatch")
	tassert.Errorf(t, priv.E == key.E, "exponent mismatch")
	tassert.Errorf(t, len(pubPEM) > 0, "empty public key PEM")
}

func TestGenerateRSAKeyBits(t *testing.T) {
	// reject below minimum
	_, err := cos.GenerateRSAKey(cos.RSAKeyMinBits - 1)
	tassert.Errorf(t, err != nil, "expected small RSA key to fail")

	// zero defaults
	key, err := cos.GenerateRSAKey(0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, key.N.BitLen() == cos.RSAKeyDefaultBits,
		"expected default %d-bit key, got %d", cos.RSAKeyDefaultBits, key.N.BitLen())
}

func TestParseRSAPrivateKeyPEMNegative(t *testing.T) {
	// not PEM at all
	_, err := cos.ParseRSAPrivateKeyPEM([]byte("not pem"))
	tassert.Errorf(t, err != nil, "expected bad private PEM to fail")

	// wrong block type: public key PEM fed into the private-key parser
	key, err := cos.GenerateRSAKey(cos.RSAKeyMinBits)
	tassert.CheckFatal(t, err)
	pubPEM, err := cos.EncodeRSAPublicKeyPEM(&key.PublicKey)
	tassert.CheckFatal(t, err)
	_, err = cos.ParseRSAPrivateKeyPEM(pubPEM)
	tassert.Errorf(t, err != nil, "expected PEM type mismatch to fail")

	// valid PEM block, garbage DER
	junk := pem.EncodeToMemory(&pem.Block{Type: cos.PEMTypePrivateKey, Bytes: []byte("junk DER")})
	_, err = cos.ParseRSAPrivateKeyPEM(junk)
	tassert.Errorf(t, err != nil, "expected garbage DER to fail PKCS8 parse")
}

func TestEncodeRSANilKeys(t *testing.T) {
	_, err := cos.EncodeRSAPrivateKeyPEM(nil)
	tassert.Errorf(t, err != nil, "expected nil private key to fail")

	_, err = cos.EncodeRSAPublicKeyPEM((*rsa.PublicKey)(nil))
	tassert.Errorf(t, err != nil, "expected nil public key to fail")
}
