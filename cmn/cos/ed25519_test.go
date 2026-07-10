// Package cos_test: unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestNodeKeyPairSignVerify(t *testing.T) {
	pub, priv, err := cos.GenerateNodeKeyPair()
	tassert.CheckFatal(t, err)

	msg := []byte("hello intra-cluster request")

	sig, err := cos.SignNodeMessage(priv, msg)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(sig) == cos.NodeSigningSignatureSize, "sig size %d", len(sig))

	err = cos.VerifyNodeSignature(pub, msg, sig)
	tassert.CheckFatal(t, err)

	err = cos.VerifyNodeSignature(pub, []byte("tampered"), sig)
	tassert.Fatalf(t, err != nil, "expected verification failure")
}

func TestNodeKeyPairBadSizes(t *testing.T) {
	err := cos.VerifyNodeSignature(nil, []byte("x"), make([]byte, cos.NodeSigningSignatureSize))
	tassert.Fatalf(t, err != nil, "expected bad public key size")

	_, err = cos.SignNodeMessage(nil, []byte("x"))
	tassert.Fatalf(t, err != nil, "expected bad private key size")

	_, err = cos.NodeVerifyingKeyFingerprint(nil)
	tassert.Fatalf(t, err != nil, "expected bad public key size")
}
