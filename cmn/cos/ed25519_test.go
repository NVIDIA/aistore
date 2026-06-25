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

func TestNodeSigningKeySignVerify(t *testing.T) {
	pub, priv, err := cos.GenerateNodeSigningKey()
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

func TestNodeSigningKeyBadSizes(t *testing.T) {
	err := cos.VerifyNodeSignature(nil, []byte("x"), make([]byte, cos.NodeSigningSignatureSize))
	tassert.Fatalf(t, err != nil, "expected bad public key size")

	_, err = cos.SignNodeMessage(nil, []byte("x"))
	tassert.Fatalf(t, err != nil, "expected bad private key size")

	_, err = cos.NodeSigningKeyFingerprint(nil)
	tassert.Fatalf(t, err != nil, "expected bad public key size")
}

func TestNodeSigningKeyPackUnpack(t *testing.T) {
	const tid = "t1234567"

	pub, priv, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)

	pair := cos.NewNodeSigningKey(priv, pub)
	b := pair.Bytes(tid)

	got, err := cos.UnpackNodeSigningKey(tid, b)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, got.Equal(pair), "unpacked key differs")
}

func TestNodeSigningKeyUnpackRejectsWrongDaemonID(t *testing.T) {
	pub, priv, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)

	pair := cos.NewNodeSigningKey(priv, pub)

	_, err = cos.UnpackNodeSigningKey("otherid", pair.Bytes("t1234567"))
	tassert.Fatalf(t, err != nil, "expected daemon ID mismatch")
}

func TestNodeSigningKeyUnpackRejectsTruncatedBlob(t *testing.T) {
	const tid = "t1234567"

	pub, priv, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)

	pair := cos.NewNodeSigningKey(priv, pub)
	b := pair.Bytes(tid)

	for i := range b {
		_, err := cos.UnpackNodeSigningKey(tid, b[:i])
		tassert.Fatalf(t, err != nil, "expected failure for truncated blob len %d", i)
	}
}

func TestNodeSigningKeyUnpackRejectsTrailingBytes(t *testing.T) {
	const tid = "t1234567"

	pub, priv, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)

	pair := cos.NewNodeSigningKey(priv, pub)
	b := append(pair.Bytes(tid), 0)

	_, err = cos.UnpackNodeSigningKey(tid, b)
	tassert.Fatalf(t, err != nil, "expected trailing-bytes failure")
}
