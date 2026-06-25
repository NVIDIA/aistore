// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// For alternative (public, private) key alg., see rsa.go

const (
	NodeSigningKeyAlg = "ed25519"

	NodeSigningPublicKeySize  = ed25519.PublicKeySize
	NodeSigningPrivateKeySize = ed25519.PrivateKeySize
	NodeSigningSignatureSize  = ed25519.SignatureSize
)

func GenerateNodeSigningKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

func NodeSigningKeyFingerprint(pub ed25519.PublicKey) (string, error) {
	if len(pub) != ed25519.PublicKeySize {
		return "", fmt.Errorf("invalid %s public key size %d", NodeSigningKeyAlg, len(pub))
	}
	sum := sha256.Sum256(pub)
	return base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

func SignNodeMessage(priv ed25519.PrivateKey, msg []byte) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid %s private key size %d", NodeSigningKeyAlg, len(priv))
	}
	return ed25519.Sign(priv, msg), nil
}

func VerifyNodeSignature(pub ed25519.PublicKey, msg, sig []byte) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid %s public key size %d", NodeSigningKeyAlg, len(pub))
	}
	if len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("invalid %s signature size %d", NodeSigningKeyAlg, len(sig))
	}
	if !ed25519.Verify(pub, msg, sig) {
		return errors.New("node signature verification failed")
	}
	return nil
}

//
// AIS node signing key: the type, pack/unpack, validation
//

const nodeSigningKeyMetaVer byte = 1

type NodeSigningKey struct {
	SigningKey   ed25519.PrivateKey
	VerifyingKey ed25519.PublicKey
}

func NewNodeSigningKey(signingKey ed25519.PrivateKey, verifyingKey ed25519.PublicKey) *NodeSigningKey {
	k := &NodeSigningKey{
		SigningKey:   append(ed25519.PrivateKey(nil), signingKey...),
		VerifyingKey: append(ed25519.PublicKey(nil), verifyingKey...),
	}
	debug.AssertNoErr(k.validate())
	return k
}

func (k *NodeSigningKey) PackedSize(daeID string) int {
	debug.Assert(daeID != "")
	return 1 + PackedStrLen(daeID) + PackedBytesLen(k.SigningKey) + PackedBytesLen(k.VerifyingKey)
}

func (k *NodeSigningKey) Pack(p *BytePack, daeID string) {
	debug.Assert(daeID != "")
	p.WriteUint8(nodeSigningKeyMetaVer)
	p.WriteString(daeID)
	p.WriteBytes(k.SigningKey)
	p.WriteBytes(k.VerifyingKey)
}

func (k *NodeSigningKey) Bytes(daeID string) []byte {
	debug.Assert(daeID != "")
	debug.AssertNoErr(k.validate())

	p := NewPacker(nil, k.PackedSize(daeID))
	k.Pack(p, daeID)
	return p.Bytes()
}

func (k *NodeSigningKey) Equal(o *NodeSigningKey) bool {
	return k != nil && o != nil &&
		CryptoEqual(k.SigningKey, o.SigningKey) &&
		CryptoEqual(k.VerifyingKey, o.VerifyingKey)
}

func (k *NodeSigningKey) validate() error {
	if k == nil {
		return errors.New("nil node signing key")
	}
	if len(k.SigningKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("invalid signing key size %d", len(k.SigningKey))
	}
	if len(k.VerifyingKey) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid verifying key size %d", len(k.VerifyingKey))
	}

	pub, ok := k.SigningKey.Public().(ed25519.PublicKey)
	if !ok {
		return errors.New("failed to derive verifying key")
	}
	if !bytes.Equal(pub, k.VerifyingKey) {
		return errors.New("signing/verifying key mismatch")
	}
	return nil
}

func UnpackNodeSigningKey(expectedID string, b []byte) (*NodeSigningKey, error) {
	u := NewUnpacker(b)

	ver, err := u.ReadByte()
	if err != nil {
		return nil, err
	}
	if ver != nodeSigningKeyMetaVer {
		return nil, fmt.Errorf("unsupported node signing key version %d", ver)
	}

	daeID, err := u.ReadString()
	if err != nil {
		return nil, err
	}
	if daeID == "" {
		return nil, errors.New("missing daemon ID")
	}
	if expectedID != "" && daeID != expectedID {
		return nil, fmt.Errorf("daemon ID mismatch: %q vs %q", expectedID, daeID)
	}

	signingKey, err := u.ReadBytes()
	if err != nil {
		return nil, err
	}
	verifyingKey, err := u.ReadBytes()
	if err != nil {
		return nil, err
	}
	if u.Len() != 0 {
		return nil, fmt.Errorf("unexpected trailing bytes: %d", u.Len())
	}

	k := &NodeSigningKey{
		SigningKey:   append(ed25519.PrivateKey(nil), signingKey...),
		VerifyingKey: append(ed25519.PublicKey(nil), verifyingKey...),
	}
	if err := k.validate(); err != nil {
		return nil, err
	}
	return k, nil
}
