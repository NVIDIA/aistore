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
	NodeKeyPairAlg = "ed25519"

	NodeSigningPublicKeySize  = ed25519.PublicKeySize
	NodeSigningPrivateKeySize = ed25519.PrivateKeySize
	NodeSigningSignatureSize  = ed25519.SignatureSize
)

type NodeKeyPair struct {
	SigningKey   ed25519.PrivateKey
	VerifyingKey ed25519.PublicKey
}

func NewNodeKeyPair(signingKey ed25519.PrivateKey, verifyingKey ed25519.PublicKey) *NodeKeyPair {
	k := &NodeKeyPair{
		SigningKey:   append(ed25519.PrivateKey(nil), signingKey...),
		VerifyingKey: append(ed25519.PublicKey(nil), verifyingKey...),
	}
	debug.AssertNoErr(k.validate())
	return k
}

func (k *NodeKeyPair) validate() error {
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

//
// utilities
//

func GenerateNodeKeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// TODO: currently used only in tests; consider to maybe validate a change upon restart, or remove
func NodeVerifyingKeyFingerprint(pub ed25519.PublicKey) (string, error) {
	if len(pub) != ed25519.PublicKeySize {
		return "", fmt.Errorf("invalid %s public key size %d", NodeKeyPairAlg, len(pub))
	}
	sum := sha256.Sum256(pub)
	return base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

func SignNodeMessage(priv ed25519.PrivateKey, msg []byte) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid %s private key size %d", NodeKeyPairAlg, len(priv))
	}
	return ed25519.Sign(priv, msg), nil
}

func VerifyNodeSignature(pub ed25519.PublicKey, msg, sig []byte) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid %s public key size %d", NodeKeyPairAlg, len(pub))
	}
	if len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("invalid %s signature size %d", NodeKeyPairAlg, len(sig))
	}
	if !ed25519.Verify(pub, msg, sig) {
		return errors.New("node signature verification failed")
	}
	return nil
}
