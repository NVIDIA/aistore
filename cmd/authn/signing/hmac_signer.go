// Package signing manages keys the auth service uses for signing and verification
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package signing

import (
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/golang-jwt/jwt/v5"
)

type hmacSigner struct {
	secret cmn.Censored
}

// interface guard
var _ tok.Signer = (*hmacSigner)(nil)

func NewHMACSigner(secret cmn.Censored) tok.Signer {
	return &hmacSigner{secret: secret}
}

func (h *hmacSigner) SignToken(c jwt.Claims) (string, error) {
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	return t.SignedString([]byte(h.secret))
}

func (h *hmacSigner) GetSigConf() *cmn.AuthSignatureConf {
	return &cmn.AuthSignatureConf{Key: h.secret, Method: cmn.SigMethodHMAC}
}

func (h *hmacSigner) ValidationConf() *authn.ServerConf {
	checksum := cos.ChecksumB2S(cos.UnsafeB(string(h.secret)), cos.ChecksumSHA256)
	return &authn.ServerConf{Secret: checksum}
}
