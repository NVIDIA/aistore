// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Intra-cluster sign/verify (per-node Ed25519) -- v5.1 replacement for the
// CSK/HMAC path in csk.go.
//
// - Same plumbing, different crypto: reuses the CSK carrier (sig/nonce/smap-ver/pid
//   via dpq fast-path or query) and the byte-identical canonical payload from
//   signer.compute; only the signature algorithm changes. sigLen() is the one
//   place the 86-byte Ed25519 vs 43-byte HMAC length is decided.
//
// - 5.0 (see IsV50Bridge) disables CSK/HMAC; 5.1 will provide sign/verify replacement
//
// - TODO: lookup all "ref Ed25519" comments - and fix them all

const USE_SIGNVERIFY = true // TODO -- FIXME: remove (ref Ed25519)

func sigLen() int {
	if USE_SIGNVERIFY {
		debug.Assert(base64.RawURLEncoding.EncodedLen(cos.NodeSigningSignatureSize) == 86)
		return 86
	}
	return 43 // = base64.RawURLEncoding.EncodedLen(sha256.Size)
}

// canonical payload -- byte-identical layout to signer.compute (method, path,
// pid, smap-ver, content-length, nonce). Writes into sb; returns sb[:len].
func (sign *signer) svPayload(pid string) []byte {
	r, sb := sign.r, sign.sb

	sb.WriteString(r.Method)
	sb.WriteUint8(cskSepa)
	sb.WriteString(r.URL.Path)
	sb.WriteUint8(cskSepa)
	sb.WriteString(pid)
	sb.WriteUint8(cskSepa)

	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], uint64(sign.smapVer))
	sb.WriteBytes(b8[:])
	binary.BigEndian.PutUint64(b8[:], uint64(max(r.ContentLength, 0)))
	sb.WriteBytes(b8[:])
	binary.BigEndian.PutUint64(b8[:], sign.nonce)
	sb.WriteBytes(b8[:])

	return sb.Bytes()[:sb.Len()]
}

// send-side: sign the redirect with this node's private key.
// sign.sig ends up pointing into sb (buildURL/qencode clone it), same as compute().
func (sign *signer) svSign(pid string) {
	debug.Assert(sign.h.nodeSigningKey != nil)

	sign.sb.Reset(sign.bufsizeSV(pid), false /*allow shrink*/) // borrow redurl's sb

	msg := sign.svPayload(pid)
	raw, err := cos.SignNodeMessage(sign.h.nodeSigningKey.SigningKey, msg)
	debug.AssertNoErr(err)

	// sign.sig points into redurl's sb, same as compute()
	sign.sig = sign.sb.ReserveAppend(sigLen())
	base64.RawURLEncoding.Encode(sign.sig, raw)
}

// receive-side: resolve sender's VerifyingKey from Smap, verify.
// returns (http-code, err): 401 on bad/absent sig, 0 on success.
func (sign *signer) svVerify(pid, b64sig string) (int, error) {
	if len(b64sig) != sigLen() {
		return 0, fmt.Errorf("invalid signature length: %d", len(b64sig))
	}
	raw, err := base64.RawURLEncoding.DecodeString(b64sig)
	if err != nil {
		return 0, fmt.Errorf("invalid signature encoding: %w", err)
	}

	// TODO -- FIXME: Smap must be passed by the caller/parent
	si := sign.h.owner.smap.get().GetNode(pid)
	if si == nil {
		return http.StatusUnauthorized, fmt.Errorf("sender %q not in Smap", pid)
	}
	if len(si.VerifyingKey) != cos.NodeSigningPublicKeySize {
		return http.StatusUnauthorized, fmt.Errorf("sender %s: no verifying key", si.StringEx())
	}

	sign.sb = sbAlloc()
	sign.sb.Reset(sign.bufsizeSV(pid), true /*allow shrink*/)
	msg := sign.svPayload(pid)
	verr := cos.VerifyNodeSignature(si.VerifyingKey, msg, raw)
	sbFree(sign.sb)

	if verr != nil {
		return http.StatusUnauthorized, verr
	}
	return 0, nil
}

func (sign *signer) bufsizeSV(pid string) int {
	r := sign.r
	return len(r.Method) + 1 + len(r.URL.Path) + 1 + len(pid) + 1 + 3*cos.SizeofI64 + sigLen()
}
