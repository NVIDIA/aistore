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
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Intra-cluster sign/verify using per-node Ed25519 keys.
//
// The request envelope carries the sender ID, Smap version, nonce, and signature
// via redirect query parameters or control-plane headers. The canonical payload
// additionally covers the HTTP method, path, and content length.
//
// The v5.0 bridge disables sign/verify during rolling upgrades from 4.x;
// v5.1 will enable this implementation.
//
// TODO - in order of priority:
// - keep local keypairs memory-only: regenerate on restart and advertise via joinCluster
// - enable control-plane signing and verification for h.call, h.bcastNodes, and related paths
// - add and validate auth.intra_cluster.algorithm
// - enable sign/verify in deploy/dev/local/aisnode_config.sh - make it default for CI
// ==
// - stress-test config transitions (the respective time windows): off -> signing -> strict, and reverse on disable
// - define replay protection and canonical query/body coverage
// - add rotation, startup, and tampering tests

const (
	svNumBase     = 36  // base for all signed int64/uint64 fields
	svOverheadURL = 160 // pid+utm+vpams+x+u qparams (~145 worst-case)
)

type (
	signer struct {
		r       *http.Request
		h       *htrun
		sb      *cos.SB
		sig     []byte
		smapVer int64
		nonce   uint64
	}
)

func sigLen() int {
	debug.Assert(base64.RawURLEncoding.EncodedLen(cos.NodeSigningSignatureSize) == 86)
	return 86
}

// canonical payload -- byte-identical layout to signer.compute (method, path,
// pid, smap-ver, content-length, nonce). Writes into sb; returns sb[:len].
func (sign *signer) svPayload(pid string) []byte {
	const (
		sepa = 0 // strings separator
	)

	r, sb := sign.r, sign.sb

	sb.WriteString(r.Method)
	sb.WriteUint8(sepa)
	sb.WriteString(r.URL.Path)
	sb.WriteUint8(sepa)
	sb.WriteString(pid)
	sb.WriteUint8(sepa)

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
func (sign *signer) sign(pid string) {
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
func (sign *signer) verify(pid, b64sig string) (int, error) {
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

func (sign *signer) buildURL(nodeURL string, now int64) string {
	var (
		h  = sign.h
		r  = sign.r
		sb = sign.sb
	)
	// 1) build the query string while `sign.sig` still points into sb.buf
	q := qAlloc()
	raw := h.qencode(q, now, sign)
	qFree(q)

	// 2) reuse sb for final URL
	size := len(nodeURL) + len(r.URL.Path) + len(r.URL.RawQuery) + len(raw) + 2
	debug.Assert(sb.Cap() >= size, sb.Cap(), " vs ", size)
	sb.Reset(size, false)

	sb.WriteString(nodeURL)
	sb.WriteString(r.URL.Path)
	sb.WriteUint8('?')

	if r.URL.RawQuery != "" {
		sb.WriteString(r.URL.RawQuery)
		sb.WriteUint8('&')
	}

	sb.WriteString(raw)
	return sb.CloneString()
}

//
// mem-pools
//

var (
	sbPool = sync.Pool{New: func() any { return new(cos.SB) }} // reusable buffers
)

func sbAlloc() *cos.SB  { return sbPool.Get().(*cos.SB) }
func sbFree(sb *cos.SB) { sbPool.Put(sb) }

// via h.checkIntraCall (compare with svgrpFromQ())
func svgrpFromHdr(hdr http.Header) (*svgrp, error) {
	sig := hdr.Get(apc.HdrSenderSig)
	if sig == "" {
		return nil, nil
	}
	if len(sig) != sigLen() {
		return nil, fmt.Errorf("invalid %s length: %d", apc.HdrSenderSig, len(sig))
	}
	nonce, err := strconv.ParseUint(hdr.Get(apc.HdrSenderNonce), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", apc.HdrSenderNonce, err)
	}
	var smapVer int64
	if v := hdr.Get(apc.HdrSenderSmapVer); v != "" {
		if smapVer, err = strconv.ParseInt(v, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", apc.HdrSenderSmapVer, err)
		}
	}
	return &svgrp{sig: sig, nonce: nonce, smapVer: smapVer}, nil
}
