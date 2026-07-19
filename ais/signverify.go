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
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
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
// TODO:
// - add canonical query/body coverage
// - enforce startup ordering: restarted node must not originate signed traffic before its new Smap entry gets propagated
// - tests: add TestSignVerifyToggleStress and run it in parallel w/ TestSmoke/prefetch/get-batch
// - add rotation, startup, and tampering tests

const (
	svNumBase     = 36  // base for all signed int64/uint64 fields
	svOverheadURL = 160 // pid+utm+vpams+x+u qparams (~145 worst-case)
)

// sign/verify request
type (
	svReq struct {
		r  *http.Request
		h  *htrun
		sb *cos.SB
		// signature (string or bytes) and its context
		sig     []byte // raw
		b64sig  string // base64
		smapVer int64
		nonce   uint64
	}
)

// sign/verify global state
type (
	_sv struct {
		on   bool
		last int64 // last `config.auth.intra_cluster.enabled` toggle
	}
	svState struct {
		cur   atomic.Pointer[_sv]
		nonce atomic.Uint64 // per-node monotonic signing nonce
	}
)

/////////////
// svState //
/////////////

func (h *htrun) toggleSignVerify(enabled bool) {
	h.svs.set(enabled)
}

func (svs *svState) init() {
	svs.nonce.Store(uint64(cos.CryptoRandI())) // cryptorand.Read => uint16

	now := mono.NanoTime()
	svs.cur.Store(&_sv{
		on:   false,                  // off until cluster-started (at least until)
		last: now - int64(time.Hour), // expired at init time
	})
}

func (svs *svState) set(on bool) {
	cur := svs.cur.Load()
	if cur.on == on {
		return
	}
	upd := &_sv{
		on:   on,
		last: mono.NanoTime(),
	}
	svs.cur.Store(upd)
}

func (svs *svState) sign() bool {
	if cmn.IsV50Bridge() {
		return false
	}

	on := cmn.Rom.SignVerifyEnabled()
	cur := svs.cur.Load()

	// A note on stateful (config <=> htrun.svs) redundancy:
	// - the config.Auth.IntraCluster.Enabled expresses desired policy
	// - htrun.svs.cur.on expresses whether and when that policy has been enacted locally

	return (on && cur.on == on) || !svs.graceExpired(cur.last)
}

func (*svState) graceExpired(last int64) bool {
	maxrtt := max(cmn.Rom.CplaneOperation(), 2*time.Second)
	window := cos.ClampDuration(cmn.Rom.MaxKeepalive(), 3*maxrtt, 10*maxrtt)
	debug.Assert(window < time.Hour>>2) // see svs.init()
	return mono.Since(last) > window
}

func (svs *svState) strict() bool {
	if cmn.IsV50Bridge() {
		return false
	}
	on := cmn.Rom.SignVerifyEnabled()
	cur := svs.cur.Load()

	if !on || cur.on != on {
		return false
	}
	return svs.graceExpired(cur.last)
}

///////////
// svReq //
///////////

func sigLen() int {
	debug.Assert(base64.RawURLEncoding.EncodedLen(cos.NodeSigningSignatureSize) == 86)
	return 86
}

//
// send-side: sign the redirect with this node's private key
//

func newSigner(r *http.Request, h *htrun, sb *cos.SB, svs *svState, smapVer int64) (sv *svReq) {
	nonce := svs.nonce.Add(1)
	sv = &svReq{
		r:       r,
		h:       h,
		sb:      sb,
		smapVer: smapVer,
		nonce:   nonce,
	}
	return
}

// sv.sig ends up pointing into sb (buildURL/qencode clone it), same as compute().
func (sv *svReq) sign(pid string) {
	debug.Assert(sv.h.nodeKeyPair != nil)

	sv.sb.Reset(sv.bufsizeSV(pid), false /*allow shrink*/) // borrow redurl's sb

	msg := sv.payload(pid)
	raw, err := cos.SignNodeMessage(sv.h.nodeKeyPair.SigningKey, msg)
	debug.AssertNoErr(err)

	// sv.sig points into redurl's sb, same as compute()
	sv.sig = sv.sb.ReserveAppend(sigLen())
	base64.RawURLEncoding.Encode(sv.sig, raw)
}

//
// receive side: verify a signed (svgrp) request from snode
//

func newVerifier(r *http.Request, h *htrun, svgrp *svgrp) (sv *svReq) {
	sv = &svReq{
		r: r,
		h: h,
	}
	if svgrp != nil {
		sv.smapVer = svgrp.smapVer
		sv.nonce = svgrp.nonce
		sv.b64sig = svgrp.sig
	}
	return
}

func (sv *svReq) verify(sid string, snode *meta.Snode, smap *smapX) (int, error) {
	const (
		na = http.StatusUnauthorized
	)
	if l := len(sv.b64sig); l != sigLen() {
		if l == 0 {
			return na, fmt.Errorf("missing signature [smapVer %d vs %s]", sv.smapVer, smap.String())
		}
		return na, fmt.Errorf("invalid signature length: %d", l)
	}
	raw, err := base64.RawURLEncoding.DecodeString(sv.b64sig)
	if err != nil {
		return na, fmt.Errorf("invalid signature encoding: %w", err)
	}

	if snode == nil {
		e := fmt.Errorf(fmtNodeNotPresent, sid, smap)
		return na, fmt.Errorf("cannot verify request: %v, smapVer=%d", e, sv.smapVer)
	}
	debug.Assert(snode.ID() == sid)

	if len(snode.VerifyingKey) != cos.NodeSigningPublicKeySize {
		return na, fmt.Errorf("sender %s: no verifying key", snode.StringEx())
	}

	sv.sb = sbAlloc()
	sv.sb.Reset(sv.bufsizeSV(sid), true /*allow shrink*/)
	msg := sv.payload(sid)
	verr := cos.VerifyNodeSignature(snode.VerifyingKey, msg, raw)
	sbFree(sv.sb)

	if verr != nil {
		return na, verr
	}
	return 0, nil
}

//
// common signing&verifying helpers
//

// canonical payload: (method, path, pid, smap-ver, content-length, nonce)
func (sv *svReq) payload(pid string) []byte {
	const (
		sepa = 0 // strings separator
	)

	r, sb := sv.r, sv.sb

	sb.WriteString(r.Method)
	sb.WriteUint8(sepa)
	sb.WriteString(r.URL.Path)
	sb.WriteUint8(sepa)
	sb.WriteString(pid)
	sb.WriteUint8(sepa)

	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], uint64(sv.smapVer))
	sb.WriteBytes(b8[:])
	binary.BigEndian.PutUint64(b8[:], uint64(max(r.ContentLength, 0)))
	sb.WriteBytes(b8[:])
	binary.BigEndian.PutUint64(b8[:], sv.nonce)
	sb.WriteBytes(b8[:])

	return sb.Bytes()[:sb.Len()]
}

func (sv *svReq) bufsizeSV(pid string) int {
	r := sv.r
	return len(r.Method) + 1 + len(r.URL.Path) + 1 + len(pid) + 1 + 3*cos.SizeofI64 + sigLen()
}

func (sv *svReq) buildURL(nodeURL string, now int64) string {
	var (
		h  = sv.h
		r  = sv.r
		sb = sv.sb
	)
	// 1) build the query string while `sv.sig` still points into sb.buf
	q := qAlloc()
	raw := h.qencode(q, now, sv)
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

//
// misc helper - via h.checkIntra (compare with svgrpFromQ())
//

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

//
// transport subsystem: sign/verify
//

// to separate transport authentication from signed HTTP requests and redirect URLs
// (which use a different canonical payload - see above)
const streamSigDomain = "ais-transport-v1"

// Canonical payload:
// | domain | 0 | trname | 0 | sender-ID | 0 | session-ID | Smap-version | nonce |
// Strings are separated with NUL; integers are fixed-width big-endian.
func _streamPayloadSize(trname, senderID string) int {
	return len(streamSigDomain) + 1 + len(trname) + 1 + len(senderID) + 1 + 3*cos.SizeofI64
}

//
// Tx: sign stream-connect (both stdlib and fasthttp clients)
//

func (t *target) streamSign(trname string, sessID int64) (auth transport.Auth) {
	smap := t.owner.smap.get()
	debug.Assert(smap.isValid())

	smapVer := smap.version()
	auth.SmapVer = smapVer
	if !t.svs.sign() {
		return
	}

	debug.Assert(t.nodeKeyPair != nil)
	var (
		senderID = t.SID()
		nonce    = t.svs.nonce.Add(1)
		sb       = sbAlloc()
	)

	sb.Reset(_streamPayloadSize(trname, senderID), false /*allow shrink*/)
	msg := _streamPayload(sb, trname, senderID, sessID, smapVer, nonce)

	raw, err := cos.SignNodeMessage(t.nodeKeyPair.SigningKey, msg)
	debug.AssertNoErr(err)
	sbFree(sb)

	auth.Nonce = nonce
	auth.Sig = base64.RawURLEncoding.EncodeToString(raw)
	return
}

//
// Rx: verify stream-connect
//

func (t *target) streamVerify(trname string, sessID int64, senderID string, r *http.Request) error {
	if net := _reqNet(r); net != reqNetCtrl && net != reqNetData {
		return fmt.Errorf("stream %s[%d] from %s: invalid arrival network %s", trname, sessID, senderID, reqNetName(net))
	}

	smap := t.owner.smap.get()
	debug.Assert(smap.isValid())

	// Sender identity and cluster membership are required even while unsigned
	// traffic remains permitted during bridge/grace operation.
	snode := smap.GetNode(senderID)
	if snode == nil {
		return fmt.Errorf(fmtNodeNotPresent, senderID, smap)
	}
	debug.Assert(snode.ID() == senderID)

	svgrp, err := svgrpFromHdr(r.Header)
	if err != nil {
		return fmt.Errorf("stream %s[%d] from %s: %w", trname, sessID, senderID, err)
	}

	if svgrp == nil {
		if t.svs.strict() {
			return fmt.Errorf("stream %s[%d] from %s: missing signature (%s)", trname, sessID, senderID, smap.StringEx())
		}
		return nil
	}

	// a present signature is verified regardless
	if len(snode.VerifyingKey) != cos.NodeSigningPublicKeySize {
		return fmt.Errorf("stream sender %s: no verifying key", snode.StringEx())
	}

	raw, err := base64.RawURLEncoding.DecodeString(svgrp.sig)
	if err != nil {
		return fmt.Errorf("stream %s[%d] from %s: invalid signature encoding: %w", trname, sessID, senderID, err)
	}
	debug.Assert(len(raw) == cos.NodeSigningSignatureSize)

	sb := sbAlloc()
	sb.Reset(_streamPayloadSize(trname, senderID), false /*allow shrink*/)
	msg := _streamPayload(sb, trname, senderID, sessID, svgrp.smapVer, svgrp.nonce)
	err = cos.VerifyNodeSignature(snode.VerifyingKey, msg, raw)
	sbFree(sb)

	if err != nil {
		return fmt.Errorf("stream %s[%d] from %s: %w", trname, sessID, senderID, err)
	}
	return nil
}

func _streamPayload(sb *cos.SB, trname, senderID string, sessID, smapVer int64, nonce uint64) []byte {
	const (
		stringSepa = uint8(0)
	)
	sb.WriteString(streamSigDomain)
	sb.WriteUint8(stringSepa)
	sb.WriteString(trname)
	sb.WriteUint8(stringSepa)
	sb.WriteString(senderID)
	sb.WriteUint8(stringSepa)

	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], uint64(sessID))
	sb.WriteBytes(b8[:])
	binary.BigEndian.PutUint64(b8[:], uint64(smapVer))
	sb.WriteBytes(b8[:])
	binary.BigEndian.PutUint64(b8[:], nonce)
	sb.WriteBytes(b8[:])

	return sb.Bytes()[:sb.Len()]
}
