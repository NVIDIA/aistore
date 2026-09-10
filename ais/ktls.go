// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// kTLS TX offload (Linux) on a target's public HTTPS listener; sendfile remains
// scoped to user GET(object).
//
// RX stays in crypto/tls for the lifetime of the connection. TX is crypto/tls
// until the connection is _armed_, and the kernel afterwards: plaintext handed
// to the TCP socket, TLS records on the wire - which is what makes sendfile(2)
// usable on the HTTPS path.
//
// Arming is best-effort by construction: every failure leaves the connection
// on userspace crypto/tls. Two constraints apply to the listener as a whole,
// armed or not: TLS session tickets are disabled (TLS 1.3 record-sequence
// prerequisite) and ALPN is pinned to http/1.1.

// TODO:
// - end-to-end benches: plain HTTP => HTTPS => (HTTPS + kTLS + sendfile).
//   The transmit-path micro-benches only show that per-write bookkeeping is
//   noise next to write(2); they say nothing about whether the offload pays.
//
// - the per-key transmit budget is wrong twice over:
//   (a) it counts bytes, but the AEAD limit counts records. kTLS emits one
//       record per send() and splits sendfile at 16KiB, so a small-object GET
//       workload spends records far faster than ktlsMaxBytes assumes. Count
//       records - or read the truth from getsockopt(SOL_TLS, TLS_TX), which
//       returns the live rec_seq (kernel: do_tls_getsockopt_conf).
//   (b) the ceiling is soft: `reserve` skips `exhaust` while txRetiring is set,
//       and txRetiring is never cleared - so the exemption is permanent for the
//       connection, not scoped to the one response that set it.
//   Per RFC 9846 (obsoletes RFC 8446) update-or-close MUST happen _before_ the
//   limit is reached, which also puts the decision at the start of the response
//   rather than at its end.
//   Fix: TLS 1.3 KeyUpdate - derive application_traffic_secret_N+1, emit the
//   KeyUpdate record through the existing cmsg path (record type 22 in place of
//   21), re-install TLS_TX with rec_seq = 0. The kernel supports it: a second
//   setsockopt(TLS_TX) on an armed socket is a rekey, TLS 1.3 only (TLS 1.2 and
//   pre-rekey kernels return EBUSY and keep retire-and-close). Must be
//   update_not_requested - update_requested obliges the peer to answer, and
//   crypto/tls would try to write that answer and poison the connection.
//
// - ktlsRetire coverage: coldStream passes res.Size, which is -1 under
//   Streaming-Cold-GET, and ktlsRetire no-ops on a negative size; _txarch
//   multi-match passes lom.Lsize() for a tar it has not produced yet. Both can
//   still reach `exhaust` mid-body - the very thing retirement exists to avoid.
//
// - observability: nothing outside the node can tell armed from unsupported.
//   Publish ktlsCnt, and /proc/net/tls_stat for the software-vs-hardware split
//   (TlsCurrTxSw vs TlsCurrTxDevice) - the kernel chooses HW or SW itself and
//   setsockopt succeeds either way. Ad hoc: `ss -tin | grep -B1 tcp-ulp-tls`
//   shows "txconf: sw|hw". No integration test or CI job yet; when there is one
//   it must assert armed > 0 - a missing TLS ULP falls back silently.
//
// - lazy arming: install TLS_TX on the first response that would use sendfile,
//   not at handshake. Ideally, must only be used for large payloads.
//
// - classify errKtlsExhausted/errKtlsPoisoned as connection-lifecycle
//   events: they are neither object-transmit errors nor FSHC input (tgtfshc)
//
// - Go 1.27: net/http drives the handshake via connectionStater +
//   handshakeContexter, making ktlsConn.timeout and netServer._timeout()
//   redundant (and restoring handshake-error logging). NextProtos stays
//   load-bearing there - the h2 handoff accepts any net.Conn, not just *tls.Conn.

// ktlsConn.txState
const (
	ktlsUnarmed   uint32 = iota
	ktlsArmed            // TLS_TX installed; the kernel owns transmit
	ktlsPoisoned         // crypto/tls tried to transmit anyway - unrecoverable
	ktlsExhausted        // per-key transmit budget reached; socket closed
)

type (
	// TLS 1.3: server application traffic secret
	// TLS 1.2: master secret and client random
	trafficSecrets struct {
		mu                sync.Mutex
		pending           []byte
		serverApp         []byte
		tls12Master       []byte
		tls12ClientRandom [32]byte
		haveTLS12         bool
		disabled          bool
	}

	// observe the outbound TLS record stream while crypto/tls owns TX;
	// TLS 1.2 exposes both facts needed at takeover on the wire:
	// ServerHello.Random and the ChangeCipherSpec boundary.
	tls12WireState struct {
		mu sync.Mutex

		header           [5]byte
		headerN          int
		recordType       byte
		recordLen        int
		payloadRemaining int

		serverHelloPrefix [38]byte // handshake header + version + random
		serverHelloN      int
		serverRandom      [32]byte
		haveServerRandom  bool

		ccsByte   byte
		ccsN      int
		active    bool
		recordSeq [8]byte
		invalid   bool
		stopped   bool
	}

	// the net.Conn handed to tls.Server: rejects crypto/tls writes once the
	// kernel owns TX, so that the two encryptors can never interleave
	tlsArmedConn struct {
		*net.TCPConn
		txState *atomic.Uint32
		tls12   tls12WireState

		// what crypto/tls has put on the wire (used by unit tests)
		nwritten atomic.Int64
	}

	ktlsParams struct {
		version      uint16
		cipherSuite  uint16
		secret       []byte // TLS 1.3 traffic secret or TLS 1.2 master secret
		clientRandom [32]byte
		serverRandom [32]byte
		recordSeq    [8]byte
	}

	// - true, nil:  TLS_TX installed; the kernel owns TX from here on
	// - false, nil: unsupported kernel, cipher, or anything else
	// - false, err: installation failed
	// point of no return: no possible error _after_ TLS_TX has been installed
	ktlsInstaller func(tcp *net.TCPConn, params *ktlsParams) (bool, error)

	ktlsConn struct {
		*tls.Conn

		tcp     *net.TCPConn
		wire    *tlsArmedConn
		cfg     *tls.Config // effective (per-connection) config
		secrets *trafficSecrets
		install ktlsInstaller
		timeout time.Duration // handshake deadline; see (*ktlsConn).init

		once    sync.Once
		initErr error

		txState  atomic.Uint32 // unarmed -> armed -> poisoned/exhausted
		txMu     sync.Mutex    // serialize the TLS_TX point of no return against transmit shutdown
		txOut    sync.Mutex    // serialize kTLS application data and close_notify
		txClosed atomic.Bool   // prevent completed Close/CloseWrite from being followed by arm() (stored under txMu vs arm())

		// per-key plaintext transmit budget
		txBytes    atomic.Int64
		txMaxBytes int64
		txRetiring atomic.Bool // current response is the last one

		closeOnce sync.Once
		closeErr  error
	}

	ktlsListener struct {
		net.Listener

		tlsConfig    *tls.Config // template; cloned per connection
		install      ktlsInstaller
		configureTCP func(*net.TCPConn)
		timeout      time.Duration // handshake timeout
	}

	ktlsState interface {
		isArmed() bool
		retire(size int64) bool
	}

	// basic offload observability; see the "observability" section below
	ktlsCounters struct {
		armed       atomic.Int64 // TLS_TX installed; the kernel owns transmit
		skipped     atomic.Int64 // arm() bailed before reaching the installer
		unsupported atomic.Int64 // kernel, TLS version, or cipher declined
		failed      atomic.Int64 // installation error
		poisoned    atomic.Int64 // crypto/tls attempted to transmit after offload
		exhausted   atomic.Int64 // per-key transmit budget reached
	}
	ktlsCtxKey struct{}

	// hides io.ReaderFrom from io.Copy (cf. net/http writerOnly)
	noReadFrom struct{ io.Writer }
)

// TODO -- FIXME: placement
var (
	keyKtls ktlsCtxKey
	ktlsCnt ktlsCounters // node-wide kTLS-offload observability
)

var (
	errKtlsActive    = errors.New("ktls-tx: kernel owns TX; crypto/tls must not write")
	errKtlsPoisoned  = errors.New("ktls-tx: crypto/tls attempted to transmit after offload; connection closed")
	errKtlsExhausted = errors.New("ktls-tx: per-key transmit budget reached; connection closed")
)

const (
	// RFC 8446 section 5.5 permits about 2^24.5 full-size AES-GCM
	// records under one key. Retire earlier, after 256 GiB of plaintext.
	ktlsMaxBytes = int64(1 << 38)
	ktlsHeadroom = int64(64 * cos.KiB) // response headers and net/http framing

	tlsRecordHeaderSize           = 5
	tlsRecordTypeChangeCipherSpec = 20
	tlsRecordTypeHandshake        = 22
	tlsHandshakeTypeServerHello   = 2

	// net/http: an exact match is what sets closeAfterReply
	hdrConnection      = "Connection"
	hdrConnectionClose = "close"
)

////////////////////
// trafficSecrets //
////////////////////

func newTrafficSecrets() *trafficSecrets {
	return &trafficSecrets{}
}

func (s *trafficSecrets) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disabled {
		return len(p), nil
	}

	s.pending = append(s.pending, p...)

	for {
		idx := bytes.IndexByte(s.pending, '\n')
		if idx < 0 {
			break
		}

		s.consumeLine(s.pending[:idx])

		oldLen := len(s.pending)
		n := copy(s.pending, s.pending[idx+1:])
		clear(s.pending[n:oldLen])
		s.pending = s.pending[:n]
	}

	// never fail the TLS handshake
	return len(p), nil
}

// under lock
func (s *trafficSecrets) consumeLine(line []byte) {
	fields := bytes.Fields(line)
	if len(fields) != 3 {
		return
	}

	switch string(fields[0]) {
	case "SERVER_TRAFFIC_SECRET_0":
		secret, ok := decodeKeyLogHex(fields[2], 0)
		if !ok {
			return
		}
		clear(s.serverApp)
		s.serverApp = secret

	case "CLIENT_RANDOM":
		clientRandom, ok := decodeKeyLogHex(fields[1], len(s.tls12ClientRandom))
		if !ok {
			return
		}
		master, ok := decodeKeyLogHex(fields[2], tls12MasterSecretSize)
		if !ok {
			clear(clientRandom)
			return
		}

		clear(s.tls12Master)
		clear(s.tls12ClientRandom[:])
		copy(s.tls12ClientRandom[:], clientRandom)
		clear(clientRandom)
		s.tls12Master = master
		s.haveTLS12 = true
	}
}

func decodeKeyLogHex(src []byte, expected int) ([]byte, bool) {
	dst := make([]byte, hex.DecodedLen(len(src)))
	n, err := hex.Decode(dst, src)
	if err != nil || (expected > 0 && n != expected) {
		clear(dst)
		return nil, false
	}
	return dst[:n], true
}

// transfers ownership to the caller, which must clear the returned secret
func (s *trafficSecrets) takeTLS13() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	secret := s.serverApp
	s.serverApp = nil
	return secret
}

// transfers ownership to the caller, which must clear the returned master secret
func (s *trafficSecrets) takeTLS12() (master []byte, clientRandom [32]byte, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.haveTLS12 || len(s.tls12Master) == 0 {
		return nil, clientRandom, false
	}
	master = s.tls12Master
	clientRandom = s.tls12ClientRandom
	s.tls12Master = nil
	clear(s.tls12ClientRandom[:])
	s.haveTLS12 = false
	return master, clientRandom, true
}

// wipes the stored key material; note that `pending` may hold a partial keylog line
func (s *trafficSecrets) zero() {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.serverApp)
	s.serverApp = nil
	clear(s.tls12Master)
	s.tls12Master = nil
	clear(s.tls12ClientRandom[:])
	s.haveTLS12 = false
	clear(s.pending[:cap(s.pending)])
	s.pending = nil
	s.disabled = true
}

////////////////////
// tls12WireState //
////////////////////

func (s *tls12WireState) observe(p []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.invalid || s.stopped {
		return
	}
	for len(p) > 0 {
		if s.payloadRemaining == 0 && s.headerN < tlsRecordHeaderSize {
			n := copy(s.header[s.headerN:], p)
			s.headerN += n
			p = p[n:]
			if s.headerN < tlsRecordHeaderSize {
				return
			}

			s.recordType = s.header[0]
			s.recordLen = int(binary.BigEndian.Uint16(s.header[3:5]))
			s.payloadRemaining = s.recordLen
			if s.recordLen == 0 {
				s.finishRecord()
				continue
			}
		}

		n := min(len(p), s.payloadRemaining)
		payload := p[:n]
		if s.recordType == tlsRecordTypeHandshake && !s.active && !s.haveServerRandom {
			s.observeServerHello(payload)
		}
		if s.recordType == tlsRecordTypeChangeCipherSpec {
			if s.ccsN < 1 && len(payload) > 0 {
				s.ccsByte = payload[0]
			}
			s.ccsN += len(payload)
		}

		s.payloadRemaining -= n
		p = p[n:]
		if s.payloadRemaining == 0 {
			s.finishRecord()
		}
	}
}

// TLS handshake messages may span records; only the fixed ServerHello prefix
// is retained, never certificates or other handshake data.
func (s *tls12WireState) observeServerHello(p []byte) {
	if s.serverHelloN == len(s.serverHelloPrefix) {
		return
	}
	n := copy(s.serverHelloPrefix[s.serverHelloN:], p)
	s.serverHelloN += n
	if s.serverHelloN < len(s.serverHelloPrefix) {
		return
	}

	msgLen := int(s.serverHelloPrefix[1])<<16 |
		int(s.serverHelloPrefix[2])<<8 |
		int(s.serverHelloPrefix[3])
	if s.serverHelloPrefix[0] != tlsHandshakeTypeServerHello || msgLen < 34 {
		s.invalid = true
		return
	}
	copy(s.serverRandom[:], s.serverHelloPrefix[6:38])
	s.haveServerRandom = true
}

func (s *tls12WireState) finishRecord() {
	if s.recordType == tlsRecordTypeChangeCipherSpec {
		if s.recordLen != 1 || s.ccsN != 1 || s.ccsByte != 1 {
			s.invalid = true
		} else {
			clear(s.recordSeq[:])
			s.active = true
		}
	} else if s.active && !incrementRecordSeq(&s.recordSeq) {
		s.invalid = true
	}

	s.headerN = 0
	s.recordType = 0
	s.recordLen = 0
	s.payloadRemaining = 0
	s.ccsByte = 0
	s.ccsN = 0
}

func incrementRecordSeq(seq *[8]byte) bool {
	for i := len(seq) - 1; i >= 0; i-- {
		seq[i]++
		if seq[i] != 0 {
			return true
		}
	}
	return false
}

func (s *tls12WireState) state() (serverRandom [32]byte, recordSeq [8]byte, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ok = !s.invalid && !s.stopped && s.haveServerRandom && s.active && s.headerN == 0 && s.payloadRemaining == 0
	return s.serverRandom, s.recordSeq, ok
}

func (s *tls12WireState) stop() {
	s.mu.Lock()
	clear(s.header[:])
	clear(s.serverHelloPrefix[:])
	clear(s.serverRandom[:])
	clear(s.recordSeq[:])
	s.stopped = true
	s.mu.Unlock()
}

//////////////////
// tlsArmedConn //
//////////////////

func (c *tlsArmedConn) Write(p []byte) (int, error) {
	if c.txState.Load() != ktlsUnarmed {
		// A peer-requested TLS 1.3 KeyUpdate reaches this crypto/tls _read_
		// path. Its response cannot be handed to the kernel after offload, so
		// fail closed: mark the connection and let Read/Write/ReadFrom refuse it.
		if c.txState.CompareAndSwap(ktlsArmed, ktlsPoisoned) {
			cnt := ktlsCnt.poisoned.Add(1)
			if cmn.Rom.V(5, cos.ModAIS) || cos.Sparse(cnt) {
				nlog.Errorln("ktls-tx: crypto/tls attempted to transmit after offload; closing the connection -",
					&ktlsCnt)
			}
			_ = c.TCPConn.Close()
		}
		return 0, errKtlsActive
	}

	n, err := c.TCPConn.Write(p)
	c.nwritten.Add(int64(n))
	if n > 0 {
		c.tls12.observe(p[:n])
	}
	return n, err
}

// NOTE:
// - do not promote (*net.TCPConn).ReadFrom
// - every outbound crypto/tls byte must pass through Write for pre-arm/post-arm
func (c *tlsArmedConn) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(noReadFrom{c}, r)
}

////////////////
// ktlsConn //
////////////////

func newKtlsConn(tcp *net.TCPConn, tmpl *tls.Config, install ktlsInstaller, timeout time.Duration) *ktlsConn {
	c := &ktlsConn{
		tcp:     tcp,
		secrets: newTrafficSecrets(),
		install: install,
		timeout: timeout,

		txMaxBytes: ktlsMaxBytes,
	}
	c.wire = &tlsArmedConn{TCPConn: tcp, txState: &c.txState}

	// KeyLogWriter is per-tls.Config, hence the per-connection clone; everything
	// else is already set on the listener's template
	cfg := tmpl.Clone()
	cfg.KeyLogWriter = c.secrets

	c.cfg = cfg
	c.Conn = tls.Server(c.wire, cfg)
	return c
}

// NOTE important sequence: handshake => armed
func (c *ktlsConn) init(ctx context.Context) error {
	c.once.Do(func() { c.initErr = c.handshakeAndArm(ctx) })
	return c.initErr
}
func (c *ktlsConn) HandshakeContext(ctx context.Context) error { return c.init(ctx) }

func (c *ktlsConn) handshakeAndArm(ctx context.Context) error {
	// KeyLogWriter may receive secrets before a handshake ultimately fails.
	// Wipe every outcome and permanently discard any later key-log writes.
	defer c.secrets.zero()
	defer c.wire.tls12.stop()

	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()

		if err := c.tcp.SetDeadline(time.Now().Add(c.timeout)); err != nil {
			return err
		}
		defer c.tcp.SetDeadline(time.Time{})
	}

	// all ordinary crypto/tls certificate, client-certificate, Finished, ALPN,
	// and VerifyConnection processing happens here
	if err := c.Conn.HandshakeContext(ctx); err != nil {
		return err
	}

	c.arm()
	return nil
}

func (c *ktlsCounters) String() string {
	armed, skipped := c.armed.Load(), c.skipped.Load()
	unsupported, failed := c.unsupported.Load(), c.failed.Load()
	poisoned, exhausted := c.poisoned.Load(), c.exhausted.Load()
	return fmt.Sprintf("ktls-tx[attempted=%d armed=%d skipped=%d unsupported=%d failed=%d poisoned=%d exhausted=%d]",
		armed+skipped+unsupported+failed, armed, skipped, unsupported, failed, poisoned, exhausted)
}

// arm() bailed on its own, before reaching the installer: a missing secret,
// session tickets, an unusable TLS version. Always a bug or a misconfiguration
// on our side, never the kernel's.
func (c *ktlsConn) skip(reason string) {
	ktlsCnt.skipped.Add(1)
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln("ktls-tx: not arming:", reason, c.tcp.RemoteAddr(), &ktlsCnt)
	}
}

func (c *ktlsConn) arm() {
	state := c.Conn.ConnectionState()
	params := ktlsParams{version: state.Version, cipherSuite: state.CipherSuite}
	switch state.Version {
	case tls.VersionTLS13:
		// The TLS 1.3 record sequence number resets to zero at every key
		// change. Session tickets are the known crypto/tls write under the
		// server application epoch during the handshake, hence they must be off.
		if !c.cfg.SessionTicketsDisabled {
			nlog.Errorln("ktls-tx: refusing to arm - session tickets are enabled")
			c.skip("session tickets are enabled")
			return
		}
		params.secret = c.secrets.takeTLS13()

	case tls.VersionTLS12:
		var ok bool
		params.serverRandom, params.recordSeq, ok = c.wire.tls12.state()
		if !ok {
			c.skip("TLS 1.2 wire state is unavailable")
			return
		}
		params.secret, params.clientRandom, ok = c.secrets.takeTLS12()
		if !ok {
			c.skip("TLS 1.2 master secret is unavailable")
			return
		}

	default:
		c.skip(fmt.Sprintf("unsupported TLS version %#x", state.Version))
		return
	}
	if len(params.secret) == 0 {
		c.skip("keylog secret is unavailable")
		return
	}
	defer clear(params.secret)

	// Serialize the point of no return against Close and CloseWrite. If transmit
	// shutdown won, the socket must never be armed afterwards.
	c.txMu.Lock()
	if c.txClosed.Load() {
		c.txMu.Unlock()
		c.skip("transmit side is closed")
		return
	}
	enabled, err := c.install(c.tcp, &params)
	if enabled && err == nil {
		c.txState.Store(ktlsArmed)
	}
	c.txMu.Unlock()

	switch {
	case err != nil:
		// distinguished from `unsupported` on purpose (see ktlsUnsupported)
		cnt := ktlsCnt.failed.Add(1)
		if cmn.Rom.V(5, cos.ModAIS) || cos.Sparse(cnt) {
			nlog.Errorln("ktls-tx: install failed, continuing with crypto/tls:", err,
				c.tcp.RemoteAddr(), &ktlsCnt)
		}
		return
	case !enabled:
		ktlsCnt.unsupported.Add(1)
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.Infoln("ktls-tx: offload unavailable", tls.VersionName(params.version),
				tls.CipherSuiteName(params.cipherSuite), c.tcp.RemoteAddr(), &ktlsCnt)
		}
		return
	}

	ktlsCnt.armed.Add(1)
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln("ktls-tx: armed", tls.VersionName(params.version),
			tls.CipherSuiteName(params.cipherSuite), c.tcp.RemoteAddr(), &ktlsCnt)
	}
}

// Go 1.26 net/http uses ConnectionState on a non-*tls.Conn to populate
// Request.TLS; the call also triggers our handshake. Go 1.27+ calls
// HandshakeContext first and then uses ConnectionState for TLS state and ALPN.
func (c *ktlsConn) ConnectionState() tls.ConnectionState {
	_ = c.init(context.Background())
	return c.Conn.ConnectionState()
}

func (c *ktlsConn) Read(p []byte) (int, error) {
	if err := c.init(context.Background()); err != nil {
		return 0, err
	}
	switch c.txState.Load() {
	case ktlsPoisoned:
		return 0, errKtlsPoisoned
	case ktlsExhausted:
		return 0, errKtlsExhausted
	}
	return c.Conn.Read(p)
}

func (c *ktlsConn) Write(p []byte) (int, error) {
	if err := c.init(context.Background()); err != nil {
		return 0, err
	}
	switch c.txState.Load() {
	case ktlsArmed:
		// plaintext enters TCP; kTLS emits encrypted TLS records
		return c.write(p)
	case ktlsPoisoned:
		return 0, errKtlsPoisoned
	case ktlsExhausted:
		return 0, errKtlsExhausted
	}
	return c.Conn.Write(p)
}

func (c *ktlsConn) write(p []byte) (int, error) {
	c.txOut.Lock()
	defer c.txOut.Unlock()
	if c.txClosed.Load() {
		return 0, net.ErrClosed
	}

	reserved, ok := c.reserve(int64(len(p)))
	if !ok {
		return 0, c.stateErr()
	}
	n, err := c.tcp.Write(p)
	c.finish(reserved, int64(n))
	return n, err
}

func (c *ktlsConn) ReadFrom(r io.Reader) (int64, error) {
	if err := c.init(context.Background()); err != nil {
		return 0, err
	}
	switch c.txState.Load() {
	case ktlsArmed:
		// NOTE: sendfile path must use io.LimitedReader (see getOI._txreg in ais/tgtobj)
		lr, ok := r.(*io.LimitedReader)
		if !ok {
			return io.Copy(noReadFrom{c}, r)
		}

		c.txOut.Lock()
		defer c.txOut.Unlock()
		if c.txClosed.Load() {
			return 0, net.ErrClosed
		}

		reserved, ok := c.reserve(max(lr.N, 0))
		if !ok {
			return 0, c.stateErr()
		}
		n, err := c.tcp.ReadFrom(lr)
		c.finish(reserved, n)
		return n, err

	case ktlsPoisoned:
		return 0, errKtlsPoisoned
	case ktlsExhausted:
		return 0, errKtlsExhausted
	}
	return io.Copy(noReadFrom{c.Conn}, r)
}

// Reserve n plaintext bytes before transmitting.
// A transfer that crosses the per-key limit is allowed to complete. Once the
// current response is marked retiring, the allowance spans all of its writes.
// The limit itself carries margin
// (2^38 bytes vs. RFC 8446's approx. 2^24.5 full-size records).
// Retiring the connection _before_ it gets here is the Tx path's responsibility -
// see ktlsRetire.
// callers hold txOut, so txBytes needs no read-modify-write retry; it stays
// atomic for the lock-free reader (remaining).
func (c *ktlsConn) reserve(n int64) (int64, bool) {
	if n <= 0 {
		return 0, c.txState.Load() == ktlsArmed
	}
	if c.txState.Load() != ktlsArmed {
		return 0, false
	}
	cur := c.txBytes.Load()
	if cur >= c.txMaxBytes && !c.txRetiring.Load() {
		c.exhaust()
		return 0, false
	}
	c.txBytes.Store(cur + n)
	return n, true
}

func (c *ktlsConn) finish(reserved, written int64) {
	if written < reserved {
		c.txBytes.Add(written - reserved) // refund a short write
	}
}

func (c *ktlsConn) exhaust() {
	if !c.txState.CompareAndSwap(ktlsArmed, ktlsExhausted) {
		return
	}
	cnt := ktlsCnt.exhausted.Add(1)
	if cmn.Rom.V(5, cos.ModAIS) || cos.Sparse(cnt) {
		nlog.Warningln(errKtlsExhausted, c.tcp.RemoteAddr(), &ktlsCnt)
	}

	// record transmit shutdown under txMu, as beginClose does
	c.txMu.Lock()
	c.txClosed.Store(true)
	c.txMu.Unlock()

	_ = c.closeNotify() // closeOnce: idempotent vs. Close/CloseWrite
	_ = c.tcp.Close()
}

func (c *ktlsConn) stateErr() error {
	if c.txState.Load() == ktlsPoisoned {
		return errKtlsPoisoned
	}
	return errKtlsExhausted
}

func (c *ktlsConn) Close() error {
	switch c.beginClose() {
	case ktlsArmed:
		// Match crypto/tls: if Close races an active write, close the socket
		// immediately instead of waiting to send close_notify.
		if !c.txOut.TryLock() {
			return c.tcp.Close()
		}
		alertErr := c.closeNotify()
		c.txOut.Unlock()
		if err := c.tcp.Close(); err != nil {
			return err
		}
		return alertErr
	case ktlsPoisoned:
		// The first forbidden crypto/tls write already closed the socket; an
		// orderly close_notify would only make it worse.
		return errKtlsPoisoned
	case ktlsExhausted:
		return errKtlsExhausted
	}
	return c.Conn.Close()
}

func (c *ktlsConn) CloseWrite() error {
	switch c.beginClose() {
	case ktlsArmed:
		// As in crypto/tls, order close_notify after any active application
		// write and prevent every subsequent write.
		c.txOut.Lock()
		err := c.closeNotify()
		c.txOut.Unlock()
		return err
	case ktlsPoisoned:
		return errKtlsPoisoned
	case ktlsExhausted:
		return errKtlsExhausted
	}
	return c.Conn.CloseWrite()
}

func (c *ktlsConn) beginClose() uint32 {
	c.txMu.Lock()
	c.txClosed.Store(true)
	state := c.txState.Load()
	c.txMu.Unlock()
	return state
}

func (c *ktlsConn) closeNotify() error {
	c.closeOnce.Do(func() {
		_ = c.tcp.SetWriteDeadline(time.Now().Add(5 * time.Second))
		c.closeErr = ktlsCloseNotify(c.tcp)
		_ = c.tcp.SetWriteDeadline(time.Now())
	})
	return c.closeErr
}

// implements ktlsState
func (c *ktlsConn) isArmed() bool { return c.txState.Load() == ktlsArmed }

// remaining per-key plaintext transmit budget; zero when the kernel does not own TX
func (c *ktlsConn) remaining() int64 {
	if c.txState.Load() != ktlsArmed {
		return 0
	}
	return max(c.txMaxBytes-c.txBytes.Load(), 0)
}

// Mark the current response as final before net/http commits its headers.
// Once retiring, all writes belonging to this response may complete.
func (c *ktlsConn) retire(size int64) bool {
	if size < 0 || c.txState.Load() != ktlsArmed {
		return false
	}
	if c.txRetiring.Load() {
		return true
	}
	rem := c.remaining()
	if rem > ktlsHeadroom && size < rem-ktlsHeadroom {
		return false
	}
	c.txRetiring.Store(true)
	return true
}

////////////////////
// ktlsListener //
////////////////////

func newKtlsListener(ln net.Listener, tlsConf *tls.Config, timeout time.Duration,
	configureTCP func(*net.TCPConn)) (*ktlsListener, error) {
	if tlsConf == nil {
		return nil, errors.New("ktls-tx: nil TLS config")
	}
	if tlsConf.GetConfigForClient != nil {
		// stdlib crypto/tls replaces the entire per-connection config with the one this
		// callback returns - dropping KeyLogWriter (no secret, hence no offload,
		// silently) and SessionTicketsDisabled
		return nil, errors.New("ktls-tx: GetConfigForClient is not supported")
	}

	tmpl := tlsConf.Clone()
	tmpl.SessionTicketsDisabled = true // TLS 1.3 record-sequence prerequisite; see arm

	tmpl.NextProtos = []string{"http/1.1"}

	return &ktlsListener{
		Listener:     ln,
		tlsConfig:    tmpl,
		install:      ktlsInstall,
		configureTCP: configureTCP,
		timeout:      timeout,
	}, nil
}

// accept TCP, apply socket options, construct the hybrid connection, and return.
// No TLS handshake here - see (*ktlsConn).init.
func (l *ktlsListener) Accept() (net.Conn, error) {
	nc, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	tcp, ok := nc.(*net.TCPConn)
	if !ok {
		nc.Close()
		return nil, fmt.Errorf("ktls-tx: expected *net.TCPConn, got %T", nc)
	}

	if l.configureTCP != nil {
		l.configureTCP(tcp)
	}

	return newKtlsConn(tcp, l.tlsConfig, l.install, l.timeout), nil
}

func isKTLS(ctx context.Context) bool {
	state := ktlsFrom(ctx)
	return state != nil && state.isArmed()
}

func ktlsFrom(ctx context.Context) ktlsState {
	state, _ := ctx.Value(keyKtls).(ktlsState)
	return state
}

// Retire an armed connection at the _end_ of the current response, when transmitting
// `size` bytes would otherwise cross the per-key budget mid-body.
//
// Go stdlib net/http turns a handler-set "Connection: close" into closeAfterReply
// (ref: chunkWriter.writeHeader), so the client receives this response in full, is told
// the connection is done, and reconnects for the next one.
//
// Ineffective for a single object larger than the entire budget - see TODO above.
func ktlsRetire(state ktlsState, whdr http.Header, size int64) {
	if state == nil || size < 0 {
		return
	}
	if state.retire(size) {
		whdr.Set(hdrConnection, hdrConnectionClose)
	}
}

//
// TLS crypto-utilities: deriveTLS12KeyIV and deriveTLS13KeyIV
//

const (
	tls12MasterSecretSize = 48
	tls12IVSize           = 4
	tls13IVSize           = 12
)

// derive the server write key and fixed IV from the TLS 1.2 master secret (RFC 5246, Section 6.3)
// (unsupported cipher suites are a normal fallback, not an error)
func deriveTLS12KeyIV(cipherSuite uint16, masterSecret []byte, clientRandom, serverRandom [32]byte) (key, iv []byte, supported bool, err error) {
	var (
		hashFn func() hash.Hash
		keyLen int
	)
	switch cipherSuite {
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256:
		hashFn, keyLen = sha256.New, 16
	case tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384:
		hashFn, keyLen = sha512.New384, 32
	default:
		return nil, nil, false, nil
	}
	if len(masterSecret) != tls12MasterSecretSize {
		return nil, nil, true, errors.New("ktls-tx: invalid TLS 1.2 master-secret size")
	}

	seed := make([]byte, 0, len("key expansion")+len(serverRandom)+len(clientRandom))
	seed = append(seed, "key expansion"...)
	seed = append(seed, serverRandom[:]...)
	seed = append(seed, clientRandom[:]...)

	// AEAD suites have no MAC keys. The key block is:
	// client_write_key || server_write_key || client_write_IV || server_write_IV.
	keyBlock := tls12PHash(hashFn, masterSecret, seed, 2*keyLen+2*tls12IVSize)
	defer clear(keyBlock)
	key = append(key, keyBlock[keyLen:2*keyLen]...)
	ivOffset := 2*keyLen + tls12IVSize
	iv = append(iv, keyBlock[ivOffset:ivOffset+tls12IVSize]...)
	return key, iv, true, nil
}

func tls12PHash(hashFn func() hash.Hash, secret, seed []byte, length int) []byte {
	result := make([]byte, length)
	a := tls12HMAC(hashFn, secret, seed)
	defer clear(a)

	for offset := 0; offset < len(result); {
		mac := hmac.New(hashFn, secret)
		_, _ = mac.Write(a)
		_, _ = mac.Write(seed)
		block := mac.Sum(nil)
		offset += copy(result[offset:], block)
		clear(block)

		next := tls12HMAC(hashFn, secret, a)
		clear(a)
		a = next
	}
	return result
}

func tls12HMAC(hashFn func() hash.Hash, secret, data []byte) []byte {
	mac := hmac.New(hashFn, secret)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

// mirror crypto/tls TLS 1.3 trafficKey derivation (ditto re: unsupported cipher suites)
func deriveTLS13KeyIV(cipherSuite uint16, trafficSecret []byte) (key, iv []byte, supported bool, err error) {
	var (
		hashFn func() hash.Hash
		keyLen int
	)
	switch cipherSuite {
	case tls.TLS_AES_128_GCM_SHA256:
		hashFn, keyLen = sha256.New, 16
	case tls.TLS_AES_256_GCM_SHA384:
		hashFn, keyLen = sha512.New384, 32
	default:
		return nil, nil, false, nil
	}

	key, err = tls13ExpandLabel(hashFn, trafficSecret, "key", keyLen)
	if err != nil {
		return nil, nil, true, err
	}
	iv, err = tls13ExpandLabel(hashFn, trafficSecret, "iv", tls13IVSize)
	if err != nil {
		clear(key)
		return nil, nil, true, err
	}
	return key, iv, true, nil
}

// HKDF-Expand-Label with an empty context (RFC 8446, Sections 7.1 and 7.3)
func tls13ExpandLabel(hashFn func() hash.Hash, secret []byte, label string, length int) ([]byte, error) {
	const prefix = "tls13 "

	info := make([]byte, 0, 2+1+len(prefix)+len(label)+1)
	info = binary.BigEndian.AppendUint16(info, uint16(length))
	info = append(info, byte(len(prefix)+len(label)))
	info = append(info, prefix...)
	info = append(info, label...)
	info = append(info, 0) // empty context
	return hkdf.Expand(hashFn, secret, string(info), length)
}
