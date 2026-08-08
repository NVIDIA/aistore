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
// on userspace crypto/tls.

// TODO:
// - opt-in config/feature-flags knob; must remain experimental until it is not
//   - restart required
// - benches - ranging from plain HTTP to (HTTPS + kTLS + sendfile)
// - The problem: one TLS key has a usage limit. Specifically, with TLS 1.3,
//   an application traffic key is not meant to encrypt unlimited data.
//   With RFC-specified ktlsTxMaxBytes budget approx. 2^38 bytes, we conservatively
//   should impose a maximum single monolithic object size = 100GiB
//   The current implementation checks for retirement at HTTP response boundaries.
//   That only works when there are those "boundaries" - which is why
//   one super-large object is a separate task.
// - lazy arming: install TLS_TX on the first response that would use sendfile,
//   not at handshake. Ideally, must only be used for large payloads.
// - classify errKTLSTxExhausted/errKTLSTxPoisoned as connection-lifecycle
//   events: they are neither object-transmit errors nor FSHC input (tgtfshc)

// ktlsTxConn.txState
const (
	ktlsTxUnarmed   uint32 = iota
	ktlsTxArmed            // TLS_TX installed; the kernel owns transmit
	ktlsTxPoisoned         // crypto/tls tried to transmit anyway - unrecoverable
	ktlsTxExhausted        // per-key transmit budget reached; socket closed
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

	ktlsTxParams struct {
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
	ktlsTxInstaller func(tcp *net.TCPConn, params *ktlsTxParams) (bool, error)

	ktlsTxConn struct {
		*tls.Conn

		tcp     *net.TCPConn
		wire    *tlsArmedConn
		cfg     *tls.Config // effective (per-connection) config
		secrets *trafficSecrets
		install ktlsTxInstaller
		timeout time.Duration // handshake deadline; see (*ktlsTxConn).init

		once    sync.Once
		initErr error

		// txMu serializes the TLS_TX point of no return against transmit shutdown.
		// txClosed prevents a completed Close/CloseWrite from being followed by arm().
		txState  atomic.Uint32 // unarmed -> armed -> poisoned/exhausted
		txMu     sync.Mutex
		txClosed bool // protected by txMu

		// per-key plaintext transmit budget
		txBytes    atomic.Int64
		txMaxBytes int64
		txRetiring atomic.Bool // current response is the last one

		closeOnce sync.Once
		closeErr  error
	}

	ktlsTxListener struct {
		net.Listener

		tlsConfig    *tls.Config // template; cloned per connection
		install      ktlsTxInstaller
		configureTCP func(*net.TCPConn)
		timeout      time.Duration // handshake timeout
	}

	ktlsTxState interface {
		KTLSTxEnabled() bool
		KTLSTxRetire(size int64) bool
	}

	// basic offload observability; see the "observability" section below
	ktlsTxCounters struct {
		armed       atomic.Int64 // TLS_TX installed; the kernel owns transmit
		skipped     atomic.Int64 // arm() bailed before reaching the installer
		unsupported atomic.Int64 // kernel, TLS version, or cipher declined
		failed      atomic.Int64 // installation error
		poisoned    atomic.Int64 // crypto/tls attempted to transmit after offload
		exhausted   atomic.Int64 // per-key transmit budget reached
	}
	ktlsTxContextKey struct{}

	// hides io.ReaderFrom from io.Copy (cf. net/http writerOnly)
	noReadFrom struct{ io.Writer }
)

// TODO -- FIXME: placement
var (
	keyKTLSTx ktlsTxContextKey
	ktlsTxCnt ktlsTxCounters // node-wide kTLS-offload observability
)

var (
	errKTLSTxActive    = errors.New("ktls-tx: kernel owns TX; crypto/tls must not write")
	errKTLSTxPoisoned  = errors.New("ktls-tx: crypto/tls attempted to transmit after offload; connection closed")
	errKTLSTxExhausted = errors.New("ktls-tx: per-key transmit budget reached; connection closed")
)

const (
	// RFC 8446 section 5.5 permits about 2^24.5 full-size AES-GCM
	// records under one key. Retire earlier, after 256 GiB of plaintext.
	ktlsTxMaxBytes = int64(1 << 38)
	ktlsTxHeadroom = int64(64 * cos.KiB) // response headers and net/http framing

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

	s.pending = append(s.pending, p...)

	for {
		idx := bytes.IndexByte(s.pending, '\n')
		if idx < 0 {
			break
		}

		s.consumeLine(s.pending[:idx])

		n := copy(s.pending, s.pending[idx+1:])
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
	if c.txState.Load() != ktlsTxUnarmed {
		// A peer-requested TLS 1.3 KeyUpdate reaches this crypto/tls _read_
		// path. Its response cannot be handed to the kernel after offload, so
		// fail closed: mark the connection and let Read/Write/ReadFrom refuse it.
		if c.txState.CompareAndSwap(ktlsTxArmed, ktlsTxPoisoned) {
			cnt := ktlsTxCnt.poisoned.Add(1)
			if cmn.Rom.V(5, cos.ModAIS) || cos.Sparse(cnt) {
				nlog.Errorln("ktls-tx: crypto/tls attempted to transmit after offload; closing the connection -",
					&ktlsTxCnt)
			}
			_ = c.TCPConn.Close()
		}
		return 0, errKTLSTxActive
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
// ktlsTxConn //
////////////////

func newKTLSTxConn(tcp *net.TCPConn, tmpl *tls.Config, install ktlsTxInstaller, timeout time.Duration) *ktlsTxConn {
	c := &ktlsTxConn{
		tcp:     tcp,
		secrets: newTrafficSecrets(),
		install: install,
		timeout: timeout,

		txMaxBytes: ktlsTxMaxBytes,
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
func (c *ktlsTxConn) init(ctx context.Context) error {
	c.once.Do(func() { c.initErr = c.handshakeAndArm(ctx) })
	return c.initErr
}
func (c *ktlsTxConn) HandshakeContext(ctx context.Context) error { return c.init(ctx) }

func (c *ktlsTxConn) handshakeAndArm(ctx context.Context) error {
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

func (c *ktlsTxCounters) String() string {
	armed, skipped := c.armed.Load(), c.skipped.Load()
	unsupported, failed := c.unsupported.Load(), c.failed.Load()
	poisoned, exhausted := c.poisoned.Load(), c.exhausted.Load()
	return fmt.Sprintf("ktls-tx[attempted=%d armed=%d skipped=%d unsupported=%d failed=%d poisoned=%d exhausted=%d]",
		armed+skipped+unsupported+failed, armed, skipped, unsupported, failed, poisoned, exhausted)
}

// arm() bailed on its own, before reaching the installer: a missing secret,
// session tickets, an unusable TLS version. Always a bug or a misconfiguration
// on our side, never the kernel's.
func (c *ktlsTxConn) skip(reason string) {
	ktlsTxCnt.skipped.Add(1)
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln("ktls-tx: not arming:", reason, c.tcp.RemoteAddr(), &ktlsTxCnt)
	}
}

func (c *ktlsTxConn) arm() {
	defer c.secrets.zero()
	defer c.wire.tls12.stop()

	state := c.Conn.ConnectionState()
	params := ktlsTxParams{version: state.Version, cipherSuite: state.CipherSuite}
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
	if c.txClosed {
		c.txMu.Unlock()
		c.skip("transmit side is closed")
		return
	}
	enabled, err := c.install(c.tcp, &params)
	if enabled && err == nil {
		c.txState.Store(ktlsTxArmed)
	}
	c.txMu.Unlock()

	switch {
	case err != nil:
		// distinguished from `unsupported` on purpose (see isKTLSTxUnsupported)
		cnt := ktlsTxCnt.failed.Add(1)
		if cmn.Rom.V(5, cos.ModAIS) || cos.Sparse(cnt) {
			nlog.Errorln("ktls-tx: install failed, continuing with crypto/tls:", err,
				c.tcp.RemoteAddr(), &ktlsTxCnt)
		}
		return
	case !enabled:
		ktlsTxCnt.unsupported.Add(1)
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.Infoln("ktls-tx: offload unavailable", tls.VersionName(params.version),
				tls.CipherSuiteName(params.cipherSuite), c.tcp.RemoteAddr(), &ktlsTxCnt)
		}
		return
	}

	ktlsTxCnt.armed.Add(1)
	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln("ktls-tx: armed", tls.VersionName(params.version),
			tls.CipherSuiteName(params.cipherSuite), c.tcp.RemoteAddr(), &ktlsTxCnt)
	}
}

// net/http recognizes ConnectionState on a non-*tls.Conn: it calls
// this before installing ReadHeaderTimeout and uses the result for Request.TLS
func (c *ktlsTxConn) ConnectionState() tls.ConnectionState {
	_ = c.init(context.Background())
	return c.Conn.ConnectionState()
}

func (c *ktlsTxConn) Read(p []byte) (int, error) {
	if err := c.init(context.Background()); err != nil {
		return 0, err
	}
	switch c.txState.Load() {
	case ktlsTxPoisoned:
		return 0, errKTLSTxPoisoned
	case ktlsTxExhausted:
		return 0, errKTLSTxExhausted
	}
	return c.Conn.Read(p)
}

func (c *ktlsTxConn) Write(p []byte) (int, error) {
	if err := c.init(context.Background()); err != nil {
		return 0, err
	}
	switch c.txState.Load() {
	case ktlsTxArmed:
		// plaintext enters TCP; kTLS emits encrypted TLS records
		return c.writeKTLSTx(p)
	case ktlsTxPoisoned:
		return 0, errKTLSTxPoisoned
	case ktlsTxExhausted:
		return 0, errKTLSTxExhausted
	}
	return c.Conn.Write(p)
}

func (c *ktlsTxConn) writeKTLSTx(p []byte) (int, error) {
	reserved, ok := c.reserveKTLSTx(int64(len(p)))
	if !ok {
		return 0, c.txStateError()
	}
	n, err := c.tcp.Write(p)
	c.finishKTLSTx(reserved, int64(n))
	return n, err
}

func (c *ktlsTxConn) ReadFrom(r io.Reader) (int64, error) {
	if err := c.init(context.Background()); err != nil {
		return 0, err
	}
	switch c.txState.Load() {
	case ktlsTxArmed:
		// NOTE: sendfile path must use io.LimitedReader (see getOI._txreg in ais/tgtobj)
		lr, ok := r.(*io.LimitedReader)
		if !ok {
			return io.Copy(noReadFrom{c}, r)
		}

		reserved, ok := c.reserveKTLSTx(max(lr.N, 0))
		if !ok {
			return 0, c.txStateError()
		}
		n, err := c.tcp.ReadFrom(lr)
		c.finishKTLSTx(reserved, n)
		return n, err

	case ktlsTxPoisoned:
		return 0, errKTLSTxPoisoned
	case ktlsTxExhausted:
		return 0, errKTLSTxExhausted
	}
	return io.Copy(noReadFrom{c.Conn}, r)
}

// Reserve n plaintext bytes before transmitting.
// A transfer that crosses the per-key limit is allowed to complete. Once the
// current response is marked retiring, the allowance spans all of its writes.
// The limit itself carries margin
// (2^38 bytes vs. RFC 8446's approx. 2^24.5 full-size records).
// Retiring the connection _before_ it gets here is the Tx path's responsibility -
// see ktlsTxRetire.
func (c *ktlsTxConn) reserveKTLSTx(n int64) (int64, bool) {
	if n <= 0 {
		return 0, c.txState.Load() == ktlsTxArmed
	}
	for {
		if c.txState.Load() != ktlsTxArmed {
			return 0, false
		}
		cur := c.txBytes.Load()
		if cur >= c.txMaxBytes && !c.txRetiring.Load() {
			c.exhaustKTLSTx()
			return 0, false
		}
		if c.txBytes.CompareAndSwap(cur, cur+n) {
			return n, true
		}
	}
}

func (c *ktlsTxConn) finishKTLSTx(reserved, written int64) {
	if written < reserved {
		c.txBytes.Add(written - reserved) // refund a short write
	}
}

func (c *ktlsTxConn) exhaustKTLSTx() {
	if !c.txState.CompareAndSwap(ktlsTxArmed, ktlsTxExhausted) {
		return
	}
	cnt := ktlsTxCnt.exhausted.Add(1)
	if cmn.Rom.V(5, cos.ModAIS) || cos.Sparse(cnt) {
		nlog.Warningln(errKTLSTxExhausted, c.tcp.RemoteAddr(), &ktlsTxCnt)
	}

	// record transmit shutdown under txMu, as beginTxClose does
	c.txMu.Lock()
	c.txClosed = true
	c.txMu.Unlock()

	_ = c.closeNotify() // closeOnce: idempotent vs. Close/CloseWrite
	_ = c.tcp.Close()
}

func (c *ktlsTxConn) txStateError() error {
	if c.txState.Load() == ktlsTxPoisoned {
		return errKTLSTxPoisoned
	}
	return errKTLSTxExhausted
}

func (c *ktlsTxConn) Close() error {
	switch c.beginTxClose() {
	case ktlsTxArmed:
		alertErr := c.closeNotify()
		if err := c.tcp.Close(); err != nil {
			return err
		}
		return alertErr
	case ktlsTxPoisoned:
		// The first forbidden crypto/tls write already closed the socket; an
		// orderly close_notify would only make it worse.
		return errKTLSTxPoisoned
	case ktlsTxExhausted:
		return errKTLSTxExhausted
	}
	return c.Conn.Close()
}

func (c *ktlsTxConn) CloseWrite() error {
	switch c.beginTxClose() {
	case ktlsTxArmed:
		return c.closeNotify()
	case ktlsTxPoisoned:
		return errKTLSTxPoisoned
	case ktlsTxExhausted:
		return errKTLSTxExhausted
	}
	return c.Conn.CloseWrite()
}

func (c *ktlsTxConn) beginTxClose() uint32 {
	c.txMu.Lock()
	c.txClosed = true
	state := c.txState.Load()
	c.txMu.Unlock()
	return state
}

func (c *ktlsTxConn) closeNotify() error {
	c.closeOnce.Do(func() {
		_ = c.tcp.SetWriteDeadline(time.Now().Add(5 * time.Second))
		c.closeErr = sendKTLSTxCloseNotify(c.tcp)
		_ = c.tcp.SetWriteDeadline(time.Now())
	})
	return c.closeErr
}

// implements ktlsTxState
func (c *ktlsTxConn) KTLSTxEnabled() bool { return c.txState.Load() == ktlsTxArmed }

// remaining per-key plaintext transmit budget; zero when the kernel does not own TX
func (c *ktlsTxConn) KTLSTxRemaining() int64 {
	if c.txState.Load() != ktlsTxArmed {
		return 0
	}
	return max(c.txMaxBytes-c.txBytes.Load(), 0)
}

// Mark the current response as final before net/http commits its headers.
// Once retiring, all writes belonging to this response may complete.
func (c *ktlsTxConn) KTLSTxRetire(size int64) bool {
	if size < 0 || c.txState.Load() != ktlsTxArmed {
		return false
	}
	if c.txRetiring.Load() {
		return true
	}
	rem := c.KTLSTxRemaining()
	if rem > ktlsTxHeadroom && size < rem-ktlsTxHeadroom {
		return false
	}
	c.txRetiring.Store(true)
	return true
}

////////////////////
// ktlsTxListener //
////////////////////

func newKTLSTxListener(ln net.Listener, tlsConf *tls.Config, timeout time.Duration,
	configureTCP func(*net.TCPConn)) (*ktlsTxListener, error) {
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

	return &ktlsTxListener{
		Listener:     ln,
		tlsConfig:    tmpl,
		install:      installKTLSTx,
		configureTCP: configureTCP,
		timeout:      timeout,
	}, nil
}

// accept TCP, apply socket options, construct the hybrid connection, and return.
// No TLS handshake here - see (*ktlsTxConn).init.
func (l *ktlsTxListener) Accept() (net.Conn, error) {
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

	return newKTLSTxConn(tcp, l.tlsConfig, l.install, l.timeout), nil
}

func isKTLSTx(ctx context.Context) bool {
	state, _ := ctx.Value(keyKTLSTx).(ktlsTxState)
	return state != nil && state.KTLSTxEnabled()
}

// Retire an armed connection at the _end_ of the current response, when transmitting
// `size` bytes would otherwise cross the per-key budget mid-body.
//
// Go stdlib net/http turns a handler-set "Connection: close" into closeAfterReply
// (ref: chunkWriter.writeHeader), so the client receives this response in full, is told
// the connection is done, and reconnects for the next one.
//
// Ineffective for a single object larger than the entire budget - see TODO above.
func ktlsTxRetire(r *http.Request, whdr http.Header, size int64) {
	if r == nil || size < 0 {
		return
	}
	state, _ := r.Context().Value(keyKTLSTx).(ktlsTxState)
	if state == nil {
		return
	}
	if state.KTLSTxRetire(size) {
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
