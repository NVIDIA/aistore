//go:build linux

// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tassert"

	"golang.org/x/sys/unix"
)

type testktlsSendfileOnly struct{ *os.File }

func (*testktlsSendfileOnly) Read([]byte) (int, error) {
	return 0, errors.New("buffered read fallback in kTLS sendfile test")
}

func TestKTLSTxLinuxRecordTypeCmsg(t *testing.T) {
	oob := kRecordTypeCmsg(kRecordTypeAlert)
	msgs, err := unix.ParseSocketControlMessage(oob)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(msgs) == 1, "expected one control message, got %d", len(msgs))
	if len(msgs) != 1 {
		return
	}
	msg := msgs[0]
	tassert.Errorf(t, msg.Header.Level == unix.SOL_TLS, "unexpected cmsg level %d", msg.Header.Level)
	tassert.Errorf(t, msg.Header.Type == kSetRecordType, "unexpected cmsg type %d", msg.Header.Type)
	tassert.Errorf(t, bytes.Equal(msg.Data, []byte{kRecordTypeAlert}), "unexpected cmsg data %v", msg.Data)
}

func TestKTLSTxLinuxCryptoInfo(t *testing.T) {
	for _, test := range testktlsCryptoCases {
		t.Run(test.name, func(t *testing.T) {
			params := testktlsCryptoParams(t, test.version, test.suite)
			recordSeq := params.recordSeq
			key, err := hex.DecodeString(test.key)
			tassert.CheckFatal(t, err)
			iv, err := hex.DecodeString(test.iv)
			tassert.CheckFatal(t, err)
			version, cipherType, size := uint16(kVersion13), uint16(kCipherAESGCM128), 40
			explicitIV, salt := iv[kSaltSize:], iv[:kSaltSize]
			if test.version == tls.VersionTLS12 {
				version, explicitIV, salt = kVersion12, recordSeq[:], iv
			}
			if len(key) == 32 {
				cipherType, size = kCipherAESGCM256, 56
			}
			info, supported, err := kCryptoInfo(&params)
			tassert.CheckFatal(t, err)
			defer clear(info)
			tassert.Fatalf(t, supported && len(info) == size, "expected crypto_info size %d, got %d (supported=%v)", size, len(info), supported)
			tassert.Errorf(t, binary.NativeEndian.Uint16(info[:2]) == version, "unexpected TLS version")
			tassert.Errorf(t, binary.NativeEndian.Uint16(info[2:4]) == cipherType, "unexpected Linux cipher type")
			keyOffset := kCryptoInfoSize + kIVSize
			saltOffset := keyOffset + len(key)
			recordSeqOffset := saltOffset + kSaltSize
			tassert.Errorf(t, bytes.Equal(info[kCryptoInfoSize:keyOffset], explicitIV), "unexpected explicit IV")
			tassert.Errorf(t, bytes.Equal(info[keyOffset:saltOffset], key), "unexpected Linux key")
			tassert.Errorf(t, bytes.Equal(info[saltOffset:recordSeqOffset], salt), "unexpected Linux salt")
			tassert.Errorf(t, bytes.Equal(info[recordSeqOffset:], recordSeq[:]), "unexpected record sequence")
			tassert.Errorf(t, params.recordSeq == recordSeq, "crypto-info construction mutated its input")
		})
	}
}

func TestKTLSTxLinuxInstaller(t *testing.T) {
	// Unsupported suites must fall back before touching the socket.
	params := ktlsParams{
		version:     tls.VersionTLS13,
		cipherSuite: tls.TLS_CHACHA20_POLY1305_SHA256,
		secret:      make([]byte, 32),
	}
	enabled, err := ktlsInstall(nil, &params)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !enabled, "unsupported cipher installed kTLS")

	for _, version := range []uint16{tls.VersionTLS13, tls.VersionTLS12} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			serverConf := testktlsServerConf(t)
			serverConf.MinVersion, serverConf.MaxVersion = version, version
			clientConf := testktlsClientConf(nil)
			clientConf.MinVersion, clientConf.MaxVersion = version, version
			if version == tls.VersionTLS12 {
				cipherSuites := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
				serverConf.CipherSuites = cipherSuites
				clientConf.CipherSuites = cipherSuites
			}

			raw, err := net.Listen("tcp", "127.0.0.1:0")
			tassert.CheckFatal(t, err)
			defer raw.Close()

			l, err := newKtlsListener(raw, serverConf, nil)
			tassert.CheckFatal(t, err)
			type result struct {
				err     error
				enabled bool
			}
			installed := make(chan result, 1)
			realInstall := l.install
			l.install = func(tcp *net.TCPConn, params *ktlsParams) (bool, error) {
				enabled, err := realInstall(tcp, params)
				installed <- result{err: err, enabled: enabled}
				return enabled, err
			}

			srvCh := testktlsListen(t, l)
			cc, err := tls.Dial("tcp", raw.Addr().String(), clientConf)
			tassert.CheckFatal(t, err)
			defer cc.Close()

			srv := <-srvCh
			tassert.CheckFatal(t, srv.err)
			defer srv.conn.Close()
			srv.conn.tryArm(ktlsMinSize)
			res := <-installed
			if res.err != nil {
				t.Skipf("kTLS TX is unavailable: %v", res.err)
			}
			if !res.enabled {
				t.Skip("kTLS TX is unsupported by this kernel")
			}
			tassert.Errorf(t, srv.conn.isArmed(), "installer succeeded but the connection is not armed")

			const payload = "aistore-ktls"
			_, err = srv.conn.Write([]byte(payload))
			tassert.CheckFatal(t, err)
			buf := make([]byte, len(payload))
			_, err = io.ReadFull(cc, buf)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, string(buf) == payload, "expected %q, got %q", payload, buf)

			const filePayload = "aistore-ktls-sendfile"
			file, err := os.CreateTemp(t.TempDir(), "ktls-sendfile-")
			tassert.CheckFatal(t, err)
			defer file.Close()
			written, err := file.WriteString(filePayload)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, written == len(filePayload), "expected to write %d bytes, wrote %d", len(filePayload), written)
			_, err = file.Seek(0, io.SeekStart)
			tassert.CheckFatal(t, err)

			lr := &io.LimitedReader{R: &testktlsSendfileOnly{file}, N: int64(len(filePayload))}
			n, err := srv.conn.ReadFrom(lr)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, n == int64(len(filePayload)), "expected sendfile size %d, got %d", len(filePayload), n)
			tassert.Errorf(t, lr.N == 0, "sendfile left %d bytes unread", lr.N)
			buf = make([]byte, len(filePayload))
			_, err = io.ReadFull(cc, buf)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, string(buf) == filePayload, "expected %q, got %q", filePayload, buf)

			tassert.CheckError(t, srv.conn.CloseWrite())
			_, err = cc.Read(make([]byte, 1))
			tassert.Errorf(t, err == io.EOF, "expected clean TLS close, got %v", err)
			tassert.CheckError(t, srv.conn.Close())
		})
	}
}

// Expected host limitations fall back silently. EINVAL is unsupported while
// attaching the TLS ULP, but remains reportable at TLS_TX because it can expose
// malformed crypto_info.
func TestKTLSTxLinuxErrors(t *testing.T) {
	tests := []struct {
		name, stage                 string
		err                         error
		unsupported, notEstablished bool
	}{
		{"ENOENT-ULP", kStageULP, unix.ENOENT, true, false},
		{"ENOENT-TX", kStageTX, unix.ENOENT, true, false},
		{"ENOPROTOOPT", kStageULP, unix.ENOPROTOOPT, true, false},
		{"EPROTONOSUPPORT", kStageTX, unix.EPROTONOSUPPORT, true, false},
		{"EOPNOTSUPP", kStageTX, unix.EOPNOTSUPP, true, false},
		{"ENOSYS", kStageULP, unix.ENOSYS, true, false},
		{"EINVAL-ULP", kStageULP, unix.EINVAL, true, false},
		{"EINVAL-TX", kStageTX, unix.EINVAL, false, false},
		{"EBADF", kStageTX, unix.EBADF, false, false},
		{"EACCES", kStageULP, unix.EACCES, false, false},
		{"nil", kStageTX, nil, false, false},
		{"ENOTCONN-ULP", kStageULP, unix.ENOTCONN, false, true},
		{"wrapped-ENOTCONN", kStageULP, fmt.Errorf("wrapped: %w", unix.ENOTCONN), false, true},
		{"ENOTCONN-TX", kStageTX, unix.ENOTCONN, false, false},
		{"ENOTCONN-no-stage", "", unix.ENOTCONN, false, false},
		{"EEXIST", kStageULP, unix.EEXIST, false, false},
		{"ECONNRESET", kStageULP, unix.ECONNRESET, false, false},
		{"EPIPE", kStageTX, unix.EPIPE, false, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tassert.Errorf(t, ktlsUnsupported(test.stage, test.err) == test.unsupported, "unexpected unsupported classification")
			tassert.Errorf(t, ktlsNotEstablished(test.stage, test.err) == test.notEstablished, "unexpected not-established classification")
		})
	}
}

// TCP_ULP fails with ENOTCONN when the socket is not in TCP_ESTABLISHED.
// ktlsInstall must not arm, and must return errKtlsNotEstablished.
func TestKTLSTxLinuxNotEstablished(t *testing.T) {
	// the client closes its sending side: the server reads EOF
	t.Run("FIN", func(t *testing.T) {
		cc, srv := testktlsTCPPair(t)
		tassert.CheckFatal(t, cc.CloseWrite())
		_, err := srv.Read(make([]byte, 1))
		tassert.Fatalf(t, err == io.EOF, "expected EOF, got %v", err)

		testktlsInstallNotEstablished(t, srv)

		// the client still receives the response
		const payload = "aistore-ktls"
		_, err = srv.Write([]byte(payload))
		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, srv.CloseWrite())
		buf, err := io.ReadAll(cc)
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, string(buf) == payload, "expected %q, got %q", payload, buf)
	})

	// the client resets the connection: the server reads ECONNRESET
	t.Run("RST", func(t *testing.T) {
		cc, srv := testktlsTCPPair(t)
		tassert.CheckFatal(t, cc.SetLinger(0))
		tassert.CheckFatal(t, cc.Close())
		_, err := srv.Read(make([]byte, 1))
		tassert.Fatalf(t, errors.Is(err, unix.ECONNRESET), "expected ECONNRESET, got %v", err)

		testktlsInstallNotEstablished(t, srv)
	})

	// a kernel without the TLS ULP
	t.Run("no-TLS-ULP", func(t *testing.T) {
		const ulp = "aistore-no-ulp"
		cc, srv := testktlsTCPPair(t)
		err := testktlsSetULP(t, srv, ulp)
		tassert.Errorf(t, ktlsUnsupported(kStageULP, err) && !ktlsNotEstablished(kStageULP, err),
			"established: expected unsupported, got %v", err)

		tassert.CheckFatal(t, cc.CloseWrite())
		_, err = srv.Read(make([]byte, 1))
		tassert.Fatalf(t, err == io.EOF, "expected EOF, got %v", err)
		err = testktlsSetULP(t, srv, ulp)
		tassert.Errorf(t, ktlsUnsupported(kStageULP, err) && !ktlsNotEstablished(kStageULP, err),
			"not established: expected unsupported, got %v", err)
	})
}

func testktlsTCPPair(t *testing.T) (cc, srv *net.TCPConn) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	defer l.Close()

	c, err := net.Dial("tcp", l.Addr().String())
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { c.Close() })
	s, err := l.Accept()
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { s.Close() })

	srv = s.(*net.TCPConn)
	tassert.CheckFatal(t, srv.SetReadDeadline(time.Now().Add(testktlsTimeout)))
	return c.(*net.TCPConn), srv
}

func testktlsInstallNotEstablished(t *testing.T, srv *net.TCPConn) {
	params := ktlsParams{
		version:     tls.VersionTLS13,
		cipherSuite: tls.TLS_AES_128_GCM_SHA256,
		secret:      make([]byte, 32),
	}
	enabled, err := ktlsInstall(srv, &params)
	tassert.Errorf(t, !enabled, "installed kTLS on a socket that is not in TCP_ESTABLISHED")

	if !testktlsHasULP(t) {
		t.Log("kTLS TX is unsupported by this kernel (no TLS ULP)")
		tassert.Errorf(t, err == nil, "expected unsupported (nil error), got %v", err)
		return
	}
	tassert.Errorf(t, errors.Is(err, errKtlsNotEstablished), "expected %v, got %v", errKtlsNotEstablished, err)
}

// Probe with a socket in TCP_ESTABLISHED.
func testktlsHasULP(t *testing.T) bool {
	_, srv := testktlsTCPPair(t)
	return testktlsSetULP(t, srv, "tls") == nil
}

func testktlsSetULP(t *testing.T, tcp *net.TCPConn, name string) (err error) {
	raw, e := tcp.SyscallConn()
	tassert.CheckFatal(t, e)
	tassert.CheckFatal(t, raw.Control(func(fd uintptr) {
		err = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_ULP, name)
	}))
	return err
}

//
// transmit benchmarks
// run: go test -run '^$' -bench '^BenchmarkKTLSTx' -benchmem
//
// Two variants over one harness, differing only in who encrypts:
//   ktls       - the real installer; the kernel encrypts. Skipped when the host
//                has no TLS ULP (`modprobe tls`; /proc/sys/net/ipv4/tcp_available_ulp)
//   crypto-tls - the installer declines; Go's userspace TLS encrypts
//
// The peer decrypts one probe record (proving the crypto_info we installed is
// the key the client agreed on), then drains the raw socket without decrypting -
// otherwise the client's own crypto becomes the bottleneck and flattens both
// variants into the same number.

const benchKtlsProbe = "aistore-ktls-probe"

// one armed-or-not server connection with a draining peer
func benchKtlsConn(b *testing.B, install ktlsInstaller) *ktlsConn {
	b.Helper()

	conn, client := testktlsConnPair(b, install)
	testktlsWaitHandshake(b, testktlsStartHandshake(b, conn, client))
	conn.tryArm(ktlsMinSize)

	// probe: the peer must be able to decrypt what we just wrote
	if _, err := conn.Write([]byte(benchKtlsProbe)); err != nil {
		b.Fatal(err)
	}
	got := make([]byte, len(benchKtlsProbe))
	if _, err := io.ReadFull(client, got); err != nil {
		b.Fatal(err)
	}
	if string(got) != benchKtlsProbe {
		b.Fatalf("probe mismatch: %q", got)
	}

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		_, _ = io.Copy(io.Discard, client.NetConn())
	}()
	b.Cleanup(func() { _ = client.NetConn().Close(); <-drained })

	return conn
}

func benchKtlsVariants(b *testing.B, run func(b *testing.B, conn *ktlsConn)) {
	b.Helper()

	b.Run("crypto-tls", func(b *testing.B) {
		conn := benchKtlsConn(b, func(*net.TCPConn, *ktlsParams) (bool, error) { return false, nil })
		if conn.isArmed() {
			b.Fatal("declining installer armed the connection")
		}
		run(b, conn)
	})

	b.Run("ktls", func(b *testing.B) {
		conn := benchKtlsConn(b, ktlsInstall)
		if !conn.isArmed() {
			b.Skip("kTLS TX is unsupported by this kernel")
		}
		run(b, conn)
	})
}

// buffered transmit: crypto/tls Write vs plaintext into an armed socket
func BenchmarkKTLSTxWrite(b *testing.B) {
	for _, size := range []int64{4 * cos.KiB, 64 * cos.KiB, cos.MiB} {
		b.Run(cos.IEC(size, 0), func(b *testing.B) {
			benchKtlsVariants(b, func(b *testing.B, conn *ktlsConn) {
				p := make([]byte, size)
				b.SetBytes(size)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if _, err := conn.Write(p); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// the question the feature exists for: sendfile(2) into an armed socket vs
// crypto/tls reading the file into userspace to encrypt it
func BenchmarkKTLSTxSendfile(b *testing.B) {
	for _, size := range []int64{64 * cos.KiB, cos.MiB, 4 * cos.MiB} {
		b.Run(cos.IEC(size, 0), func(b *testing.B) {
			file, err := os.CreateTemp(b.TempDir(), "ktls-sendfile")
			tassert.CheckFatal(b, err)
			b.Cleanup(func() { _ = file.Close() })
			if _, err := file.Write(make([]byte, size)); err != nil {
				b.Fatal(err)
			}

			benchKtlsVariants(b, func(b *testing.B, conn *ktlsConn) {
				armed := conn.isArmed()
				var src io.Reader = file
				if armed {
					// fail loudly if ReadFrom silently falls back to a copy
					src = &testktlsSendfileOnly{file}
				}
				lr := &io.LimitedReader{R: src}
				var buf []byte
				if !armed {
					// match the regular (slab-pooled) GET path - allocate once outside the timed loop
					buf = make([]byte, min(size, memsys.MaxPageSlabSize))
				}
				b.SetBytes(size)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if _, err := file.Seek(0, io.SeekStart); err != nil {
						b.Fatal(err)
					}
					lr.N = size
					var (
						n   int64
						err error
					)
					if armed {
						n, err = conn.ReadFrom(lr)
					} else {
						n, err = cos.CopyBuffer(conn, lr, buf)
					}
					if err != nil {
						b.Fatal(err)
					}
					if n != size {
						b.Fatalf("transmitted %d of %d", n, size)
					}
				}
			})
		})
	}
}
