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
	"io"
	"net"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/tools/tassert"

	"golang.org/x/sys/unix"
)

func TestKTLSTxLinuxRecordTypeCmsg(t *testing.T) {
	oob := linuxTLSRecordTypeCmsg(linuxTLSRecordTypeAlert)
	msgs, err := unix.ParseSocketControlMessage(oob)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(msgs) == 1, "expected one control message, got %d", len(msgs))
	if len(msgs) != 1 {
		return
	}
	msg := msgs[0]
	tassert.Errorf(t, msg.Header.Level == unix.SOL_TLS, "unexpected cmsg level %d", msg.Header.Level)
	tassert.Errorf(t, msg.Header.Type == linuxTLSSetRecordType, "unexpected cmsg type %d", msg.Header.Type)
	tassert.Errorf(t, bytes.Equal(msg.Data, []byte{linuxTLSRecordTypeAlert}), "unexpected cmsg data %v", msg.Data)
}

func TestKTLSTxLinuxCryptoInfo(t *testing.T) {
	tests := []struct {
		name         string
		secret       string
		key          string
		iv           string
		cipherSuite  uint16
		cipherType   uint16
		cryptoInfoSz int
	}{
		{
			name:         "aes-128-gcm",
			cipherSuite:  tls.TLS_AES_128_GCM_SHA256,
			cipherType:   linuxTLSCipherAESGCM128,
			cryptoInfoSz: 40,
			secret:       "a11af9f05531f856ad47116b45a950328204b4f44bfb6b3a4b4f1f3fcb631643",
			key:          "9f02283b6c9c07efc26bb9f2ac92e356",
			iv:           "cf782b88dd83549aadf1e984",
		},
		{
			name:         "aes-256-gcm",
			cipherSuite:  tls.TLS_AES_256_GCM_SHA384,
			cipherType:   linuxTLSCipherAESGCM256,
			cryptoInfoSz: 56,
			secret: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
				"202122232425262728292a2b2c2d2e2f",
			key: "6877d022f1c61d24ebb7487c16752d9a4798e40431c75b39320e537c90e23225",
			iv:  "42822531a0fe88648fc09e9f",
		},
	}
	recordSeq := [linuxTLSRecordSeqSize]byte{0, 1, 2, 3, 4, 5, 6, 7}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			secret, err := hex.DecodeString(test.secret)
			tassert.CheckFatal(t, err)
			key, err := hex.DecodeString(test.key)
			tassert.CheckFatal(t, err)
			iv, err := hex.DecodeString(test.iv)
			tassert.CheckFatal(t, err)

			params := ktlsTxParams{
				version:     tls.VersionTLS13,
				cipherSuite: test.cipherSuite,
				secret:      secret,
				recordSeq:   recordSeq,
			}
			info, supported, err := linuxTLSCryptoInfo(&params)
			tassert.CheckFatal(t, err)
			defer clear(info)
			tassert.Errorf(t, supported, "cipher suite %#x is not supported", test.cipherSuite)
			tassert.Errorf(t, len(info) == test.cryptoInfoSz, "expected crypto_info size %d, got %d", test.cryptoInfoSz, len(info))
			tassert.Errorf(t, binary.NativeEndian.Uint16(info[0:2]) == linuxTLSVersion13,
				"unexpected TLS version %#x", binary.NativeEndian.Uint16(info[0:2]))
			tassert.Errorf(t, binary.NativeEndian.Uint16(info[2:4]) == test.cipherType,
				"unexpected Linux cipher type %d", binary.NativeEndian.Uint16(info[2:4]))

			keyOffset := linuxTLSCryptoInfoSize + linuxTLSIVSize
			saltOffset := keyOffset + len(key)
			recordSeqOffset := saltOffset + linuxTLSSaltSize
			tassert.Errorf(t, bytes.Equal(info[linuxTLSCryptoInfoSize:keyOffset], iv[linuxTLSSaltSize:]),
				"unexpected Linux IV")
			tassert.Errorf(t, bytes.Equal(info[keyOffset:saltOffset], key), "unexpected Linux key")
			tassert.Errorf(t, bytes.Equal(info[saltOffset:recordSeqOffset], iv[:linuxTLSSaltSize]),
				"unexpected Linux salt")
			tassert.Errorf(t, bytes.Equal(info[recordSeqOffset:], recordSeq[:]), "unexpected record sequence")
		})
	}
}

func TestKTLSTxLinuxTLS12CryptoInfo(t *testing.T) {
	master, err := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
		"202122232425262728292a2b2c2d2e2f")
	tassert.CheckFatal(t, err)
	clientBytes, err := hex.DecodeString("202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")
	tassert.CheckFatal(t, err)
	serverBytes, err := hex.DecodeString("404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f")
	tassert.CheckFatal(t, err)
	clientRandom, serverRandom := [32]byte{}, [32]byte{}
	copy(clientRandom[:], clientBytes)
	copy(serverRandom[:], serverBytes)
	recordSeq := [linuxTLSRecordSeqSize]byte{7: 2}

	tests := []struct {
		name         string
		key          string
		salt         string
		cipherSuite  uint16
		cipherType   uint16
		cryptoInfoSz int
	}{
		{
			name:         "aes-128-gcm/sha256",
			cipherSuite:  tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			cipherType:   linuxTLSCipherAESGCM128,
			cryptoInfoSz: 40,
			key:          "617bfc73135fe88287599ae2278f1202",
			salt:         "3ffdfdf2",
		},
		{
			name:         "aes-256-gcm/sha384",
			cipherSuite:  tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			cipherType:   linuxTLSCipherAESGCM256,
			cryptoInfoSz: 56,
			key:          "2beee8b9885b18471b6d987d01c2e7fb36b5c2cdb42fd5a1ba07e906aeef53cf",
			salt:         "9e7af7e8",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := hex.DecodeString(test.key)
			tassert.CheckFatal(t, err)
			salt, err := hex.DecodeString(test.salt)
			tassert.CheckFatal(t, err)
			params := ktlsTxParams{
				version:      tls.VersionTLS12,
				cipherSuite:  test.cipherSuite,
				secret:       master,
				clientRandom: clientRandom,
				serverRandom: serverRandom,
				recordSeq:    recordSeq,
			}

			info, supported, err := linuxTLSCryptoInfo(&params)
			tassert.CheckFatal(t, err)
			defer clear(info)
			tassert.Errorf(t, supported, "cipher suite %#x is not supported", test.cipherSuite)
			tassert.Errorf(t, len(info) == test.cryptoInfoSz, "expected crypto_info size %d, got %d",
				test.cryptoInfoSz, len(info))
			tassert.Errorf(t, binary.NativeEndian.Uint16(info[0:2]) == linuxTLSVersion12,
				"unexpected TLS version %#x", binary.NativeEndian.Uint16(info[0:2]))
			tassert.Errorf(t, binary.NativeEndian.Uint16(info[2:4]) == test.cipherType,
				"unexpected Linux cipher type %d", binary.NativeEndian.Uint16(info[2:4]))

			keyOffset := linuxTLSCryptoInfoSize + linuxTLSIVSize
			saltOffset := keyOffset + len(key)
			recordSeqOffset := saltOffset + linuxTLSSaltSize
			tassert.Errorf(t, bytes.Equal(info[linuxTLSCryptoInfoSize:keyOffset], recordSeq[:]),
				"unexpected TLS 1.2 explicit IV")
			tassert.Errorf(t, bytes.Equal(info[keyOffset:saltOffset], key), "unexpected Linux key")
			tassert.Errorf(t, bytes.Equal(info[saltOffset:recordSeqOffset], salt), "unexpected Linux salt")
			tassert.Errorf(t, bytes.Equal(info[recordSeqOffset:], recordSeq[:]), "unexpected record sequence")
			tassert.Errorf(t, params.recordSeq == recordSeq, "crypto-info construction mutated its input")
		})
	}
}

func TestKTLSTxLinuxInstaller(t *testing.T) {
	// Unsupported suites must fall back before touching the socket.
	params := ktlsTxParams{
		version:     tls.VersionTLS13,
		cipherSuite: tls.TLS_CHACHA20_POLY1305_SHA256,
		secret:      make([]byte, 32),
	}
	enabled, err := installKTLSTx(nil, &params)
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

			l, err := newKTLSTxListener(raw, serverConf, testktlsTimeout, nil)
			tassert.CheckFatal(t, err)
			type result struct {
				err     error
				enabled bool
			}
			installed := make(chan result, 1)
			realInstall := l.install
			l.install = func(tcp *net.TCPConn, params *ktlsTxParams) (bool, error) {
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
			res := <-installed
			if res.err != nil {
				t.Skipf("kTLS TX is unavailable: %v", res.err)
			}
			if !res.enabled {
				t.Skip("kTLS TX is unsupported by this kernel")
			}
			tassert.Errorf(t, srv.conn.KTLSTxEnabled(), "installer succeeded but connection is not armed")

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

			n, err := srv.conn.ReadFrom(file)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, n == int64(len(filePayload)), "expected sendfile size %d, got %d", len(filePayload), n)
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

// Expected host limitations fall back silently. EINVAL is necessarily in this
// set: at TLS_TX it can mean either malformed crypto_info or a TLS version or
// cipher that the kernel does not implement. Crypto-info layout is covered by
// the deterministic tests above.
func TestKTLSTxLinuxUnsupported(t *testing.T) {
	tests := []struct {
		name  string
		stage string
		err   error
		want  bool
	}{
		// stage-independent: unavailable protocol, syscall, or crypto implementation
		{"ENOENT-ULP", ktlsStageULP, unix.ENOENT, true}, // TLS ULP unavailable
		{"ENOENT-TX", ktlsStageTX, unix.ENOENT, true},   // no gcm(aes) implementation

		// stage-independent (cont-d)
		{"ENOPROTOOPT", ktlsStageULP, unix.ENOPROTOOPT, true},
		{"EPROTONOSUPPORT", ktlsStageTX, unix.EPROTONOSUPPORT, true},
		{"EOPNOTSUPP", ktlsStageTX, unix.EOPNOTSUPP, true},
		{"ENOSYS", ktlsStageULP, unix.ENOSYS, true},

		// EINVAL only: ambiguous at TLS_TX between unsupported and defective crypto-info
		{"EINVAL-ULP", ktlsStageULP, unix.EINVAL, true},
		{"EINVAL-TX", ktlsStageTX, unix.EINVAL, false},

		// neither
		{"EBADF", ktlsStageTX, unix.EBADF, false},
		{"EACCES", ktlsStageULP, unix.EACCES, false},
		{"nil", ktlsStageTX, nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := isKTLSTxUnsupported(test.stage, test.err)
			tassert.Errorf(t, got == test.want,
				"isKTLSTxUnsupported(%s, %v) = %v, wanted %v", test.stage, test.err, got, test.want)
		})
	}
}
