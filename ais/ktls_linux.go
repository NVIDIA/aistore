// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	ktlsTxPlatform          = true
	ktlsStageULP            = "TCP_ULP"
	ktlsStageTX             = "TLS_TX"
	linuxTLSVersion12       = 0x0303
	linuxTLSVersion13       = 0x0304
	linuxTLSCipherAESGCM128 = 51
	linuxTLSCipherAESGCM256 = 52
	linuxTLSCryptoInfoSize  = 4
	linuxTLSIVSize          = 8
	linuxTLSSaltSize        = 4
	linuxTLSRecordSeqSize   = 8
	linuxTLSSetTX           = 1
	linuxTLSSetRecordType   = 1
	linuxTLSRecordTypeAlert = 21
	tlsAlertLevelWarning    = 1
	tlsAlertCloseNotify     = 0
)

func installKTLSTx(tcp *net.TCPConn, params *ktlsTxParams) (bool, error) {
	cryptoInfo, supported, err := linuxTLSCryptoInfo(params)
	if !supported || err != nil {
		return false, err
	}
	defer clear(cryptoInfo)

	if tcp == nil {
		return false, errors.New("ktls-tx: nil TCP connection")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return false, fmt.Errorf("ktls-tx: syscall connection: %w", err)
	}

	var (
		sockErr   error
		stage     string
		installed bool
	)
	controlErr := raw.Control(func(fd uintptr) {
		stage = ktlsStageULP
		sockErr = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_ULP, "tls")
		if sockErr != nil {
			return
		}

		// TLS_TX is intentionally the last fallible operation: once it succeeds,
		// crypto/tls cannot safely resume ownership of the transmit path.
		stage = ktlsStageTX
		sockErr = setsockoptBytes(fd, unix.SOL_TLS, linuxTLSSetTX, cryptoInfo)
		if sockErr != nil {
			return
		}
		installed = true
	})

	// Nothing may turn a successful TLS_TX installation into fallback.
	if installed {
		return true, nil
	}
	if controlErr != nil {
		return false, fmt.Errorf("ktls-tx: raw control: %w", controlErr)
	}
	if isKTLSTxUnsupported(sockErr) {
		return false, nil
	}
	if sockErr == nil {
		return false, errors.New("ktls-tx: raw control callback was not executed")
	}
	return false, fmt.Errorf("ktls-tx: setsockopt %s: %w", stage, sockErr)
}

func sendKTLSTxCloseNotify(tcp *net.TCPConn) error {
	if tcp == nil {
		return errors.New("ktls-tx: nil TCP connection")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return fmt.Errorf("ktls-tx: close_notify syscall connection: %w", err)
	}

	oob := linuxTLSRecordTypeCmsg(linuxTLSRecordTypeAlert)
	alert := [...]byte{tlsAlertLevelWarning, tlsAlertCloseNotify}
	var (
		n       int
		sendErr error
	)
	controlErr := raw.Write(func(fd uintptr) bool {
		for {
			n, sendErr = unix.SendmsgN(int(fd), alert[:], oob, nil, unix.MSG_NOSIGNAL)
			if !errors.Is(sendErr, unix.EINTR) {
				break
			}
		}
		return !errors.Is(sendErr, unix.EAGAIN)
	})
	if controlErr != nil {
		return fmt.Errorf("ktls-tx: close_notify raw write: %w", controlErr)
	}
	if sendErr != nil {
		return fmt.Errorf("ktls-tx: close_notify sendmsg: %w", sendErr)
	}
	if n != len(alert) {
		return fmt.Errorf("ktls-tx: close_notify: %w (%d/%d)", io.ErrShortWrite, n, len(alert))
	}
	return nil
}

func linuxTLSRecordTypeCmsg(recordType byte) []byte {
	oob := make([]byte, unix.CmsgSpace(1))
	hdr := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
	hdr.Level = unix.SOL_TLS
	hdr.Type = linuxTLSSetRecordType
	hdr.SetLen(unix.CmsgLen(1))
	oob[unix.CmsgLen(0)] = recordType
	return oob
}

// derive and marshal one of the Linux tls12_crypto_info_aes_gcm_* structures
func linuxTLSCryptoInfo(params *ktlsTxParams) ([]byte, bool, error) {
	cipherType, keyLen, supported := linuxTLSCipher(params.version, params.cipherSuite)
	if !supported {
		return nil, false, nil
	}

	var (
		key     []byte
		iv      []byte
		salt    []byte
		version uint16
		err     error
	)
	switch params.version {
	case tls.VersionTLS13:
		var derivedIV []byte
		key, derivedIV, supported, err = deriveTLS13KeyIV(params.cipherSuite, params.secret)
		if !supported || err != nil {
			return nil, supported, err
		}
		if len(derivedIV) != tls13IVSize {
			clear(key)
			clear(derivedIV)
			return nil, true, errors.New("ktls-tx: invalid TLS 1.3 IV size")
		}
		iv, salt = derivedIV[linuxTLSSaltSize:], derivedIV[:linuxTLSSaltSize]
		version = linuxTLSVersion13

	case tls.VersionTLS12:
		key, salt, supported, err = deriveTLS12KeyIV(params.cipherSuite, params.secret,
			params.clientRandom, params.serverRandom)
		if !supported || err != nil {
			return nil, supported, err
		}
		iv = append(iv, params.recordSeq[:]...)
		version = linuxTLSVersion12
	}
	defer clear(key)
	defer clear(iv)
	defer clear(salt)
	if len(key) != keyLen || len(iv) != linuxTLSIVSize || len(salt) != linuxTLSSaltSize {
		return nil, true, fmt.Errorf("ktls-tx: invalid key/IV/salt sizes %d/%d/%d for cipher %#x",
			len(key), len(iv), len(salt), params.cipherSuite)
	}

	const keyOffset = linuxTLSCryptoInfoSize + linuxTLSIVSize
	saltOffset := keyOffset + keyLen
	recordSeqOffset := saltOffset + linuxTLSSaltSize
	cryptoInfo := make([]byte, recordSeqOffset+linuxTLSRecordSeqSize)

	binary.NativeEndian.PutUint16(cryptoInfo[0:2], version)
	binary.NativeEndian.PutUint16(cryptoInfo[2:4], cipherType)
	copy(cryptoInfo[linuxTLSCryptoInfoSize:keyOffset], iv)
	copy(cryptoInfo[keyOffset:saltOffset], key)
	copy(cryptoInfo[saltOffset:recordSeqOffset], salt)
	copy(cryptoInfo[recordSeqOffset:], params.recordSeq[:])
	return cryptoInfo, true, nil
}

func linuxTLSCipher(version, cipherSuite uint16) (cipherType uint16, keyLen int, supported bool) {
	switch version {
	case tls.VersionTLS13:
		switch cipherSuite {
		case tls.TLS_AES_128_GCM_SHA256:
			return linuxTLSCipherAESGCM128, 16, true
		case tls.TLS_AES_256_GCM_SHA384:
			return linuxTLSCipherAESGCM256, 32, true
		}

	case tls.VersionTLS12:
		switch cipherSuite {
		case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256:
			return linuxTLSCipherAESGCM128, 16, true
		case tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384:
			return linuxTLSCipherAESGCM256, 32, true
		}
	}
	return 0, 0, false
}

func setsockoptBytes(fd uintptr, level, opt int, value []byte) error {
	_, _, errno := unix.Syscall6(unix.SYS_SETSOCKOPT, fd, uintptr(level), uintptr(opt),
		uintptr(unsafe.Pointer(&value[0])), uintptr(len(value)), 0)
	runtime.KeepAlive(value)
	if errno != 0 {
		return errno
	}
	return nil
}

// Whether a setsockopt failure means that this host cannot perform the
// requested offload. EINVAL is deliberately excluded: at TLS_TX it is
// ambiguous between unsupported input and a kernel-side or crypto-info defect,
// and must surface through the sparse failed-install logging.
func isKTLSTxUnsupported(err error) bool {
	return errors.Is(err, unix.ENOENT) || // TLS ULP is unavailable
		errors.Is(err, unix.ENOPROTOOPT) ||
		errors.Is(err, unix.EPROTONOSUPPORT) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.ENOSYS)
}
