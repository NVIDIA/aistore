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
	ktlsPlatform         = true
	kStageULP            = "TCP_ULP"
	kStageTX             = "TLS_TX"
	kVersion12           = 0x0303
	kVersion13           = 0x0304
	kCipherAESGCM128     = 51
	kCipherAESGCM256     = 52
	kCryptoInfoSize      = 4
	kIVSize              = 8
	kSaltSize            = 4
	kRecordSeqSize       = 8
	kSetTX               = 1
	kSetRecordType       = 1
	kRecordTypeAlert     = 21
	tlsAlertLevelWarning = 1
	tlsAlertCloseNotify  = 0
)

func ktlsInstall(tcp *net.TCPConn, params *ktlsParams) (bool, error) {
	cryptoInfo, supported, err := kCryptoInfo(params)
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
		stage = kStageULP
		sockErr = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_ULP, "tls")
		if sockErr != nil {
			return
		}

		// TLS_TX is intentionally the last fallible operation: once it succeeds,
		// crypto/tls cannot safely resume ownership of the transmit path.
		stage = kStageTX
		sockErr = setsockoptBytes(fd, unix.SOL_TLS, kSetTX, cryptoInfo)
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
	if ktlsUnsupported(stage, sockErr) {
		return false, nil
	}
	if sockErr == nil {
		return false, errors.New("ktls-tx: raw control callback was not executed")
	}
	return false, fmt.Errorf("ktls-tx: setsockopt %s: %w", stage, sockErr)
}

func ktlsCloseNotify(tcp *net.TCPConn) error {
	if tcp == nil {
		return errors.New("ktls-tx: nil TCP connection")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return fmt.Errorf("ktls-tx: close_notify syscall connection: %w", err)
	}

	oob := kRecordTypeCmsg(kRecordTypeAlert)
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

func kRecordTypeCmsg(recordType byte) []byte {
	oob := make([]byte, unix.CmsgSpace(1))
	hdr := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
	hdr.Level = unix.SOL_TLS
	hdr.Type = kSetRecordType
	hdr.SetLen(unix.CmsgLen(1))
	oob[unix.CmsgLen(0)] = recordType
	return oob
}

// derive and marshal one of the Linux tls12_crypto_info_aes_gcm_* structures
func kCryptoInfo(params *ktlsParams) ([]byte, bool, error) {
	cipherType, keyLen, supported := kCipher(params.version, params.cipherSuite)
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
		iv, salt = derivedIV[kSaltSize:], derivedIV[:kSaltSize]
		version = kVersion13

	case tls.VersionTLS12:
		key, salt, supported, err = deriveTLS12KeyIV(params.cipherSuite, params.secret,
			params.clientRandom, params.serverRandom)
		if !supported || err != nil {
			return nil, supported, err
		}
		iv = append(iv, params.recordSeq[:]...)
		version = kVersion12
	}
	defer clear(key)
	defer clear(iv)
	defer clear(salt)
	if len(key) != keyLen || len(iv) != kIVSize || len(salt) != kSaltSize {
		return nil, true, fmt.Errorf("ktls-tx: invalid key/IV/salt sizes %d/%d/%d for cipher %#x",
			len(key), len(iv), len(salt), params.cipherSuite)
	}

	const keyOffset = kCryptoInfoSize + kIVSize
	saltOffset := keyOffset + keyLen
	recordSeqOffset := saltOffset + kSaltSize
	cryptoInfo := make([]byte, recordSeqOffset+kRecordSeqSize)

	binary.NativeEndian.PutUint16(cryptoInfo[0:2], version)
	binary.NativeEndian.PutUint16(cryptoInfo[2:4], cipherType)
	copy(cryptoInfo[kCryptoInfoSize:keyOffset], iv)
	copy(cryptoInfo[keyOffset:saltOffset], key)
	copy(cryptoInfo[saltOffset:recordSeqOffset], salt)
	copy(cryptoInfo[recordSeqOffset:], params.recordSeq[:])
	return cryptoInfo, true, nil
}

func kCipher(version, cipherSuite uint16) (cipherType uint16, keyLen int, supported bool) {
	switch version {
	case tls.VersionTLS13:
		switch cipherSuite {
		case tls.TLS_AES_128_GCM_SHA256:
			return kCipherAESGCM128, 16, true
		case tls.TLS_AES_256_GCM_SHA384:
			return kCipherAESGCM256, 32, true
		}

	case tls.VersionTLS12:
		switch cipherSuite {
		case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256:
			return kCipherAESGCM128, 16, true
		case tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384:
			return kCipherAESGCM256, 32, true
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
// requested offload. EINVAL is considered unsupported only while attaching
// the fixed TLS ULP. At TLS_TX it may instead indicate defective crypto-info
// input and must surface through the sparse failed-install logging.
func ktlsUnsupported(stage string, err error) bool {
	if stage == kStageULP && errors.Is(err, unix.EINVAL) {
		return true
	}
	return errors.Is(err, unix.ENOENT) || // TLS ULP unavailable; at TLS_TX: no gcm(aes) implementation
		errors.Is(err, unix.ENOPROTOOPT) ||
		errors.Is(err, unix.EPROTONOSUPPORT) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.ENOSYS)
}
