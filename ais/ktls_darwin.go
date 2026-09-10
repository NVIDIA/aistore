// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import "net"

const ktlsPlatform = false

func ktlsInstall(*net.TCPConn, *ktlsParams) (bool, error) {
	return false, nil
}

func ktlsCloseNotify(*net.TCPConn) error { return nil }

func kRecordTypeCmsg(byte) []byte { return nil }

func kCryptoInfo(*ktlsParams) ([]byte, bool, error) { return nil, false, nil }

func kCipher(uint16, uint16) (uint16, int, bool) { return 0, 0, false }

func setsockoptBytes(uintptr, int, int, []byte) error { return nil }

func ktlsUnsupported(string, error) bool { return true }
