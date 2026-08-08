// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import "net"

const ktlsTxPlatform = false

func installKTLSTx(*net.TCPConn, *ktlsTxParams) (bool, error) {
	return false, nil
}

func sendKTLSTxCloseNotify(*net.TCPConn) error { return nil }

func linuxTLSRecordTypeCmsg(byte) []byte { return nil }

func linuxTLSCryptoInfo(*ktlsTxParams) ([]byte, bool, error) { return nil, false, nil }

func linuxTLSCipher(uint16, uint16) (uint16, int, bool) { return 0, 0, false }

func setsockoptBytes(uintptr, int, int, []byte) error { return nil }

func isKTLSTxUnsupported(error) bool { return true }
