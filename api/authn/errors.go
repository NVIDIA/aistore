// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import "errors"

// Auth-related errors shared by AuthN server, AIS gateways, and clients (e.g. CLI).
var (
	ErrNoPermissions = errors.New("insufficient permissions")
	ErrInvalidToken  = errors.New("invalid token")
	ErrNoSubject     = errors.New("missing 'sub' claim")
	ErrNoToken       = errors.New("token required")
	ErrTokenExpired  = errors.New("token expired")
	ErrTokenRevoked  = errors.New("token revoked")
)
