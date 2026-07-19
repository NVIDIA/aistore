// Package transport provides long-lived http/tcp connections for intra-cluster communications.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
)

type (
	headerSetter interface {
		Set(name, value string)
	}

	// Auth is the signed authentication envelope for a stream connection.
	Auth struct {
		SmapVer int64
		Nonce   uint64
		Sig     string
	}

	// Target-provided callbacks:
	// - SignFn is called immediately before every stream (re)dial.
	// - VerifyFn authenticates an incoming connection to a registered stream.
	SignFn   func(trname string, sessID int64) Auth
	VerifyFn func(trname string, sessID int64, senderID string, r *http.Request) error
)

func (a Auth) stamp(hdr headerSetter) {
	hdr.Set(apc.HdrSenderSmapVer, strconv.FormatInt(a.SmapVer, 10))
	if a.Sig == "" { // signing not enabled yet
		return
	}
	hdr.Set(apc.HdrSenderNonce, strconv.FormatUint(a.Nonce, 10))
	hdr.Set(apc.HdrSenderSig, a.Sig)
}
