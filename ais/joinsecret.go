// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Node-join admission: mutual proof of possession of a pre-provisioned shared secret.
//
// Distinct from the intra-cluster request/stream signing in signverify.go: different key
// (shared, out-of-band provisioned vs. per-node Ed25519), different lifetime (loaded once
// at startup vs. rotated), and a different protected event (Smap membership vs. per-request
// authenticity).
//
// An empty node_join_secret_path preserves legacy self-join. When configured, the secret
// must load and self-join admission must be mutually authenticated.
//
// The secret itself lives in htrun.joinSecret; see docs/auth_node_join.md.

// Proof domains: distinct so that neither directional proof can be reflected as the other.
const (
	nodeJoinRequestHMACDomain  = "ais-self-join-v1"
	nodeJoinResponseHMACDomain = "ais-self-join-response-v1"
)

// Rejection reasons. Deliberately coarse and free of secret material: they are returned to
// an as-yet unauthenticated caller. Compare with errors.Is, never by message text.
var (
	errJoinTimestamp = errors.New("invalid node-join timestamp")
	errJoinExpired   = errors.New("expired node-join timestamp")
	errJoinSignature = errors.New("invalid node-join signature")
)

//
// credential file
//

func loadNodeJoinSecret(secretPath string) ([]byte, error) {
	finfo, err := os.Stat(secretPath) // follows projected K8s Secret symlinks
	if err != nil {
		return nil, fmt.Errorf("failed to stat node-join secret %q: %w", secretPath, err)
	}

	// See "Credential File" in docs/auth_node_join.md.
	if mode := finfo.Mode().Perm(); mode&0o077 != 0 {
		return nil, fmt.Errorf("node-join secret %q permissions %04o allow group or other access", secretPath, mode)
	}

	// TODO: load ordered rotation secrets (first signs, all verify)
	secret, err := cos.ReadOneLine(secretPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read node-join secret %q: %w", secretPath, err)
	}
	if secret == "" {
		return nil, fmt.Errorf("node-join secret %q is empty", secretPath)
	}
	return []byte(secret), nil
}

//
// sign & verify
//

// the canonical proof: base64 RawURL HMAC-SHA256 binding the domain, timestamp, and body.
// Fields are NUL-separated in fixed order so that no two distinct inputs produce the same
// MAC input. Pinned by TestNodeJoinSignatureKAT - changing it is a wire-format break.
func nodeJoinSignature(domain string, secret, body []byte, timestamp string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(domain))
	mac.Write([]byte{0})
	mac.Write([]byte(timestamp))
	mac.Write([]byte{0})
	mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func signNodeJoin(domain string, secret, body []byte, hdr http.Header) {
	debug.Assert(len(secret) > 0)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	hdr.Set(apc.HdrJoinTime, timestamp)
	hdr.Set(apc.HdrJoinSig, nodeJoinSignature(domain, secret, body, timestamp))
}

// TODO: add nonce (see docs/auth_node_join.md)
func verifyNodeJoin(domain string, secret, body []byte, hdr http.Header, maxSkew time.Duration) error {
	timestamp := hdr.Get(apc.HdrJoinTime)
	unix, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return errJoinTimestamp
	}
	if time.Since(time.Unix(unix, 0)).Abs() > maxSkew {
		return errJoinExpired
	}
	expected := nodeJoinSignature(domain, secret, body, timestamp)
	if !cos.CryptoEqual([]byte(hdr.Get(apc.HdrJoinSig)), []byte(expected)) {
		return errJoinSignature
	}
	return nil
}
