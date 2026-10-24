// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/mono"

	onexxh "github.com/OneOfOne/xxhash"
)

const (
	// tie-breaker alphabet (GenTie, GenYAID)
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"
)

const (
	LenShortID = 9  // min valid UUID length; retain compatibility with older nodes
	LenBEID    = 10 // BEID

	lenUUID = 11 // GenUUID: letter + 9 alphanumerics + letter, ~65 bits

	lenDaemonID   = 8 // min length, via cryptographic rand
	lenK8sProxyID = 13

	// NOTE: cannot be smaller than any of the valid max lengths - see above
	tooLongID = 32
)

// bucket name, remais alias, inventory name
const (
	MaxNameLength = 64
)

// (bit spread)
const (
	GoldenRatio = 0x9e3779b97f4a7c15
	Mix64Mul    = 0xbf58476d1ce4e5b9
)

const (
	mayOnlyContain = "may only contain letters, numbers, dashes (-), underscores (_)"
	OnlyNice       = "must be less than 32 characters and " + mayOnlyContain // NOTE tooLongID
	OnlyPlus       = mayOnlyContain + ", and dots (.)"
)

var rtie atomic.Uint64

// seed tie-breakers (GenTie, GenYAID)
func InitShortID(seed uint64) {
	rtie.Store(uint64(mono.NanoTime()) ^ seed)
}

//
// UUID
//

// generate a cryptographically random ID with letters at both ends
// vs pre-5.2:
// - no clock or ordering dependency; safe to call concurrently
// - never parses as a rebalance ID
// - removes the shortid dependency
// compare with xreg.GenBEID (see also: bench/micro/uuid/genid_test.go)
func GenUUID() string {
	var (
		b    [lenUUID]byte
		ends [2]byte
	)
	randBytes(crnd, LetterRunes, ends[:])
	randBytes(crnd, AlnumRunes, b[1:lenUUID-1])
	b[0], b[lenUUID-1] = ends[0], ends[1]
	return string(b[:])
}

// "best-effort ID" encoder: turn 64-bit value into a short printable ID
// (called by xreg.GenBEID)
func GenBEID(val uint64, l int) string {
	if l <= 0 || l > tooLongID {
		l = LenBEID
	}
	b := make([]byte, l)
	for i := range l {
		idx := int(val & letterIdxMask)
		if idx >= LenRunes {
			idx -= LenRunes
		}
		b[i] = LetterRunes[idx]
		val >>= letterIdxBits
	}
	return UnsafeS(b)
}

func IsValidUUID(uuid string) bool {
	return len(uuid) >= LenShortID && IsAlphaNice(uuid)
}

//
// Daemon ID
//

func GenDaemonID() string { return CryptoRandS(lenDaemonID) }

func ValidateDaemonID(id string) error {
	if len(id) < lenDaemonID {
		return fmt.Errorf("node ID %q is too short", id)
	}
	if !IsAlphaNice(id) {
		return fmt.Errorf("node ID %q is invalid: must start with a letter, "+OnlyNice, id)
	}
	return nil
}

func HashK8sProxyID(nodeName string) (pid string) {
	digest := onexxh.Checksum64S(UnsafeB(nodeName), MLCG32)
	pid = strconv.FormatUint(digest, 36)
	if pid[0] >= '0' && pid[0] <= '9' {
		pid = pid[1:]
	}
	if l := lenK8sProxyID - len(pid); l > 0 {
		return GenBEID(digest, l) + pid
	}
	return pid
}

// (when config.TestingEnv)
func GenTestingDaemonID(suffix string) string {
	l := max(lenDaemonID-len(suffix), 3)
	return CryptoRandS(l) + suffix
}

//
// chunk manifest ID
//

func ValidateManifestID(id string) error {
	const (
		utag = "chunk-manifest ID"
		lmin = 8
		lmax = 128
	)
	l := len(id)
	if l < lmin {
		return fmt.Errorf("%s %q is too short (expecting >= %d chars)", utag, id, lmin)
	}
	if l > lmax {
		return fmt.Errorf("%s %q is too long (expecting <= %d chars)", utag, id, lmax)
	}
	for i := range l {
		c := id[i]
		if c < 32 || c > 126 || c == '/' || c == '\\' || c == ' ' {
			return fmt.Errorf("%s %q contains invalid character at position %d (expecting printable ASCII with no space and no slashes)",
				utag, id, i)
		}
	}
	return nil
}

//
// utility functions
//

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isNum(c byte) bool { return c >= '0' && c <= '9' }

// letters and numbers w/ '-' and '_' permitted with limitations (see OnlyNice const)
func IsAlphaNice(s string) bool {
	l := len(s)
	if l > tooLongID {
		return false
	}
	for i := range l {
		c := s[i]
		if isAlpha(c) || isNum(c) {
			continue
		}
		if c != '-' && c != '_' {
			return false
		}
		if i == 0 || i == l-1 {
			return false
		}
	}
	return true
}

// alphanumeric++ including letters, numbers, dashes (-), and underscores (_)
// period (.) is allowed except for '..' (OnlyPlus const)
func CheckAlphaPlus(s, tag string) error {
	l := len(s)
	if l > MaxNameLength {
		return fmt.Errorf("%s is too long: %d > %d(max length)", tag, l, MaxNameLength)
	}
	for i := range l {
		c := s[i]
		if isAlpha(c) || isNum(c) || c == '-' || c == '_' {
			continue
		}
		if c != '.' {
			return errors.New(tag + " is invalid: " + OnlyPlus)
		}
		if i < l-1 && s[i+1] == '.' {
			return errors.New(tag + " is invalid: " + OnlyPlus)
		}
	}
	return nil
}

// value is exactly n hex chars
func isHexN(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range n {
		c := s[i]
		if isNum(c) {
			continue
		}
		lc := c | 0x20 // lowercase
		if lc >= 'a' && lc <= 'f' {
			continue
		}
		return false
	}
	return true
}

// 3-letter tie breaker (fast)
// see also:
// - bench/micro/uuid/genid_test.go
// - cmn/xoshiro256
const lenTie = 3

func GenTie() string {
	tie := rtie.Add(1)
	tie *= GoldenRatio

	b := [lenTie]byte{
		uuidABC[tie&0x3f],
		uuidABC[(tie>>6)&0x3f],
		uuidABC[(tie>>12)&0x3f],
	}
	return UnsafeS(b[:])
}

func ValidTie(tie string) bool {
	if len(tie) != lenTie {
		return false
	}
	for i := range lenTie {
		if strings.IndexByte(uuidABC, tie[i]) < 0 {
			return false
		}
	}
	return true
}

// GenYAID - yet another unique ID:
// - it uses simple multiplicative by golden ratio to spread sequential counter values
// - for stronger mixing, see cmn/xoshiro256 package
// - compare w/ GenUUID and GenBEID above
func GenYAID(sid string) string {
	const (
		lsuffix = 5
		lprefix = 7
	)
	var (
		shift uint
		l     = min(len(sid), lprefix)
		b     = make([]byte, l+1+lsuffix)
	)
	copy(b, sid[:l])
	tie := rtie.Add(1)
	tie *= 0x9e3779b97f4a7c15 // ditto
	b[l] = '-'
	for i := range lsuffix {
		b[l+1+i] = uuidABC[(tie>>shift)&0x3f]
		shift += 6
	}
	return UnsafeS(b)
}

func GenTAID(t time.Time) string {
	timeStr := t.Format(StampSec2) // HHMMSS format
	baseID := "t" + timeStr        // must start with a letter

	id := GenYAID(baseID)
	return id
}
