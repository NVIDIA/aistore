// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/teris-io/shortid"
)

const (
	// Alphabet for generating UUIDs similar to the shortid.DEFAULT_ABC
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"
)

const (
	LenShortID    = 9 // UUID length, as per https://github.com/teris-io/shortid#id-length
	lenDaemonID   = 8 // min length, via cryptographic rand
	lenK8sProxyID = 13

	// NOTE: cannot be smaller than any of the valid max lengths - see above
	tooLongID = 32
)

// bucket name, remais alias
const (
	tooLongName = 64
)

const (
	mayOnlyContain = "may only contain letters, numbers, dashes (-), underscores (_)"
	OnlyNice       = "must be less than 32 characters and " + mayOnlyContain // NOTE tooLongID
	OnlyPlus       = mayOnlyContain + ", and dots (.)"
)

var (
	sid  *shortid.Shortid
	rtie atomic.Uint64
)

func InitShortID(seed uint64) {
	sid = shortid.MustNew(4 /*worker*/, uuidABC, seed)
}

//
// UUID
//

// compare with xreg.GenBEID
// (see also: bench/micro/uuid/genid_test.go)
func GenUUID() (uuid string) {
	var h, t string
	uuid = sid.MustGenerate()
	if c := uuid[0]; c == 'g' || !isAlpha(c) { // see also: `xact.RebID2S`
		tie := int(rtie.Add(1))
		h = string(rune('A' + tie%26))
	}
	if c := uuid[len(uuid)-1]; c == '-' || c == '_' {
		tie := int(rtie.Add(1))
		t = string(rune('a' + tie%26))
	}
	return h + uuid + t
}

// "best-effort ID" - to independently and locally generate globally unique ID
// called by xreg.GenBEID
func GenBEID(val uint64, l int) string {
	b := make([]byte, l)
	for i := range l {
		if idx := int(val & letterIdxMask); idx < LenRunes {
			b[i] = LetterRunes[idx]
		} else {
			b[i] = LetterRunes[idx-LenRunes]
		}
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
		lmin = 8
		lmax = 128
	)
	l := len(id)
	if l < lmin {
		return fmt.Errorf("chunk manifest ID %q is too short (expecting >= %d)", id, lmin)
	}
	if l > lmax {
		return fmt.Errorf("chunk manifest ID %q is too long (expecting <= %d)", id, lmax)
	}
	for i := range l {
		c := id[i]
		if c < 32 || c > 126 || c == '/' || c == '\\' {
			return fmt.Errorf("chunk manifest ID %q contains invalid character at position %d (expecting ASCII)", id, i)
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

// letters and numbers w/ '-' and '_' permitted with limitations (see OnlyNice const)
func IsAlphaNice(s string) bool {
	l := len(s)
	if l > tooLongID {
		return false
	}
	for i := range l {
		c := s[i]
		if isAlpha(c) || (c >= '0' && c <= '9') {
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
	if l > tooLongName {
		return fmt.Errorf("%s is too long: %d > %d(max length)", tag, l, tooLongName)
	}
	for i := range l {
		c := s[i]
		if isAlpha(c) || (c >= '0' && c <= '9') || c == '-' || c == '_' {
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
		if c >= '0' && c <= '9' {
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
// (see also: bench/micro/uuid/genid_test.go)
func GenTie() string {
	tie := rtie.Add(1)
	tie *= 0x9e3779b97f4a7c15 // golden ratio multiplier (bit spread)

	b := [3]byte{
		uuidABC[tie&0x3f],
		uuidABC[(tie>>6)&0x3f],
		uuidABC[(tie>>12)&0x3f],
	}
	return UnsafeS(b[:])
}

// yet another unique ID (compare w/ GenUUID and GenBEID)
func GenYAID(sid string) string {
	var (
		l = len(sid)
		b = make([]byte, l+4)
	)
	copy(b, sid)
	tie := rtie.Add(1)
	tie *= 0x9e3779b97f4a7c15 // ditto
	b[l] = '-'
	b[l+1] = uuidABC[tie&0x3f]
	b[l+2] = uuidABC[(tie>>6)&0x3f]
	b[l+3] = uuidABC[(tie>>12)&0x3f]
	return UnsafeS(b)
}

func GenTAID(t time.Time) string {
	timeStr := t.Format(StampSec2) // HHMMSS format
	baseID := "t" + timeStr        // must start with a letter

	id := GenYAID(baseID)
	return id
}
