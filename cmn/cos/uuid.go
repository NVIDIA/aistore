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
	"github.com/teris-io/shortid"
)

const (
	// Alphabet for generating UUIDs similar to the shortid.DEFAULT_ABC
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"
)

const (
	LenShortID = 9  // UUID length, as per https://github.com/teris-io/shortid#id-length
	LenBEID    = 10 // BEID

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

var (
	sid  *shortid.Shortid
	rtie atomic.Uint64
)

func InitShortID(seed uint64) {
	worker := uint8(seed & 0x1f) // must be < 32
	sid = shortid.MustNew(worker, uuidABC, seed)

	rtie.Store(uint64(mono.NanoTime()) ^ seed)
}

//
// UUID
//

// compare with xreg.GenBEID
// (see also: bench/micro/uuid/genid_test.go)

func GenUUID() string {
	var (
		h, t string
		uuid = sid.MustGenerate()
	)
	c := uuid[0]
	if (c == 'g' && looksLikeReb(uuid)) || !isAlpha(c) { // cosmetic prefix
		tie := rtie.Add(1)
		h = string(rune('A' + tie%26))
	}
	c = uuid[len(uuid)-1]
	if c == '-' || c == '_' { // ditto
		tie := rtie.Add(1)
		t = string(rune('a' + tie%26))
	}
	return h + uuid + t
}

func looksLikeReb(id string) bool {
	for i := 1; i < len(id); i++ {
		if !isNum(id[i]) {
			return false
		}
	}
	return true
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
	if strings.Contains(id, inv1) || strings.Contains(id, inv2) {
		return fmt.Errorf("%s %q contains invalid substring %q or %q", utag, id, inv1, inv2)
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
func GenTie() string {
	tie := rtie.Add(1)
	tie *= GoldenRatio

	b := [3]byte{
		uuidABC[tie&0x3f],
		uuidABC[(tie>>6)&0x3f],
		uuidABC[(tie>>12)&0x3f],
	}
	return UnsafeS(b[:])
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
