// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"math/rand"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/teris-io/shortid"
)

const (
	// Alphabet for generating UUIDs similar to the shortid.DEFAULT_ABC
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"

	lenShortID   = 9  // UUID length, as per https://github.com/teris-io/shortid#id-length
	lenDaemonID  = 8  // via cryptographic rand
	lenTooLongID = 32 // suspiciously long
)

const (
	OnlyNice = "may only contain letters, numbers, dashes (-), underscores (_), and dots (.)"
	OnlyPlus = OnlyNice + ", and dots (.)"
)

var (
	sid  *shortid.Shortid
	rtie atomic.Int32
)

func InitShortID(seed uint64) {
	sid = shortid.MustNew(4 /*worker*/, uuidABC, seed)
}

//
// UUID
//

func GenUUID() (uuid string) {
	var h, t string
	uuid = sid.MustGenerate()
	if !isAlpha(uuid[0]) {
		h = string(rune('A' + rand.Int()%26))
	}
	c := uuid[len(uuid)-1]
	if c == '-' || c == '_' {
		t = string(rune('a' + rand.Int()%26))
	}
	return h + uuid + t
}

func IsValidUUID(uuid string) bool {
	return len(uuid) >= lenShortID && IsAlphaNice(uuid)
}

func ValidateNiceID(id string, minlen int, tag string) (err error) {
	if len(id) < minlen {
		return fmt.Errorf("%s %q is too short", tag, id)
	}
	if len(id) >= lenTooLongID {
		return fmt.Errorf("%s %q is too long", tag, id)
	}
	if !IsAlphaNice(id) {
		err = fmt.Errorf("%s %q is invalid: must start with a letter and can only contain [A-Za-z0-9-_]", tag, id)
	}
	return
}

//
// Daemon ID
//

func GenDaemonID() string              { return RandStringStrong(lenDaemonID) }
func ValidateDaemonID(id string) error { return ValidateNiceID(id, lenDaemonID, "node ID") }

// (when config.TestingEnv)
func GenTestingDaemonID(suffix string) string {
	l := Max(lenDaemonID-len(suffix), 3)
	return RandStringStrong(l) + suffix
}

//
// utility functions
//

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// letters and numbers w/ '-' and '_' permitted with limitations (below)
// (see OnlyNice above)
func IsAlphaNice(s string) bool {
	l := len(s)
	for i, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
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

// alpha-numeric++ including letters, numbers, dashes (-), and underscores (_)
// period (.) is allowed except for '..'
// (see OnlyPlus above)
func IsAlphaPlus(s string) bool {
	for i, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		if c != '.' {
			return false
		}
		if i < len(s)-1 && s[i+1] == '.' {
			return false
		}
	}
	return true
}

// 3-letter tie breaker (fast)
func GenTie() string {
	tie := rtie.Add(1)
	b0 := uuidABC[tie&0x3f]
	b1 := uuidABC[-tie&0x3f]
	b2 := uuidABC[(tie>>2)&0x3f]
	return string([]byte{b0, b1, b2})
}
