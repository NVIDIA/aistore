// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"math/rand"
	"regexp"

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

var (
	sid     *shortid.Shortid
	rtie    atomic.Int32
	idRegex *regexp.Regexp
)

func InitShortID(seed uint64) {
	sid = shortid.MustNew(4 /*worker*/, uuidABC, seed)
}

// GenUUID generates unique and human-readable IDs.
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
	return len(uuid) >= lenShortID && isAlpha(uuid[0])
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

//
// 3-letter tie breaker (fast)
//

func GenTie() string {
	tie := rtie.Add(1)
	b0 := uuidABC[tie&0x3f]
	b1 := uuidABC[-tie&0x3f]
	b2 := uuidABC[(tie>>2)&0x3f]
	return string([]byte{b0, b1, b2})
}

//
// ETL ID
//

func ValidateEtlID(id string) error {
	return _validateID(id, 6)
}

func _validateID(id string, minlen int) error {
	if len(id) < minlen {
		return fmt.Errorf("ID %q is invalid: too short", id)
	}
	if len(id) >= lenTooLongID {
		return fmt.Errorf("ID %q is invalid: too long", id)
	}
	if !idRegex.MatchString(id) {
		return fmt.Errorf("ID %q is invalid: can only contain [A-Za-z0-9-_]", id)
	}
	return nil
}

//
// Daemon ID
//

func GenDaemonID() string { return RandStringStrong(lenDaemonID) }

func GenTestingDaemonID(suffix string) string {
	l := Max(lenDaemonID-len(suffix), 3)
	return RandStringStrong(l) + suffix
}

func ValidateDaemonID(id string) error {
	return _validateID(id, lenDaemonID)
}
