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
	uuidABC  = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"
	minIDLen = 5
	maxIDLen = 21
)

var (
	sid     *shortid.Shortid
	rtie    atomic.Int32
	idRegex = regexp.MustCompile("^[A-Za-z][A-Za-z0-9-_]{4,20}$")
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
	const idlen = 9 // as per https://github.com/teris-io/shortid#id-length
	return len(uuid) >= idlen && isAlpha(uuid[0])
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// misc
func GenTie() string {
	tie := rtie.Add(1)
	b0 := uuidABC[tie&0x3f]
	b1 := uuidABC[-tie&0x3f]
	b2 := uuidABC[(tie>>2)&0x3f]
	return string([]byte{b0, b1, b2})
}

func ValidateID(id string) error {
	if len(id) <= minIDLen || len(id) >= maxIDLen {
		return fmt.Errorf("invalid ID: length should be > %d and < %d", minIDLen, maxIDLen)
	}

	if !idRegex.MatchString(id) {
		return fmt.Errorf("invalid ID: can contain only alphabets, numbers, '_', or '-'")
	}
	return nil
}
