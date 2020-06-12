// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/teris-io/shortid"
)

const (
	// Alphabet for generating UUIDs similar to the shortid.DEFAULT_ABC
	// NOTE: len(uuidABC) > 0x3f - see GenTie()
	uuidABC = "-5nZJDft6LuzsjGNpPwY7rQa39vehq4i1cV2FROo8yHSlC0BUEdWbIxMmTgKXAk_"
)

var (
	sids [16]*shortid.Shortid
)

func InitShortid(seed uint64) {
	for i := range sids {
		sids[i] = shortid.MustNew(uint8(i+1) /*worker*/, uuidABC, seed)
	}
}

// GenUUID generates unique and user-friendly IDs.
func GenUUID() (uuid string) {
	var (
		err  error
		h, t string
		now  = mono.NanoTime()
		i    = int(now & int64(len(sids)-1))
	)
	for {
		sid := sids[i]
		uuid, err = sid.Generate()
		if err != nil {
			i = (i + 1) % len(sids)
			continue
		}
		if !isalpha(uuid[0]) {
			h = string('A' + i)
		}
		u := uuid[len(uuid)-1]
		if u == '-' || u == '_' {
			t = string('a' + i)
		}
		return h + uuid + t
	}
}

func isalpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' || c <= 'Z')
}

// misc
func GenTie() string {
	tie := rtie.Add(1)
	b0 := uuidABC[tie&0x3f]
	b1 := uuidABC[-tie&0x3f]
	b2 := uuidABC[(tie>>2)&0x3f]
	return string([]byte{b0, b1, b2})
}
