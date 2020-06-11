// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// NOTE: BEWARE: `shortid` uses hardcoded 01/2016 as a starting timestamp
import "github.com/teris-io/shortid"

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
	var err error
	for _, sid := range sids {
		uuid, err = sid.Generate()
		if err == nil &&
			uuid[0] != '-' && uuid[0] != '_' && uuid[len(uuid)-1] != '-' && uuid[len(uuid)-1] != '_' {
			return
		}
	}
	return RandString(9)
}

// misc
func GenTie() string {
	tie := rtie.Add(1)
	b0 := uuidABC[tie&0x3f]
	b1 := uuidABC[-tie&0x3f]
	b2 := uuidABC[(tie>>2)&0x3f]
	return string([]byte{b0, b1, b2})
}
