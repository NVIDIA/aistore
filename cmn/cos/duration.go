// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

// is used in cmn/config; is known to cmn/iter-fields parser
// (compare w/ size.go)

type Duration time.Duration

func (d Duration) D() time.Duration             { return time.Duration(d) }
func (d Duration) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(d.String()) }

func (d Duration) String() (s string) {
	s = time.Duration(d).String()
	// see related: https://github.com/golang/go/issues/39064
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	return
}

func (d *Duration) UnmarshalJSON(b []byte) (err error) {
	var (
		dur time.Duration
		val string
	)
	if err = jsoniter.Unmarshal(b, &val); err != nil {
		return
	}
	dur, err = time.ParseDuration(val)
	*d = Duration(dur)
	return
}
