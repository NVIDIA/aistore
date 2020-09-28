// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"strconv"
	"time"
)

const (
	timestampFormat = "15:04:05.000000"
)

func FormatUnixNano(unixnano int64, format string) string {
	t := time.Unix(0, unixnano)
	switch format {
	case "", RFC822:
		return t.Format(time.RFC822)
	default:
		return t.Format(format)
	}
}

func FormatTimestamp(tm time.Time) string { return tm.Format(timestampFormat) }

func S2Duration(s string) (time.Duration, error) {
	d, err := strconv.ParseInt(s, 0, 64)
	return time.Duration(d), err
}

func UnixNano2S(unixnano int64) string   { return strconv.FormatInt(unixnano, 10) }
func S2UnixNano(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
func IsTimeZero(t time.Time) bool        { return t.IsZero() || t.UTC().Unix() == 0 } // https://github.com/golang/go/issues/33597
