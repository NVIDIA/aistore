// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
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

// wait duration => probing frequency
func CalcProbeFreq(dur time.Duration) time.Duration {
	sleep := MinDuration(dur/20, time.Second)
	sleep = MaxDuration(dur/100, sleep)
	return MaxDuration(sleep, 10*time.Millisecond)
}

// FormatMilli returns a duration formatted as milliseconds. For values bigger
// than millisecond, it returns an integer number "#ms". For values smaller than
// millisecond, the function returns fractional number "0.##ms"
func FormatMilli(tm time.Duration) string {
	milli := tm.Milliseconds()
	if milli > 0 {
		return fmt.Sprintf("%dms", milli)
	}
	micro := tm.Microseconds()
	if micro == 0 {
		return "0"
	}
	return fmt.Sprintf("%.2fms", float64(micro)/1000.0)
}
