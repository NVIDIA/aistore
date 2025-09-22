// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// in addition to standard layouts at /usr/local/go/src/time/format.go
const (
	StampMicro = "15:04:05.000000" // time.StampMicro without a date
	StampSec   = "15:04:05"        // time.Stamp without a date
	StampSec2  = "150405"          // HHMMSS (same as above but without ':')

	// S3 ListObjectsV2
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
	ISO8601 = "2006-01-02T15:04:05.000Z"
)

// `unixnano` here is expected to be "nanoseconds since January 1, 1970 UTC"
func FormatNanoTime(unixnano int64, format string) string {
	t := time.Unix(0, unixnano)
	return FormatTime(t, format)
}

func FormatTime(t time.Time, format string) string {
	switch format {
	case "", time.RFC822:
		return t.Format(time.RFC822) // default
	default:
		return t.Format(format)
	}
}

func FormatNowStamp() string { return FormatTime(time.Now(), StampMicro) }

func S2Duration(s string) (time.Duration, error) {
	d, err := strconv.ParseInt(s, 0, 64)
	return time.Duration(d), err
}

func UnixNano2S(unixnano int64) string   { return strconv.FormatInt(unixnano, 10) }
func S2UnixNano(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
func IsTimeZero(t time.Time) bool        { return t.IsZero() || t.UTC().Unix() == 0 } // https://github.com/golang/go/issues/33597

// wait duration => probing frequency
func ProbingFrequency(dur time.Duration) time.Duration {
	sleep := min(dur>>3, time.Second)
	sleep = max(dur>>6, sleep)
	return max(sleep, 100*time.Millisecond)
}

// constrain duration `d` to the closed interval [mind, maxd]
func ClampDuration(d, mind, maxd time.Duration) time.Duration {
	debug.Assert(mind <= maxd, mind, " vs ", maxd)
	if d < mind {
		return mind
	}
	return min(d, maxd)
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

// access time validity; prefetch special (negative) case - sets atime=-now
// 946771140000000000 = time.Parse(time.RFC3339Nano, "2000-01-01T23:59:00Z").UnixNano()
func IsValidAtime(atime int64) bool {
	return atime > 946771140000000000 ||
		(atime < -946771140000000000 && atime != -6795364578871345152) // time.IsZero()
}
