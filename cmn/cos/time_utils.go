// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// in addition to standard layouts at /usr/local/go/src/time/format.go
const (
	FmtTimestamp = "15:04:05.000000" // internal (same as glog)

	// S3 ListObjectsV2
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
	ISO8601 = "2006-01-02T15:04:05.000Z"

	// S3 HeadObject
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_Examples
	RFC1123GMT = "Mon, 17 Dec 2012 02:14:10 GMT"
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
	case RFC1123GMT:
		s := t.UTC().Format(time.RFC1123)
		debug.Assert(strings.HasSuffix(s, "UTC"), s)
		return strings.TrimSuffix(s, "UTC") + "GMT"
	default:
		return t.Format(format)
	}
}

func S2Duration(s string) (time.Duration, error) {
	d, err := strconv.ParseInt(s, 0, 64)
	return time.Duration(d), err
}

func UnixNano2S(unixnano int64) string   { return strconv.FormatInt(unixnano, 10) }
func S2UnixNano(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
func IsTimeZero(t time.Time) bool        { return t.IsZero() || t.UTC().Unix() == 0 } // https://github.com/golang/go/issues/33597

// wait duration => probing frequency
func ProbingFrequency(dur time.Duration) time.Duration {
	sleep := MinDuration(dur>>3, time.Second)
	sleep = MaxDuration(dur>>6, sleep)
	return MaxDuration(sleep, 100*time.Millisecond)
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
