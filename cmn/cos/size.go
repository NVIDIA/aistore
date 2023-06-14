// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

const (
	UnitsIEC = "iec" // default
	UnitsSI  = "si"  // NOTE: currently, SI system is CLI-only (compare with cmn/cos/size.go)
	UnitsRaw = "raw"
)

// IEC (binary) units
const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

// IS (metric) units
const (
	KB = 1000
	MB = 1000 * KB
	GB = 1000 * MB
	TB = 1000 * GB
)

func _suffix(s string) string {
	switch {
	case strings.HasSuffix(s, "KIB"):
		return "KIB"
	case strings.HasSuffix(s, "MIB"):
		return "MIB"
	case strings.HasSuffix(s, "GIB"):
		return "GIB"
	case strings.HasSuffix(s, "TIB"):
		return "TIB"
	case strings.HasSuffix(s, "KB"):
		return "KB"
	case strings.HasSuffix(s, "MB"):
		return "MB"
	case strings.HasSuffix(s, "GB"):
		return "GB"
	case strings.HasSuffix(s, "TB"):
		return "TB"
	case strings.HasSuffix(s, "K"):
		return "K"
	case strings.HasSuffix(s, "M"):
		return "M"
	case strings.HasSuffix(s, "G"):
		return "G"
	case strings.HasSuffix(s, "T"):
		return "T"
	case strings.HasSuffix(s, "B"):
		return "B"
	default:
		return ""
	}
}

/////////////
// SizeIEC //
/////////////

// is used in cmn/config; is known*** to cmn/iter-fields parser (compare w/ duration.go)

type SizeIEC int64

func (siz SizeIEC) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(siz.String()) }
func (siz SizeIEC) String() string               { return ToSizeIEC(int64(siz), 0) }

func (siz *SizeIEC) UnmarshalJSON(b []byte) (err error) {
	var (
		n   int64
		val string
	)
	if err = jsoniter.Unmarshal(b, &val); err != nil {
		return
	}
	n, err = ParseSize(val, UnitsIEC)
	*siz = SizeIEC(n)
	return
}

// (compare w/ CLI `ToSizeIS`)
func ToSizeIEC(b int64, digits int) string {
	switch {
	case b >= TiB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(TiB), "TiB")
	case b >= GiB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(GiB), "GiB")
	case b >= MiB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(MiB), "MiB")
	case b >= KiB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(KiB), "KiB")
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// when `units` arg is empty conversion is defined by the suffix
func ParseSize(size, units string) (int64, error) {
	if size == "" {
		return 0, nil
	}
	// validation
	if len(units) > 0 {
		switch units {
		case "", UnitsIEC, UnitsSI, UnitsRaw:
		default:
			return 0, fmt.Errorf("ParseSize %q: invalid units %q (expecting %s, %s, or %s)", size, units,
				UnitsRaw, UnitsSI, UnitsIEC)
		}
	}
	// units, more validation
	var (
		u      = UnitsRaw
		s      = strings.ToUpper(strings.TrimSpace(size))
		suffix = _suffix(s)
	)
	if suffix == "KIB" || suffix == "MIB" || suffix == "GIB" || suffix == "TIB" {
		u = UnitsIEC
		if units != "" && units != UnitsIEC {
			return 0, fmt.Errorf("ParseSize %q error: %q vs %q units", size, u, units)
		}
	} else if suffix != "" && suffix != "B" {
		u = UnitsSI
		if units != "" {
			if units == UnitsRaw {
				return 0, fmt.Errorf("ParseSize %q error: %q vs %q units", size, u, units)
			}
			// NOTE: the case when units (arg) take precedence over the suffix
			u = units
		}
	}
	// trim suffix and convert
	if len(suffix) > 0 {
		s = strings.TrimSuffix(s, suffix)
	}
	switch {
	case strings.IndexByte(suffix, 'K') >= 0:
		return _convert(s, u, KB, KiB)
	case strings.IndexByte(suffix, 'M') >= 0:
		return _convert(s, u, MB, MiB)
	case strings.IndexByte(suffix, 'G') >= 0:
		return _convert(s, u, GB, GiB)
	case strings.IndexByte(suffix, 'T') >= 0:
		return _convert(s, u, TB, TiB)
	default:
		return _convert(s, u, 1, 1)
	}
}

func _convert(s, units string, mult, multIEC int64) (val int64, err error) {
	if strings.IndexByte(s, '.') >= 0 {
		var f float64
		f, err = strconv.ParseFloat(s, 64)
		if err != nil {
			return
		}
		if units == UnitsIEC {
			return int64(f * float64(multIEC)), err
		}
		return int64(f * float64(mult)), err
	}
	val, err = strconv.ParseInt(s, 10, 64)
	if units == UnitsIEC {
		return val * multIEC, err
	}
	return val * mult, err
}
