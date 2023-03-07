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

/////////////
// SizeIEC //
/////////////

// is used in cmn/config; is known*** to cmn/iter-fields parser (compare w/ duration.go)

type SizeIEC int64

var iecBytes = map[string]int64{
	"K":   KiB,
	"KB":  KiB,
	"KIB": KiB,
	"M":   MiB,
	"MB":  MiB,
	"MIB": MiB,
	"G":   GiB,
	"GB":  GiB,
	"GIB": GiB,
	"T":   TiB,
	"TB":  TiB,
	"TIB": TiB,
}

var siBytes = map[string]int64{
	"K":  KB,
	"KB": KB,
	"M":  MB,
	"MB": MB,
	"G":  GB,
	"GB": GB,
	"T":  TB,
	"TB": TB,
}

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

func ParseSize(size, units string) (int64, error) {
	if size == "" {
		return 0, nil
	}
	switch units {
	case "", UnitsIEC: // NOTE default
		return _parseSize(size, iecBytes)
	case UnitsSI:
		return _parseSize(size, siBytes)
	case UnitsRaw:
		return strconv.ParseInt(size, 10, 64)
	}
	return 0, fmt.Errorf("invalid %q (expecting one of: %s, %s, %s or \"\")", units, UnitsIEC, UnitsSI, UnitsRaw)
}

func _parseSize(s string, multipliers map[string]int64) (int64, error) {
	s = strings.ToUpper(s)
	for k, v := range multipliers {
		if ns := strings.TrimSuffix(s, k); ns != s {
			if strings.IndexByte(ns, '.') >= 0 {
				f, err := strconv.ParseFloat(strings.TrimSpace(ns), 64)
				return int64(float64(v) * f), err
			}
			i, err := strconv.ParseInt(strings.TrimSpace(ns), 10, 64)
			return i * v, err
		}
	}
	ns := strings.TrimSuffix(s, "B")
	return strconv.ParseInt(strings.TrimSpace(ns), 10, 64)
}
