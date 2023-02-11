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

var toBiBytes = map[string]int64{
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
	n, err = ParseSizeIEC(val)
	*siz = SizeIEC(n)
	return
}

func ParseSizeIEC(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	s = strings.ToUpper(s)
	for k, v := range toBiBytes {
		if ns := strings.TrimSuffix(s, k); ns != s {
			f, err := strconv.ParseFloat(strings.TrimSpace(ns), 64)
			return int64(float64(v) * f), err
		}
	}
	ns := strings.TrimSuffix(s, "B")
	f, err := strconv.ParseFloat(strings.TrimSpace(ns), 64)
	return int64(f), err
}

func ToSizeIEC(b int64, digits int) string {
	if b >= TiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(TiB), "TiB")
	}
	if b >= GiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(GiB), "GiB")
	}
	if b >= MiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(MiB), "MiB")
	}
	if b >= KiB {
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(KiB), "KiB")
	}
	return fmt.Sprintf("%dB", b)
}
