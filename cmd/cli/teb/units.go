// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"k8s.io/apimachinery/pkg/util/duration"
)

const (
	UnitsIEC = "iec" // default
	UnitsSI  = "si"  // NOTE: currently, SI system is CLI-only (compare with cmn/cos/size.go)
	UnitsRaw = "raw"
)

func ValidateUnits(units string) error {
	switch units {
	case "", UnitsIEC, UnitsSI, UnitsRaw:
		return nil
	default:
		return fmt.Errorf("expecting one of: %q (default), %q, %q", UnitsIEC, UnitsSI, UnitsRaw)
	}
}

func AltFuncMapSizeBytes(units string) (m template.FuncMap) {
	m = make(template.FuncMap, 3)
	m["FormatBytesSig"] = func(size int64, digits int) string { return fmtSize(size, units, digits) }
	m["FormatBytesUns"] = func(size uint64, digits int) string { return fmtSize(int64(size), units, digits) }
	m["FormatMAM"] = func(u int64) string { return fmt.Sprintf("%-10s", fmtSize(u, units, 2)) }
	return
}

func fmtSize(size int64, units string, digits int) string {
	switch units {
	case "", UnitsIEC:
		return cos.ToSizeIEC(size, digits)
	case UnitsSI:
		return ToSizeSI(size, digits)
	case UnitsRaw:
		return strconv.FormatInt(size, 10)
	default:
		debug.Assert(false, units)
		return ""
	}
}

func ToSizeSI(b int64, digits int) string {
	switch {
	case b >= cos.TB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(cos.TB), "TB")
	case b >= cos.GB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(cos.GB), "GB")
	case b >= cos.MB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(cos.MB), "MB")
	case b >= cos.KB:
		return fmt.Sprintf("%.*f%s", digits, float32(b)/float32(cos.KB), "KB")
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// (with B, ns, and /s suffix)
func FmtStatValue(name, kind string, value int64, units string) string {
	if value == 0 {
		return "0"
	}
	// uptime
	if strings.HasSuffix(name, ".time") {
		if units == UnitsRaw {
			return fmt.Sprintf("%dns", value)
		}
		dur := time.Duration(value)
		return duration.HumanDuration(dur)
	}
	// units (enum)
	switch units {
	case UnitsRaw:
		switch kind {
		case stats.KindLatency:
			return fmt.Sprintf("%dns", value)
		case stats.KindSize:
			return fmt.Sprintf("%dB", value)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return fmt.Sprintf("%dB/s", value)
		default:
			return fmt.Sprintf("%d", value)
		}
	case "", UnitsIEC:
		switch kind {
		case stats.KindLatency:
			dur := time.Duration(value)
			return dur.String()
		case stats.KindSize:
			return cos.ToSizeIEC(value, 2)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return cos.ToSizeIEC(value, 2) + "/s"
		default:
			return fmt.Sprintf("%d", value)
		}
	case UnitsSI:
		switch kind {
		case stats.KindLatency:
			dur := time.Duration(value)
			return dur.String()
		case stats.KindSize:
			return ToSizeSI(value, 2)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return ToSizeSI(value, 2) + "/s"
		default:
			return fmt.Sprintf("%d", value)
		}
	}
	debug.Assert(false, units)
	return ""
}
