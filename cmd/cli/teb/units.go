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

type unitsCtx struct {
	units string
}

func (ctx *unitsCtx) sizeSig(siz int64, digits int) string {
	return FmtSize(siz, ctx.units, digits)
}

func (ctx *unitsCtx) sizeUns(siz uint64, digits int) string {
	return FmtSize(int64(siz), ctx.units, digits)
}

func (ctx *unitsCtx) sizeMam(u int64) string {
	return fmt.Sprintf("%-10s", FmtSize(u, ctx.units, 2))
}

func (ctx *unitsCtx) durMilli(dur cos.Duration) string {
	return fmtMilli(dur, ctx.units)
}

func FuncMapUnits(units string) (m template.FuncMap) {
	ctx := &unitsCtx{units}
	m = make(template.FuncMap, 4)
	m["FormatBytesSig"] = ctx.sizeSig
	m["FormatBytesUns"] = ctx.sizeUns
	m["FormatMAM"] = ctx.sizeMam
	m["FormatMilli"] = ctx.durMilli
	return
}

func ValidateUnits(units string) error {
	switch units {
	case "", UnitsIEC, UnitsSI, UnitsRaw:
		return nil
	default:
		return fmt.Errorf("expecting one of: %q (default), %q, %q", UnitsIEC, UnitsSI, UnitsRaw)
	}
}

func FmtSize(size int64, units string, digits int) string {
	switch units {
	case "", UnitsIEC:
		return cos.ToSizeIEC(size, digits)
	case UnitsSI:
		return toSizeSI(size, digits)
	case UnitsRaw:
		return strconv.FormatInt(size, 10)
	default:
		debug.Assert(false, units)
		return ""
	}
}

func toSizeSI(b int64, digits int) string {
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
	if strings.HasSuffix(name, ".time") || kind == stats.KindLatency {
		return fmtDuration(value, units)
	}
	// units (enum)
	switch units {
	case UnitsRaw:
		switch kind {
		case stats.KindSize:
			return fmt.Sprintf("%dB", value)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return fmt.Sprintf("%dB/s", value)
		default:
			return fmt.Sprintf("%d", value)
		}
	case "", UnitsIEC:
		switch kind {
		case stats.KindSize:
			return cos.ToSizeIEC(value, 2)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return cos.ToSizeIEC(value, 2) + "/s"
		default:
			return fmt.Sprintf("%d", value)
		}
	case UnitsSI:
		switch kind {
		case stats.KindSize:
			return toSizeSI(value, 2)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return toSizeSI(value, 2) + "/s"
		default:
			return fmt.Sprintf("%d", value)
		}
	}
	debug.Assert(false, units)
	return ""
}

func fmtDuration(ns int64, units string) string {
	if units == UnitsRaw {
		return fmt.Sprintf("%dns", ns)
	}
	dur := time.Duration(ns)
	if dur > 10*time.Second {
		return duration.HumanDuration(dur)
	}
	return dur.String()
}

func fmtMilli(val cos.Duration, units string) string {
	if units == UnitsRaw {
		return fmt.Sprintf("%dns", val)
	}
	return cos.FormatMilli(time.Duration(val))
}
