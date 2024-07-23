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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
)

type unitsCtx struct {
	units string
}

func (ctx *unitsCtx) sizeSig(siz int64, digits int) string {
	return FmtSize(siz, ctx.units, digits)
}

func (ctx *unitsCtx) sizeSig2(siz int64, digits int, flags uint16) string {
	return fmtSize2(siz, ctx.units, digits, flags)
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

func (ctx *unitsCtx) durHuman(dur time.Duration) string {
	return FmtDuration(dur.Nanoseconds(), ctx.units)
}

func FuncMapUnits(units string) (m template.FuncMap) {
	ctx := &unitsCtx{units}
	m = make(template.FuncMap, 6)
	m["FormatBytesSig"] = ctx.sizeSig
	m["FormatBytesSig2"] = ctx.sizeSig2
	m["FormatBytesUns"] = ctx.sizeUns
	m["FormatMAM"] = ctx.sizeMam
	m["FormatMilli"] = ctx.durMilli
	m["FormatDuration"] = ctx.durHuman
	return m
}

func ValidateUnits(units string) error {
	switch units {
	case "", cos.UnitsIEC, cos.UnitsSI, cos.UnitsRaw:
		return nil
	default:
		return fmt.Errorf("invalid %q (expecting one of: %s, %s, %s or \"\")", units, cos.UnitsIEC, cos.UnitsSI, cos.UnitsRaw)
	}
}

func FmtSize(size int64, units string, digits int) string {
	switch units {
	case "", cos.UnitsIEC:
		return cos.ToSizeIEC(size, digits)
	case cos.UnitsSI:
		return toSizeSI(size, digits)
	case cos.UnitsRaw:
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

func fmtSize2(size int64, units string, digits int, flags uint16) string {
	if flags&apc.EntryIsDir != 0 {
		return ""
	}
	return FmtSize(size, units, digits)
}

// (with B, ns, and /s suffix)
func FmtStatValue(name, kind string, value int64, units string) string {
	if value == 0 {
		return "0"
	}
	// uptime
	if strings.HasSuffix(name, ".time") || kind == stats.KindLatency || kind == stats.KindTotal {
		return FmtDuration(value, units)
	}
	// units (enum)
	switch units {
	case cos.UnitsRaw:
		switch kind {
		case stats.KindSize:
			return fmt.Sprintf("%dB", value)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return fmt.Sprintf("%dB/s", value)
		default:
			return strconv.FormatInt(value, 10)
		}
	case "", cos.UnitsIEC:
		switch kind {
		case stats.KindSize:
			return cos.ToSizeIEC(value, 2)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return cos.ToSizeIEC(value, 2) + "/s"
		default:
			return strconv.FormatInt(value, 10)
		}
	case cos.UnitsSI:
		switch kind {
		case stats.KindSize:
			return toSizeSI(value, 2)
		case stats.KindThroughput, stats.KindComputedThroughput:
			return toSizeSI(value, 2) + "/s"
		default:
			return strconv.FormatInt(value, 10)
		}
	}
	debug.Assert(false, units)
	return ""
}

func FmtDuration(ns int64, units string) string {
	if units == cos.UnitsRaw {
		return fmt.Sprintf("%dns", ns)
	}
	return FormatDuration(time.Duration(ns))
}

// round to an assorted set of multiples
func FormatDuration(d time.Duration) string {
	switch {
	case d < 100*time.Millisecond:
		// do nothing on purpose
	case d < time.Second:
		d = d.Round(time.Millisecond)
	case d < 10*time.Second:
		d = d.Round(10 * time.Millisecond)
	case d < 100*time.Second:
		d = d.Round(100 * time.Millisecond)
	default:
		d = d.Round(time.Second)
	}
	return d.String()
}

func fmtMilli(val cos.Duration, units string) string {
	if units == cos.UnitsRaw {
		return fmt.Sprintf("%dns", val)
	}
	return cos.FormatMilli(time.Duration(val))
}
