// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"flag"
	"strconv"
	"time"
)

type (
	BoolExt struct {
		IsSet bool
		Val   bool
	}

	DurationExt struct {
		IsSet bool
		Val   time.Duration
	}
)

var (
	_ flag.Value = &BoolExt{}
	_ flag.Value = &DurationExt{}
)

func (b *BoolExt) Set(s string) (err error) {
	b.Val, err = strconv.ParseBool(s)
	b.IsSet = true
	return err
}

func (b *BoolExt) Get() interface{} { return b.Val }
func (b *BoolExt) String() string   { return strconv.FormatBool(b.Val) }
func (b *BoolExt) IsBoolFlag() bool { return true }
func BoolExtVar(f *flag.FlagSet, p *BoolExt, name string, usage string) {
	f.Var(p, name, usage)
}

func (d *DurationExt) Set(s string) (err error) {
	d.Val, err = time.ParseDuration(s)
	d.IsSet = true
	return err
}

func (d *DurationExt) Get() interface{} { return d.Val }
func (d *DurationExt) String() string   { return d.Val.String() }
func (d *DurationExt) IsBoolFlag() bool { return false }
func DurationExtVar(f *flag.FlagSet, p *DurationExt, name string, defValue time.Duration, usage string) {
	p.Val = defValue
	f.Var(p, name, usage)
}
