// Package aisloader
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"flag"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
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

// interface guard
var (
	_ flag.Value = (*BoolExt)(nil)
	_ flag.Value = (*DurationExt)(nil)
)

func (b *BoolExt) Set(s string) (err error) {
	b.Val, err = cos.ParseBool(s)
	b.IsSet = true
	return err
}

func (b *BoolExt) Get() any       { return b.Val }
func (b *BoolExt) String() string { return strconv.FormatBool(b.Val) }
func (*BoolExt) IsBoolFlag() bool { return true }

func BoolExtVar(f *flag.FlagSet, p *BoolExt, name, usage string) {
	f.Var(p, name, usage)
}

func (d *DurationExt) Set(s string) (err error) {
	d.Val, err = time.ParseDuration(s)
	d.IsSet = true
	return err
}

func (d *DurationExt) Get() any       { return d.Val }
func (d *DurationExt) String() string { return d.Val.String() }
func (*DurationExt) IsBoolFlag() bool { return false }

func DurationExtVar(f *flag.FlagSet, p *DurationExt, name string, defValue time.Duration, usage string) {
	p.Val = defValue
	f.Var(p, name, usage)
}
