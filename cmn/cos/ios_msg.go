// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "github.com/tinylib/msgp/msgp"

// This is hand-written `*_msg.go` for types that `msgp` cannot follow across sources (from fs.TcdfExt).
// See scripts/msgp/README.md for the existing msgpack coverage and notes.

func (z *AllDiskStats) DecodeMsg(dc *msgp.Reader) error {
	sz, err := dc.ReadMapHeader()
	if err != nil {
		return err
	}
	if *z == nil {
		*z = make(AllDiskStats, sz)
	}
	for range sz {
		k, err := dc.ReadString()
		if err != nil {
			return err
		}
		var v DiskStats
		if err := v.DecodeMsg(dc); err != nil {
			return err
		}
		(*z)[k] = v
	}
	return nil
}

func (z AllDiskStats) EncodeMsg(en *msgp.Writer) error {
	if err := en.WriteMapHeader(uint32(len(z))); err != nil {
		return err
	}
	for k, v := range z {
		if err := en.WriteString(k); err != nil {
			return err
		}
		if err := v.EncodeMsg(en); err != nil {
			return err
		}
	}
	return nil
}

func (z AllDiskStats) Msgsize() int {
	s := msgp.MapHeaderSize
	for k, v := range z {
		s += msgp.StringPrefixSize + len(k) + v.Msgsize()
	}
	return s
}
