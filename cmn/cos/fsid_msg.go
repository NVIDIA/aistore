// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "github.com/tinylib/msgp/msgp"

// NOTE:
// - this is hand-written `*_msg.go` for types that `msgp` cannot follow across sources
// - FsID is [2]int32

func (z *FsID) DecodeMsg(dc *msgp.Reader) error {
	sz, err := dc.ReadArrayHeader()
	if err != nil {
		return err
	}
	if sz != 2 {
		return msgp.ArrayError{Wanted: 2, Got: sz}
	}
	if z[0], err = dc.ReadInt32(); err != nil {
		return err
	}
	z[1], err = dc.ReadInt32()
	return err
}

func (z FsID) EncodeMsg(en *msgp.Writer) error {
	if err := en.WriteArrayHeader(2); err != nil {
		return err
	}
	if err := en.WriteInt32(z[0]); err != nil {
		return err
	}
	return en.WriteInt32(z[1])
}

func (FsID) Msgsize() int { return msgp.ArrayHeaderSize + 2*msgp.Int32Size }
