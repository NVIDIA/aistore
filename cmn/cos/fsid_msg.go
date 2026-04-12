// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
package cos

import "github.com/tinylib/msgp/msgp"

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
