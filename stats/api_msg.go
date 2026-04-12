// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
package stats

import "github.com/tinylib/msgp/msgp"

// copyValue encodes as a bare int64 (not a 1-element map).
// Wire savings: ~2-3 bytes * ~200 metrics = ~400-600 bytes per NodeStatus.
// NOTE: msgpack only; JSON form unchanged for backward compat.

func (z *copyValue) DecodeMsg(dc *msgp.Reader) (err error) {
	z.Value, err = dc.ReadInt64()
	return
}

func (z copyValue) EncodeMsg(en *msgp.Writer) error {
	return en.WriteInt64(z.Value)
}

func (copyValue) Msgsize() int { return msgp.Int64Size }

func (z *copyTracker) DecodeMsg(dc *msgp.Reader) error {
	sz, err := dc.ReadMapHeader()
	if err != nil {
		return err
	}
	if *z == nil {
		*z = make(copyTracker, sz)
	}
	for range sz {
		k, err := dc.ReadString()
		if err != nil {
			return err
		}
		var v copyValue
		if err := v.DecodeMsg(dc); err != nil {
			return err
		}
		(*z)[k] = v
	}
	return nil
}

func (z copyTracker) EncodeMsg(en *msgp.Writer) error {
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

func (z copyTracker) Msgsize() int {
	s := msgp.MapHeaderSize
	for k, v := range z {
		s += msgp.StringPrefixSize + len(k) + v.Msgsize()
	}
	return s
}
