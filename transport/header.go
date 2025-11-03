// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
)

const (
	// flags
	msgFl       = uint64(1) << (63 - iota) // message vs object demux
	pduFl                                  // is PDU
	pduLastFl                              // is last PDU
	pduStreamFl                            // PDU-based stream

	// NOTE: update when adding/changing flags :NOTE
	allFlags = msgFl | pduFl | pduLastFl | pduStreamFl

	// all 3 headers
	sizeProtoHdr = cos.SizeofI64 * 2
)

//
// proto header: serialization
//

func insObjHeader(hbuf []byte, hdr *ObjHdr, usePDU bool) (off int) {
	debug.Assert(usePDU || !hdr.IsUnsized())
	off = sizeProtoHdr
	off = insString(off, hbuf, hdr.SID)
	off = insUint16(off, hbuf, hdr.Opcode)
	off = insString(off, hbuf, hdr.Bck.Name)
	off = insString(off, hbuf, hdr.Bck.Provider)
	off = insString(off, hbuf, hdr.Bck.Ns.Name)
	off = insString(off, hbuf, hdr.Bck.Ns.UUID)
	off = insString(off, hbuf, hdr.ObjName)
	off = insBytes(off, hbuf, hdr.Opaque)
	off = insString(off, hbuf, hdr.Demux)
	off = insAttrs(off, hbuf, &hdr.ObjAttrs)
	word1 := uint64(off - sizeProtoHdr)
	if usePDU {
		word1 |= pduStreamFl
	}
	insUint64(0, hbuf, word1)
	checksum := xoshiro256.Hash(word1)
	insUint64(cos.SizeofI64, hbuf, checksum)
	return
}

func (pdu *spdu) insHeader() {
	buf, plen := pdu.buf, pdu.plength()
	word1 := uint64(plen) | pduFl
	if pdu.last {
		word1 |= pduLastFl
	}
	insUint64(0, buf, word1)
	checksum := xoshiro256.Hash(word1)
	insUint64(cos.SizeofI64, buf, checksum)
	pdu.done = true
}

func insString(off int, to []byte, str string) int {
	return insBytes(off, to, cos.UnsafeB(str))
}

func insBytes(off int, to, b []byte) int {
	l := len(b)
	debug.Assert(l <= 65535, "the field is uint16")
	binary.BigEndian.PutUint16(to[off:], uint16(l))
	off += cos.SizeofI16
	n := copy(to[off:], b)
	debug.Assert(n == l)
	return off + l
}

func insUint16(off int, to []byte, i int) int {
	debug.Assert(i >= 0 && i < math.MaxUint16)
	binary.BigEndian.PutUint16(to[off:], uint16(i))
	return off + cos.SizeofI16
}

func insInt64(off int, to []byte, i int64) int {
	return insUint64(off, to, uint64(i))
}

func insUint64(off int, to []byte, i uint64) int {
	binary.BigEndian.PutUint64(to[off:], i)
	return off + cos.SizeofI64
}

func insAttrs(off int, to []byte, attr *cmn.ObjAttrs) int {
	off = insInt64(off, to, attr.Size)
	off = insInt64(off, to, attr.Atime)
	if cksum := attr.Checksum(); cksum == nil {
		off = insString(off, to, "")
		off = insString(off, to, "")
	} else {
		off = insString(off, to, cksum.Ty())
		off = insString(off, to, cksum.Val())
	}
	off = insString(off, to, attr.Version())
	custom := attr.GetCustomMD()
	for k, v := range custom {
		debug.Assert(k != "")
		off = insString(off, to, k)
		off = insString(off, to, v)
	}
	off = insString(off, to, "") // term
	return off
}

//
// proto header: deserialization
//

func (it *iterator) extProtoHdr(hbuf []byte) (hlen int, flags uint64, err error) {
	off, word1 := extUint64(0, hbuf)
	hlen = int(word1 & ^allFlags)
	flags = word1 & allFlags
	//
	// validate checksum
	//
	_, checksum := extUint64(0, hbuf[off:])
	chc := xoshiro256.Hash(word1)
	if checksum != chc {
		err = it.newErr(nil, sbrHdrChecksum, fmt.Sprintf("%x != %x (hlen=%d)", checksum, chc, hlen))
	}
	return
}

func ExtObjHeader(body []byte, hlen int) (hdr ObjHdr) {
	var off int
	off, hdr.SID = extString(0, body)
	off, hdr.Opcode = extUint16(off, body)
	off, hdr.Bck.Name = extString(off, body)
	off, hdr.Bck.Provider = extString(off, body)
	off, hdr.Bck.Ns.Name = extString(off, body)
	off, hdr.Bck.Ns.UUID = extString(off, body)
	off, hdr.ObjName = extString(off, body)
	off, hdr.Opaque = extBytes(off, body)
	off, hdr.Demux = extString(off, body)
	off, hdr.ObjAttrs = extAttrs(off, body)
	debug.Assertf(off == hlen, "off %d, hlen %d", off, hlen)
	return
}

func extString(off int, from []byte) (int, string) {
	off, bt := extBytes(off, from)
	return off, string(bt)
}

func extBytes(off int, from []byte) (int, []byte) {
	l := int(binary.BigEndian.Uint16(from[off:]))
	off += cos.SizeofI16
	return off + l, from[off : off+l]
}

func extUint16(off int, from []byte) (int, int) {
	val := binary.BigEndian.Uint16(from[off:])
	off += cos.SizeofI16
	return off, int(val)
}

func extInt64(off int, from []byte) (int, int64) {
	off, val := extUint64(off, from)
	return off, int64(val)
}

func extUint64(off int, from []byte) (int, uint64) {
	val := binary.BigEndian.Uint64(from[off:])
	off += cos.SizeofI64
	return off, val
}

func extAttrs(off int, from []byte) (n int, attr cmn.ObjAttrs) {
	var cksumTyp, cksumVal, k, v string
	off, attr.Size = extInt64(off, from)
	off, attr.Atime = extInt64(off, from)
	off, cksumTyp = extString(off, from)
	off, cksumVal = extString(off, from)
	attr.SetCksum(cksumTyp, cksumVal)
	off, v = extString(off, from)
	attr.SetVersion(v)
	for {
		off, k = extString(off, from)
		if k == "" {
			break
		}
		off, v = extString(off, from)
		attr.SetCustomKey(k, v)
	}
	return off, attr
}

////////////////////
// Obj and ObjHdr //
////////////////////

func (obj *Obj) IsHeaderOnly() bool { return obj.Hdr.IsHeaderOnly() }
func (obj *Obj) IsUnsized() bool    { return obj.Hdr.IsUnsized() }

func (obj *Obj) Size() int64 { return obj.Hdr.ObjSize() }

func (obj *Obj) String() string {
	s := "sobj-" + obj.Hdr.Cname()
	if obj.IsHeaderOnly() {
		return s
	}
	return fmt.Sprintf("%s(size=%d)", s, obj.Hdr.ObjAttrs.Size)
}

func (obj *Obj) SetPrc(n int) {
	obj.prc = atomic.NewInt64(int64(n))
}

func (hdr *ObjHdr) Cname() string { return hdr.Bck.Cname(hdr.ObjName) } // see also: lom.Cname()

func (hdr *ObjHdr) IsUnsized() bool    { return hdr.ObjAttrs.Size == SizeUnknown }
func (hdr *ObjHdr) IsHeaderOnly() bool { return hdr.ObjAttrs.Size == 0 }
func (hdr *ObjHdr) ObjSize() int64     { return hdr.ObjAttrs.Size }

// reserved opcodes
func (hdr *ObjHdr) isFin() bool      { return hdr.Opcode == opcFin }
func (hdr *ObjHdr) isIdleTick() bool { return hdr.Opcode == opcIdleTick }
