// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xoshiro256"
)

const (
	// header flags
	msgMask = uint64(1 << (63 - iota)) // message vs object demux
	PDUMask                            // PDU
	lastPDU                            // last in unsized

	// header sizes
	sizeObjHdr = cmn.SizeofI64 * 2
	sizeMsgHdr = cmn.SizeofI64 * 2
	sizePDUHdr = cmn.SizeofI64 * 2

	lastMarker = math.MaxInt64              // end of stream: send and receive
	tickMarker = math.MaxInt64 ^ 0xa5a5a5a5 // idle tick - send side only
)

////////////////////////////////
// proto header serialization //
////////////////////////////////

func insObjHeader(hbuf []byte, hdr *ObjHdr) (off int) {
	off = sizeObjHdr
	off = insString(off, hbuf, hdr.Bck.Name)
	off = insString(off, hbuf, hdr.ObjName)
	off = insString(off, hbuf, hdr.Bck.Provider)
	off = insString(off, hbuf, hdr.Bck.Ns.Name)
	off = insString(off, hbuf, hdr.Bck.Ns.UUID)
	off = insByte(off, hbuf, hdr.Opaque)
	off = insAttrs(off, hbuf, hdr.ObjAttrs)
	hlen := uint64(off - sizeObjHdr)
	insUint64(0, hbuf, hlen)
	checksum := xoshiro256.Hash(hlen)
	insUint64(cmn.SizeofI64, hbuf, checksum)
	return
}

func insMsg(hbuf []byte, msg *Msg) (off int) {
	off = sizeMsgHdr
	off = insInt64(off, hbuf, msg.Flags)
	off = insByte(off, hbuf, msg.Body)
	hlen := uint64(off - sizeMsgHdr)
	insUint64(0, hbuf, hlen|msgMask)
	checksum := xoshiro256.Hash(hlen)
	insUint64(cmn.SizeofI64, hbuf, checksum)
	return
}

func insPDUHeader(buf []byte, paylen int, err error) {
	size := uint64(paylen)
	// NOTE: partially filled PDU and any error triggers last-PDU
	if err != nil || paylen < len(buf)-sizePDUHdr {
		size |= lastPDU
	}
	insUint64(0, buf, size|PDUMask)
	checksum := xoshiro256.Hash(size)
	insUint64(cmn.SizeofI64, buf, checksum)
}

func insString(off int, to []byte, str string) int {
	return insByte(off, to, []byte(str))
}

func insByte(off int, to, b []byte) int {
	l := len(b)
	binary.BigEndian.PutUint64(to[off:], uint64(l))
	off += cmn.SizeofI64
	n := copy(to[off:], b)
	cmn.Assert(n == l)
	return off + l
}

func insInt64(off int, to []byte, i int64) int {
	return insUint64(off, to, uint64(i))
}

func insUint64(off int, to []byte, i uint64) int {
	binary.BigEndian.PutUint64(to[off:], i)
	return off + cmn.SizeofI64
}

func insAttrs(off int, to []byte, attr ObjectAttrs) int {
	off = insInt64(off, to, attr.Size)
	off = insInt64(off, to, attr.Atime)
	off = insString(off, to, attr.CksumType)
	off = insString(off, to, attr.CksumValue)
	off = insString(off, to, attr.Version)
	return off
}

//////////////////////////////////
// proto header deserialization //
//////////////////////////////////

func extProtoHdr(hbuf []byte, trname string) (hlen int, isObj bool, err error) {
	_, hl64ex := extUint64(0, hbuf)
	isObj = hl64ex&msgMask == 0
	hl64 := int64(hl64ex & ^msgMask)
	hlen = int(hl64)
	if hlen > len(hbuf) {
		err = fmt.Errorf("sbrk %s: hlen %d exceeds buf %d", trname, hlen, len(hbuf))
		return
	}
	// validate checksum
	_, checksum := extUint64(0, hbuf[cmn.SizeofI64:])
	chc := xoshiro256.Hash(uint64(hl64))
	if checksum != chc {
		err = fmt.Errorf("sbrk %s: bad checksum %x != %x (hlen=%d)", trname, checksum, chc, hlen)
	}
	return
}

func ExtObjHeader(body []byte, hlen int) (hdr ObjHdr) {
	var off int
	off, hdr.Bck.Name = extString(0, body)
	off, hdr.ObjName = extString(off, body)
	off, hdr.Bck.Provider = extString(off, body)
	off, hdr.Bck.Ns.Name = extString(off, body)
	off, hdr.Bck.Ns.UUID = extString(off, body)
	off, hdr.Opaque = extByte(off, body)
	off, hdr.ObjAttrs = extAttrs(off, body)
	debug.Assertf(off == hlen, "off %d, hlen %d", off, hlen)
	return
}

func ExtMsg(body []byte, hlen int) (msg Msg) {
	var off int
	off, msg.Flags = extInt64(0, body)
	off, msg.Body = extByte(off, body)
	debug.Assertf(off == hlen, "off %d, hlen %d", off, hlen)
	return
}

func extString(off int, from []byte) (int, string) {
	off, bt := extByte(off, from)
	return off, string(bt)
}

func extByte(off int, from []byte) (int, []byte) {
	l := int(binary.BigEndian.Uint64(from[off:]))
	off += cmn.SizeofI64
	return off + l, from[off : off+l]
}

func extInt64(off int, from []byte) (int, int64) {
	off, val := extUint64(off, from)
	return off, int64(val)
}

func extUint64(off int, from []byte) (int, uint64) {
	size := binary.BigEndian.Uint64(from[off:])
	off += cmn.SizeofI64
	return off, size
}

func extAttrs(off int, from []byte) (n int, attr ObjectAttrs) {
	off, attr.Size = extInt64(off, from)
	off, attr.Atime = extInt64(off, from)
	off, attr.CksumType = extString(off, from)
	off, attr.CksumValue = extString(off, from)
	off, attr.Version = extString(off, from)
	return off, attr
}
