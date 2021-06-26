// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"encoding/binary"
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xoshiro256"
)

// proto header
const (
	// flags
	msgFlag  = uint64(1 << (63 - iota)) // message vs object demux
	pduFlag                             // PDU
	lastPDU                             // last PDU in a given obj/msg
	firstPDU                            // first --/--

	allFlags = msgFlag | pduFlag | lastPDU | firstPDU // NOTE: update when adding flags

	// all 3 headers
	sizeProtoHdr = cos.SizeofI64 * 2
)

////////////////////////////////
// proto header serialization //
////////////////////////////////

func insObjHeader(hbuf []byte, hdr *ObjHdr, usePDU bool) (off int) {
	var flags uint64
	if usePDU {
		flags = firstPDU
	} else {
		debug.Assert(!hdr.IsUnsized())
	}
	off = sizeProtoHdr
	off = insString(off, hbuf, hdr.Bck.Name)
	off = insString(off, hbuf, hdr.ObjName)
	off = insString(off, hbuf, hdr.Bck.Provider)
	off = insString(off, hbuf, hdr.Bck.Ns.Name)
	off = insString(off, hbuf, hdr.Bck.Ns.UUID)
	off = insByte(off, hbuf, hdr.Opaque)
	off = insAttrs(off, hbuf, &hdr.ObjAttrs)
	word1 := uint64(off-sizeProtoHdr) | flags
	insUint64(0, hbuf, word1)
	checksum := xoshiro256.Hash(word1)
	insUint64(cos.SizeofI64, hbuf, checksum)
	return
}

func insMsg(hbuf []byte, msg *Msg) (off int) {
	off = sizeProtoHdr
	off = insInt64(off, hbuf, msg.Flags)
	off = insByte(off, hbuf, msg.Body)
	word1 := uint64(off-sizeProtoHdr) | msgFlag
	insUint64(0, hbuf, word1)
	checksum := xoshiro256.Hash(word1)
	insUint64(cos.SizeofI64, hbuf, checksum)
	return
}

func (pdu *spdu) insHeader() {
	buf, plen := pdu.buf, pdu.plength()
	word1 := uint64(plen) | pduFlag
	if pdu.last {
		word1 |= lastPDU
	}
	insUint64(0, buf, word1)
	checksum := xoshiro256.Hash(word1)
	insUint64(cos.SizeofI64, buf, checksum)
	pdu.done = true
}

func insString(off int, to []byte, str string) int {
	return insByte(off, to, []byte(str))
}

func insByte(off int, to, b []byte) int {
	l := len(b)
	binary.BigEndian.PutUint64(to[off:], uint64(l))
	off += cos.SizeofI64
	n := copy(to[off:], b)
	cos.Assert(n == l)
	return off + l
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
	off = insString(off, to, attr.Checksum().Type())
	off = insString(off, to, attr.Checksum().Value())
	off = insString(off, to, attr.Ver)
	return off
}

//////////////////////////////////
// proto header deserialization //
//////////////////////////////////

func extProtoHdr(hbuf []byte, loghdr string) (hlen int, flags uint64, err error) {
	off, word1 := extUint64(0, hbuf)
	hlen = int(word1 & ^allFlags)
	flags = word1 & allFlags
	// validate checksum
	_, checksum := extUint64(0, hbuf[off:])
	chc := xoshiro256.Hash(word1)
	if checksum != chc {
		err = fmt.Errorf("sbrk %s: bad checksum %x != %x (hlen=%d)", loghdr, checksum, chc, hlen)
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
	off += cos.SizeofI64
	return off + l, from[off : off+l]
}

func extInt64(off int, from []byte) (int, int64) {
	off, val := extUint64(off, from)
	return off, int64(val)
}

func extUint64(off int, from []byte) (int, uint64) {
	size := binary.BigEndian.Uint64(from[off:])
	off += cos.SizeofI64
	return off, size
}

func extAttrs(off int, from []byte) (n int, attr cmn.ObjAttrs) {
	var cksumTyp, cksumVal string
	off, attr.Size = extInt64(off, from)
	off, attr.Atime = extInt64(off, from)
	off, cksumTyp = extString(off, from)
	off, cksumVal = extString(off, from)
	attr.SetCksum(cksumTyp, cksumVal)
	off, attr.Ver = extString(off, from)
	return off, attr
}
