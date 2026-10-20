// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/binpack"
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
// ObjHdr.Opcode enums: 3 groups
//

// group 1: application-level opcodes (sentinels)
const (
	OpcDone = iota + 27182
	OpcAbort
	OpcRequest
	OpcResponse
)

// group 2: transport/bundle (data mover's) opcodes
const (
	OpcReconnect = iota + 46351
)

// group 3: transport's internal range of 16 `Obj.Hdr.Opcode` values
const (
	opcFin = iota + math.MaxUint16 - 16
	opcIdleTick
)

func ReservedOpcode(opc int) bool { return opc >= opcFin }

// control-plane opcodes start at`OpcDone`
func (hdr *ObjHdr) IsControl() bool { return hdr.Opcode >= OpcDone }

// object-header decode failures (sentinels: use errors.Is)
var (
	// a field's declared length runs past the end of the header, or the
	// header ends mid-field
	ErrHdrMalformed = errors.New("malformed object header")

	// every field decoded, but the consumed offset != the declared hlen -
	// implies sender/receiver disagreement on the encoding, not truncation
	ErrHdrLength = errors.New("object header length mismatch")
)

//
// proto header: serialization
//

func insObjHeader(hbuf []byte, hdr *ObjHdr, usePDU bool) (off int) {
	debug.AssertFunc(func() bool { return usePDU || !hdr.IsUnsized() })
	off = sizeProtoHdr
	off = binpack.InsString(off, hbuf, hdr.SID)
	off = binpack.InsUint16(off, hbuf, hdr.Opcode)
	off = binpack.InsString(off, hbuf, hdr.Bck.Name)
	off = binpack.InsString(off, hbuf, hdr.Bck.Provider)
	off = binpack.InsString(off, hbuf, hdr.Bck.Ns.Name)
	off = binpack.InsString(off, hbuf, hdr.Bck.Ns.UUID)
	off = binpack.InsString(off, hbuf, hdr.ObjName)
	off = binpack.InsBytes(off, hbuf, hdr.Opaque)
	off = binpack.InsString(off, hbuf, hdr.Demux)
	off = hdr.ObjAttrs.InsAttrs(off, hbuf)
	word1 := uint64(off - sizeProtoHdr)
	if usePDU {
		word1 |= pduStreamFl
	}
	binpack.InsUint64(0, hbuf, word1)
	checksum := xoshiro256.Hash(word1)
	binpack.InsUint64(cos.SizeofI64, hbuf, checksum)
	return
}

func (pdu *spdu) insHeader() {
	buf, plen := pdu.buf, pdu.plength()
	word1 := uint64(plen) | pduFl
	if pdu.last {
		word1 |= pduLastFl
	}
	binpack.InsUint64(0, buf, word1)
	checksum := xoshiro256.Hash(word1)
	binpack.InsUint64(cos.SizeofI64, buf, checksum)
	pdu.done = true
}

//
// proto header: deserialization
//

// zero-value interning source; keeps `prev` nil-free at every call site.
// MUST remain read-only.
var noPrevHdr ObjHdr

func (it *iterator) extProtoHdr(hbuf []byte) (hlen int, flags uint64, err error) {
	d := binpack.NewDecoder(hbuf)
	word1 := d.Uint64()
	checksum := d.Uint64()
	if d.Bad() {
		return 0, 0, it.newErr(io.ErrUnexpectedEOF, sbrProtoHdr, "n="+strconv.Itoa(len(hbuf)))
	}

	hlen = int(word1 & ^allFlags)
	flags = word1 & allFlags
	//
	// validate checksum
	//
	chc := xoshiro256.Hash(word1)
	if checksum != chc {
		err = it.newErr(nil, sbrHdrChecksum, fmt.Sprintf("%x != %x (hlen=%d)", checksum, chc, hlen))
	}
	return
}

func ExtObjHeader(body []byte, hlen int) (ObjHdr, error) {
	return extObjHeader(body, hlen, &noPrevHdr)
}

// extObjHeader is ExtObjHeader with an interning source - `prev` being the
// last header successfully decoded on this same stream (see iterator.prev).
func extObjHeader(body []byte, hlen int, prev *ObjHdr) (hdr ObjHdr, err error) {
	if hlen <= 0 || hlen > len(body) {
		return hdr, fmt.Errorf("%w: declared %d, buffer %d", ErrHdrMalformed, hlen, len(body))
	}

	d := binpack.NewDecoder(body[:hlen])
	hdr.SID = d.StrCached(prev.SID)
	hdr.Opcode = d.Uint16()
	hdr.Bck.Name = d.StrCached(prev.Bck.Name)
	hdr.Bck.Provider = d.StrCached(prev.Bck.Provider)
	hdr.Bck.Ns.Name = d.StrCached(prev.Bck.Ns.Name)
	hdr.Bck.Ns.UUID = d.StrCached(prev.Bck.Ns.UUID)
	hdr.ObjName = d.Str()
	hdr.Opaque = d.Bytes() // NOTE: aliases the receive buffer - see ObjHdr.Opaque
	hdr.Demux = d.StrCached(prev.Demux)
	hdr.ObjAttrs.ExtAttrs(&d, &prev.ObjAttrs)

	if d.Bad() {
		return hdr, fmt.Errorf("%w: at offset %d: %w", ErrHdrMalformed, d.Off(), io.ErrUnexpectedEOF)
	}
	if d.Off() != hlen {
		return hdr, fmt.Errorf("%w: decoded %d of %d bytes", ErrHdrLength, d.Off(), hlen)
	}
	return hdr, nil
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

// initialize shared send-completion state for a multi-destination (bundled) send
func (obj *Obj) SetCmpl(n int) {
	debug.Assert(n > 1, "expecting multiple destinations, got ", n)
	obj.cmpl = &sendCmpl{}
	obj.cmpl.refs.Store(int64(n))
}

func (hdr *ObjHdr) Cname() string { return hdr.Bck.Cname(hdr.ObjName) } // see also: lom.Cname()

func (hdr *ObjHdr) IsUnsized() bool    { return hdr.ObjAttrs.Size == SizeUnknown }
func (hdr *ObjHdr) IsHeaderOnly() bool { return hdr.ObjAttrs.Size == 0 }
func (hdr *ObjHdr) ObjSize() int64     { return hdr.ObjAttrs.Size }

// reserved opcodes
func (hdr *ObjHdr) isFin() bool      { return hdr.Opcode == opcFin }
func (hdr *ObjHdr) isIdleTick() bool { return hdr.Opcode == opcIdleTick }
