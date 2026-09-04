// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/NVIDIA/aistore/cmn"
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

// zero-value interning source; keeps `prev` nil-free at every call site.
// MUST remain read-only.
var noPrevHdr ObjHdr

type hdrDecoder struct {
	b    []byte
	prev *ObjHdr // previous header on this same stream
	off  int
	bad  bool
}

func (d *hdrDecoder) take(n int) []byte {
	if d.bad {
		return nil
	}
	if n < 0 || d.off > len(d.b) || n > len(d.b)-d.off {
		d.bad = true
		return nil
	}
	b := d.b[d.off : d.off+n]
	d.off += n
	return b
}

func (d *hdrDecoder) bytes() []byte {
	b := d.take(cos.SizeofI16)
	if d.bad {
		return nil
	}
	return d.take(int(binary.BigEndian.Uint16(b)))
}

func (d *hdrDecoder) str() string {
	return string(d.bytes())
}

func (d *hdrDecoder) strCached(prev string) string {
	b := d.bytes()
	if d.bad {
		return ""
	}
	if len(b) == len(prev) && (len(b) == 0 || bytes.Equal(b, cos.UnsafeB(prev))) {
		return prev
	}
	return string(b)
}

func (d *hdrDecoder) uint16() int {
	b := d.take(cos.SizeofI16)
	if d.bad {
		return 0
	}
	return int(binary.BigEndian.Uint16(b))
}

func (d *hdrDecoder) uint64() uint64 {
	b := d.take(cos.SizeofI64)
	if d.bad {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (d *hdrDecoder) int64() int64 {
	return int64(d.uint64())
}

func (d *hdrDecoder) attrs(attr *cmn.ObjAttrs) {
	pa := &d.prev.ObjAttrs
	var prevCksumTyp string
	if cksum := pa.Checksum(); cksum != nil {
		prevCksumTyp = cksum.Ty()
	}

	attr.Size = d.int64()
	attr.Atime = d.int64()
	cksumTyp := d.strCached(prevCksumTyp)
	cksumVal := d.str()
	version := d.strCached(pa.Version())
	if d.bad {
		return
	}

	attr.SetCksum(cksumTyp, cksumVal)
	attr.SetVersion(version)
	for {
		// custom-MD keys are never cached
		k := d.str()
		if d.bad || k == "" {
			return
		}
		v := d.str()
		if d.bad {
			return
		}
		attr.SetCustomKey(k, v)
	}
}

func (it *iterator) extProtoHdr(hbuf []byte) (hlen int, flags uint64, err error) {
	d := hdrDecoder{b: hbuf, prev: &noPrevHdr}
	word1 := d.uint64()
	checksum := d.uint64()
	if d.bad {
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

	d := hdrDecoder{b: body[:hlen], prev: prev}
	hdr.SID = d.strCached(prev.SID)
	hdr.Opcode = d.uint16()
	hdr.Bck.Name = d.strCached(prev.Bck.Name)
	hdr.Bck.Provider = d.strCached(prev.Bck.Provider)
	hdr.Bck.Ns.Name = d.strCached(prev.Bck.Ns.Name)
	hdr.Bck.Ns.UUID = d.strCached(prev.Bck.Ns.UUID)
	hdr.ObjName = d.str()
	hdr.Opaque = d.bytes() // NOTE: aliases the receive buffer - see ObjHdr.Opaque
	hdr.Demux = d.strCached(prev.Demux)
	d.attrs(&hdr.ObjAttrs)

	if d.bad {
		return hdr, fmt.Errorf("%w: at offset %d: %w", ErrHdrMalformed, d.off, io.ErrUnexpectedEOF)
	}
	if d.off != hlen {
		return hdr, fmt.Errorf("%w: decoded %d of %d bytes", ErrHdrLength, d.off, hlen)
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
