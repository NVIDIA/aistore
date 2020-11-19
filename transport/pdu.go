// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"github.com/NVIDIA/aistore/cmn"
)

type pdu struct {
	buf  []byte
	roff int
	woff int
	done bool
	last bool
}

func newPDU(buf []byte) (p *pdu) {
	cmn.Assert(len(buf) >= cmn.KiB && len(buf) <= MaxSizePDU)
	p = &pdu{buf: buf}
	p.reset()
	return
}

func (pdu *pdu) read(b []byte) (n int) {
	n = copy(b, pdu.buf[pdu.roff:pdu.woff])
	pdu.roff += n
	return
}

func (pdu *pdu) readFrom(sendoff *sendoff) (err error) {
	var (
		obj = &sendoff.obj
		b   = pdu.buf[pdu.woff:]
		n   int
	)
	n, err = obj.Reader.Read(b)
	pdu.woff += n
	pdu.done = (pdu.woff == len(pdu.buf))
	if err != nil {
		pdu.done, pdu.last = true, true
	} else if !obj.IsUnsized() && sendoff.off+int64(pdu.plength()) >= obj.Hdr.ObjAttrs.Size {
		pdu.done, pdu.last = true, true
	}
	return
}

func (pdu *pdu) plength() int { return pdu.woff - sizeProtoHdr } // just the payload
func (pdu *pdu) slength() int { return pdu.roff - sizeProtoHdr } // payload transmitted so far
func (pdu *pdu) rlength() int { return pdu.woff - pdu.roff }     // not yet transmitted part of the PDU (including header)

func (pdu *pdu) reset() {
	pdu.roff, pdu.woff = 0, sizeProtoHdr
	pdu.done, pdu.last = false, false
}

//
// misc
//

func fl2s(flags uint64) (s string) {
	if flags&msgFlag == 0 && flags&pduFlag == 0 {
		s += "[obj]"
	} else if flags&msgFlag != 0 {
		s += "[msg]"
	} else if flags&pduFlag != 0 {
		s += "[pdu]"
	}
	if flags&firstPDU != 0 {
		s += "[frs]"
	}
	if flags&lastPDU != 0 {
		s += "[lst]"
	}
	return
}
