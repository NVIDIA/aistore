// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	pdu struct {
		buf  []byte
		roff int
		woff int
		done bool
		last bool
	}
	spdu struct {
		pdu
	}
	rpdu struct {
		parent *iterator
		pdu
		flags uint64
		plen  int
	}
)

/////////
// pdu //
/////////

func (pdu *pdu) plength() int { return pdu.woff - sizeProtoHdr } // just the payload
func (pdu *pdu) slength() int { return pdu.roff - sizeProtoHdr } // payload transmitted/received so far
func (pdu *pdu) rlength() int { return pdu.woff - pdu.roff }     // not yet sent/received part of the PDU

func (pdu *pdu) read(b []byte) (n int) {
	n = copy(b, pdu.buf[pdu.roff:pdu.woff])
	pdu.roff += n
	return
}

func (pdu *pdu) free(mm *memsys.MMSA) {
	if pdu.buf != nil {
		mm.Free(pdu.buf)
	}
}

//////////
// spdu //
//////////

func newSendPDU(buf []byte) (p *spdu) {
	debug.Assert(len(buf) >= cos.KiB && len(buf) <= maxSizePDU)
	p = &spdu{pdu{buf: buf}}
	p.reset()
	return
}

func (pdu *spdu) readFrom(sendoff *sendoff) (err error) {
	var (
		obj = &sendoff.obj
		b   = pdu.buf[pdu.woff:]
		n   int
	)
	n, err = obj.Reader.Read(b)
	pdu.woff += n
	pdu.done = pdu.woff == len(pdu.buf)
	if err != nil {
		pdu.done, pdu.last = true, true
	} else if !obj.IsUnsized() && sendoff.off+int64(pdu.plength()) >= obj.Hdr.ObjAttrs.Size {
		pdu.done, pdu.last = true, true
	}
	return
}

func (pdu *spdu) reset() {
	pdu.roff, pdu.woff = 0, sizeProtoHdr
	pdu.done, pdu.last = false, false
}

//////////
// rpdu //
//////////

func newRecvPDU(it *iterator, buf []byte) (p *rpdu) {
	p = &rpdu{parent: it, pdu: pdu{buf: buf}}
	p.reset()
	return
}

func (pdu *rpdu) readHdr() error {
	debug.Assert(pdu.woff == 0)
	const (
		fmterr = "(plen=%d, flags=%s)"
	)
	n, err := pdu.parent.body.Read(pdu.buf[:sizeProtoHdr])
	if n < sizeProtoHdr {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return pdu.parent.newErr(err, sbrPDUHdrTooShort, fmt.Sprintf("n=%d", n))
	}
	// extract/validate
	pdu.plen, pdu.flags, err = pdu.parent.extProtoHdr(pdu.buf)
	if err != nil {
		return err
	}
	if pdu.flags&pduFl == 0 || pdu.plen > maxSizePDU || pdu.plen < 0 {
		detail := fmt.Sprintf(fmterr, pdu.plen, fl2s(pdu.flags))
		return pdu.parent.newErr(nil, sbrPDUHdrInvalid, detail)
	}

	pdu.woff = sizeProtoHdr
	pdu.last = pdu.flags&pduLastFl != 0
	debug.Assertf(pdu.plen > 0 || (pdu.plen == 0 && pdu.last), fmterr, pdu.plen, fl2s(pdu.flags))
	return nil
}

func (pdu *rpdu) reset() {
	pdu.roff, pdu.woff = sizeProtoHdr, 0
	pdu.done, pdu.last = false, false
}

func (pdu *rpdu) readFrom() (n int, err error) {
	n, err = pdu.parent.body.Read(pdu.buf[pdu.woff : sizeProtoHdr+pdu.plen]) // NOTE: maxSizePDU
	pdu.woff += n
	pdu.done = pdu.plength() == pdu.plen
	if err != nil {
		pdu.done, pdu.last = true, true
	}
	return
}

//
// misc
//

func fl2s(flags uint64) (s string) {
	switch {
	case flags&msgFl == 0 && flags&pduFl == 0:
		s += "[obj]"
	case flags&msgFl != 0:
		s += "[msg]"
	case flags&pduFl != 0:
		s += "[pdu]"
	}
	if flags&pduStreamFl != 0 {
		s += "[pdu-stream]"
	}
	if flags&pduLastFl != 0 {
		s += "[lst]"
	}
	return
}
