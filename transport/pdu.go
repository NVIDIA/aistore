// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"

	"github.com/NVIDIA/aistore/cmn"
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
		flags uint64
		body  io.Reader
		plen  int
		pdu
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
	debug.Assert(len(buf) >= cmn.KiB && len(buf) <= MaxSizePDU)
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

func newRecvPDU(body io.Reader, buf []byte) (p *rpdu) {
	p = &rpdu{body: body, pdu: pdu{buf: buf}}
	p.reset()
	return
}

func (pdu *rpdu) readHdr(loghdr string) (err error) {
	var n int
	debug.Assert(pdu.woff == 0)
	n, err = pdu.body.Read(pdu.buf[:sizeProtoHdr])
	if n < sizeProtoHdr {
		if err == nil {
			err = fmt.Errorf("sbrk %s: failed to receive pdu hdr (n=%d)", loghdr, n)
		}
		return
	}
	pdu.plen, pdu.flags, err = extProtoHdr(pdu.buf, loghdr)
	pdu.last = pdu.flags&lastPDU != 0
	pdu.woff = sizeProtoHdr
	debug.Func(func() {
		debug.Infof("%d(%s)", pdu.plen, fl2s(pdu.flags))
		debug.Assert(pdu.flags&pduFlag != 0)
		debug.Assert(pdu.plen <= MaxSizePDU)
		debug.Assert(pdu.plen > 0 || (pdu.plen == 0 && pdu.flags&lastPDU != 0))
	})
	return
}

func (pdu *rpdu) reset() {
	pdu.roff, pdu.woff = sizeProtoHdr, 0
	pdu.done, pdu.last = false, false
}

func (pdu *rpdu) readFrom() (n int, err error) {
	n, err = pdu.body.Read(pdu.buf[pdu.woff : sizeProtoHdr+pdu.plen]) // NOTE: MaxSizePDU
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
