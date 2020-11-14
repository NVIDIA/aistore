// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

type pdu struct {
	buf  []byte
	roff int
	woff int
}

func newPDU(buf []byte) (p *pdu) { return &pdu{buf: buf} }

func (pdu *pdu) read(b []byte) (n int, err error) {
	n = copy(b, pdu.buf[pdu.roff:pdu.woff])
	pdu.roff += n
	return
}

func (pdu *pdu) next(obj *Obj) (err error) {
	pdu.roff, pdu.woff = sizePDUHdr, 0
	pdu.woff, err = obj.Reader.Read(pdu.buf[pdu.roff:])
	insPDUHeader(pdu.buf, pdu.rem(), err)
	return
}

func (pdu *pdu) rem() int { return pdu.woff - pdu.roff }
