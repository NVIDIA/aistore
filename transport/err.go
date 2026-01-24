// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

// ErrSBR codes (receive-side stream breakage)
const (
	sbrProtoHdr        = "sbr_proto_hdr"      // failed to receive protocol header
	sbrProtoHdrTooLong = "sbr_proto_hdr_long" // header exceeds max size
	sbrHdrChecksum     = "sbr_hdr_checksum"   // bad header checksum
	sbrObjHdrTooShort  = "sbr_obj_hdr_short"  // object header too short
	sbrObjData         = "sbr_obj_data"       // object data read failure
	sbrObjDataEOF      = "sbr_obj_data_eof"   // premature EOF reading object
	sbrObjDataSize     = "sbr_obj_data_size"  // size mismatch
	// PDU
	sbrPDUHdrTooShort = "sbr_pdu_hdr_short"   // PDU header too short
	sbrPDUHdrInvalid  = "sbr_pdu_hdr_invalid" // invalid PDU header
	sbrPDUData        = "sbr_pdu_data"        // PDU data read failure
	sbrPDUDataSize    = "sbr_pdu_data_size"   // PDU size mismatch
)

// (do not wrap %w these errors; use Unwrap)
type (
	// Tx
	errStreamTerm struct {
		err    error
		loghdr string
		dst    string // destination node ID // TODO: needed?
		reason string
		ctx    string
	}
	// Rx
	ErrSBR struct {
		err    error
		loghdr string
		sid    string // sender node ID
		code   string
		ctx    string
		ts     int64
	}
)

///////////////////
// errStreamTerm //
///////////////////

func (e *errStreamTerm) Error() string {
	debug.Assert(e.err != nil)
	debug.Assert(!cos.IsTypedNil(e.err))
	debug.Assert(strings.Contains(e.loghdr, e.dst), e.loghdr, " vs ", e.dst)

	var sb cos.SB
	sb.Init(256)
	sb.WriteString(e.loghdr)
	sb.WriteString(" terminated [reason: '")
	sb.WriteString(e.reason)
	sb.WriteString("' ")
	if e.ctx != "" {
		sb.WriteString("ctx: ")
		sb.WriteString(e.ctx)
		sb.WriteUint8(' ')
	}
	sb.WriteString("err: ")
	sb.WriteString(e.err.Error())
	sb.WriteUint8(']')
	return sb.String()
}

func (e *errStreamTerm) Unwrap() error { return e.err }

////////////
// ErrSBR //
////////////

func (e *ErrSBR) Error() string {
	debug.Assert(strings.Contains(e.loghdr, e.sid), e.loghdr, " vs ", e.sid)
	var sb cos.SB
	sb.Init(256)
	sb.WriteString(e.loghdr)
	sb.WriteUint8(' ')
	sb.WriteString(e.code)
	sb.WriteString(":[")
	if e.ctx != "" {
		sb.WriteString("ctx: ")
		sb.WriteString(e.ctx)
	}
	if e.err != nil {
		if e.ctx != "" {
			sb.WriteUint8(' ')
		}
		sb.WriteString("err: ")
		sb.WriteString(e.err.Error())
	}
	sb.WriteUint8(']')
	return sb.String()
}

func (e *ErrSBR) Unwrap() error        { return e.err }
func (e *ErrSBR) SID() string          { return e.sid }
func (e *ErrSBR) Since() time.Duration { return mono.Since(e.ts) }

func IsErrSBR(err error) bool {
	_, ok := err.(*ErrSBR)
	return ok
}

func AsErrSBR(err error) *ErrSBR {
	if e, ok := err.(*ErrSBR); ok {
		return e
	}
	return nil
}
