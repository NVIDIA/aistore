// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
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

type (
	// Tx
	ErrStreamTerm struct {
		err    error
		loghdr string
		dst    string // destination node ID
		reason string
		detail string
	}
	// Rx
	ErrSBR struct {
		err    error
		loghdr string
		sid    string // sender node ID
		code   string
		detail string
	}
)

///////////////////
// ErrStreamTerm //
///////////////////

func (e *ErrStreamTerm) Error() string {
	return fmt.Sprintf("%s terminated [dst: %s, reason: %s, detail: %s, err: %v]", e.loghdr, e.dst, e.reason, e.detail, e.err)
}

func (e *ErrStreamTerm) Unwrap() error { return e.err }

// func (e *ErrStreamTerm) DestID() string { return e.dst } // uncomment if needed

func IsErrStreamTerm(err error) bool {
	_, ok := err.(*ErrStreamTerm)
	return ok
}

////////////
// ErrSBR //
////////////

func (e *ErrSBR) Error() string {
	if e.err == nil {
		return fmt.Sprintf("%s %s: [sender: %s, detail: %s]", e.loghdr, e.code, e.sid, e.detail)
	}
	return fmt.Sprintf("%s %s: [sender: %s, detail: %s, err: %v]", e.loghdr, e.code, e.sid, e.detail, e.err)
}

func (e *ErrSBR) Unwrap() error { return e.err }

// func (e *ErrSBR) SID() string   { return e.sid } // uncomment if needed

func IsErrSBR(err error) bool {
	_, ok := err.(*ErrSBR)
	return ok
}
