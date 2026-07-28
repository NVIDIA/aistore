// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"bytes"
	"testing"
)

func TestRecvPDUPayloadLength(t *testing.T) {
	tests := []struct {
		name    string
		bufSize int
		plen    int
		wantErr bool
	}{
		{"maximum valid", maxSizePDU, maxSizePDU - sizeProtoHdr, false},
		{"header overflow", maxSizePDU, maxSizePDU - sizeProtoHdr + 1, true},
		{"receive buffer overflow", sizeProtoHdr + 1, 2, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hdr := make([]byte, sizeProtoHdr)
			send := &spdu{pdu: pdu{buf: hdr, woff: sizeProtoHdr + test.plen}}
			send.insHeader()

			recv := newRecvPDU(&iterator{body: bytes.NewReader(hdr)}, make([]byte, test.bufSize))
			err := recv.readHdr()
			if (err != nil) != test.wantErr {
				t.Fatalf("readHdr() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}
