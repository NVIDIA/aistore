// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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

type rxSizeStats struct {
	cos.StatsUpdater
	bytes int64
}

func (s *rxSizeStats) Add(name string, val int64) {
	if name == cos.StreamsInObjSize {
		s.bytes += val
	}
}

func TestRxUnsizedPDUStats(t *testing.T) {
	orig := g.tstats
	stats := &rxSizeStats{StatsUpdater: orig}
	g.tstats = stats
	t.Cleanup(func() { g.tstats = orig })

	it := &iterator{
		handler: &handler{rxObj: func(_ *ObjHdr, r io.Reader, err error) error {
			DrainAndFreeReader(r)
			return err
		}},
		hbuf: make([]byte, 1024),
	}
	hdr := &ObjHdr{ObjAttrs: cmn.ObjAttrs{Size: SizeUnknown}}
	var want int64
	for _, chunks := range [][]string{{"some", " bytes"}, {""}} {
		header := make([]byte, 1024)
		end := insObjHeader(header, hdr, true /*usePDU*/)
		body := bytes.NewBuffer(header[sizeProtoHdr:end])
		for i, chunk := range chunks {
			frame := make([]byte, sizeProtoHdr+len(chunk))
			copy(frame[sizeProtoHdr:], chunk)
			send := &spdu{pdu: pdu{buf: frame, woff: len(frame), last: i == len(chunks)-1}}
			send.insHeader()
			body.Write(frame)
			want += int64(len(chunk))
		}

		it.body = body
		it.pdu = newRecvPDU(it, make([]byte, 64))
		if err := it.rxObj(end - sizeProtoHdr); err != nil {
			t.Fatal(err)
		}
		if stats.bytes != want {
			t.Fatalf("stream.in.size = %d, want %d", stats.bytes, want)
		}
	}
}

func TestRecvPDUObjectEOF(t *testing.T) {
	tests := []struct {
		name string
		body string
		plen int
		last bool
		want error
	}{
		{"final", "abc", 3, true, nil},
		{"empty final", "", 0, true, nil},
		{"short final", "ab", 3, true, io.ErrUnexpectedEOF},
		{"short nonfinal", "ab", 3, false, io.ErrUnexpectedEOF},
		{"missing final", "abc", 3, false, io.ErrUnexpectedEOF},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hdr := &ObjHdr{ObjAttrs: cmn.ObjAttrs{Size: SizeUnknown}}
			header := make([]byte, 1024)
			end := insObjHeader(header, hdr, true)
			frame := make([]byte, sizeProtoHdr+len(test.body))
			copy(frame[sizeProtoHdr:], test.body)
			send := &spdu{pdu: pdu{buf: frame, woff: sizeProtoHdr + test.plen, last: test.last}}
			send.insHeader()
			wire := bytes.NewBuffer(header[sizeProtoHdr:end])
			wire.Write(frame)

			it := &iterator{
				body: iotest.DataErrReader(wire),
				hbuf: make([]byte, 1024),
				handler: &handler{rxObj: func(hdr *ObjHdr, r io.Reader, _ error) error {
					defer FreeRecv(r)
					n, err := io.Copy(io.Discard, r)
					if n != int64(len(test.body)) || !errors.Is(err, test.want) {
						t.Errorf("read = (%d, %v), want (%d, %v)", n, err, len(test.body), test.want)
					}
					if err == nil && hdr.ObjSize() != n {
						t.Errorf("final size = %d, want %d", hdr.ObjSize(), n)
					}
					return nil // simulate a callback discarding a drain error
				}},
			}
			it.pdu = newRecvPDU(it, make([]byte, 64))
			if err := it.rxObj(end - sizeProtoHdr); !errors.Is(err, test.want) {
				t.Fatalf("rxObj() error = %v, want %v", err, test.want)
			}
		})
	}
}
