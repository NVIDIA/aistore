// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type closeErrReader struct {
	io.Reader
	closes int
}

func (r *closeErrReader) Close() error {
	r.closes++
	return io.ErrUnexpectedEOF
}

func TestDoCmplRetainsNonFinalError(t *testing.T) {
	expected := errors.New("destination failed")
	var (
		called int
		gotErr error
	)

	objs := [3]*Obj{AllocSend(), AllocSend(), AllocSend()}
	objs[0].SentCB = func(_ *ObjHdr, _ io.ReadCloser, _ any, err error) {
		called++
		gotErr = err
	}
	objs[0].SetCmpl(len(objs))
	*objs[1], *objs[2] = *objs[0], *objs[0]
	reader := &closeErrReader{}
	objs[0].Reader = reader

	s := &Stream{}
	s.doCmpl(objs[0], expected)
	s.doCmpl(objs[1], errors.New("another destination failed"))
	s.doCmpl(objs[2], nil)

	if reader.closes != 1 {
		t.Fatalf("reader closed %d times, expected 1", reader.closes)
	}
	if called != 1 {
		t.Fatalf("completion count %d, expected 1", called)
	}
	if !errors.Is(gotErr, expected) {
		t.Fatalf("completion error %v, expected %v", gotErr, expected)
	}
}

// termination is visible while the sender is still posting its final completion
func TestCollectorWaitsForRemove(t *testing.T) {
	s := &Stream{}
	s.streamer = s
	eos := errEndOfStream
	s.term.err.Store(&eos)
	s.time.ticks = 1
	s.workCh = make(chan *Obj)
	s.cmplCh = make(chan cmpl)
	s.maxhdr, _ = g.mm.Alloc()
	gc := &collector{streams: map[int64]*base{s.sessID: &s.base}, ticker: time.NewTicker(time.Hour)}
	defer gc.ticker.Stop()

	gc.do()
	if len(gc.streams) != 1 {
		t.Fatal("reaped stream before remove")
	}
	gc.apply(&ctrl{s: &s.base})
	gc.do()
	if len(gc.streams) != 0 {
		t.Fatal("did not reap stream after remove")
	}
}

func TestFinCanceledDuringCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	peerErr := errors.New("peer rejected FIN")
	for _, tc := range []struct {
		name                    string
		term, pending, finished error
	}{
		{"peer-error", peerErr, peerErr, peerErr},
		{"end-of-stream", errEndOfStream, context.Canceled, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Stream{}
			s.stopCh.Init()
			s.finCh = make(chan struct{})
			term := tc.term
			s.term.err.Store(&term)
			if err := s.FinContext(ctx); err != tc.pending {
				t.Fatalf("during cleanup: got %v, want %v", err, tc.pending)
			}
			close(s.finCh)
			if err := s.FinContext(ctx); err != tc.finished {
				t.Fatalf("after cleanup: got %v, want %v", err, tc.finished)
			}
		})
	}
}

func TestSendPDUObjectEOF(t *testing.T) {
	sourceErr := errors.New("source failed")
	tests := []struct {
		name string
		body io.Reader
		size int64
		want error
	}{
		{"unsized", strings.NewReader("abc"), SizeUnknown, nil},
		{"empty unsized", strings.NewReader(""), SizeUnknown, nil},
		{"sized", iotest.DataErrReader(strings.NewReader("abc")), 3, nil},
		{"short sized", strings.NewReader("abc"), 4, io.ErrUnexpectedEOF},
		{"source error", io.MultiReader(strings.NewReader("abc"), iotest.ErrReader(sourceErr)), SizeUnknown, sourceErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Stream{
				pdu: newSendPDU(make([]byte, 1024)),
				sendoff: sendoff{ins: inPDU, obj: Obj{
					Hdr:    ObjHdr{ObjAttrs: cmn.ObjAttrs{Size: test.size}},
					Reader: io.NopCloser(test.body),
				}},
			}
			// read just one byte so the frame stays available for inspection
			n, err := s.Read(make([]byte, 1))
			if test.want == nil {
				if n != 1 || err != nil || !s.pdu.last {
					t.Fatalf("Read() = (%d, %v), last = %t, want one byte of the final pdu", n, err, s.pdu.last)
				}
			} else if n != 0 || !errors.Is(err, test.want) {
				t.Fatalf("Read() = (%d, %v), want (0, %v)", n, err, test.want)
			}
		})
	}
}
