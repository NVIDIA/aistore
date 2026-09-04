// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"errors"
	"io"
	"testing"
)

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
	objs[0].SetPrc(len(objs))
	*objs[1], *objs[2] = *objs[0], *objs[0]

	s := &Stream{}
	s.doCmpl(objs[0], expected)
	s.doCmpl(objs[1], errors.New("another destination failed"))
	s.doCmpl(objs[2], nil)

	if called != 1 {
		t.Fatalf("completion count %d, expected 1", called)
	}
	if !errors.Is(gotErr, expected) {
		t.Fatalf("completion error %v, expected %v", gotErr, expected)
	}
}
