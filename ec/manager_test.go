// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ec //nolint:testpackage // Tests unexported EC receive handlers.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/transport"
)

func TestECStreamAfterBucketDeletion(t *testing.T) {
	previousTarget := core.T
	t.Cleanup(func() { core.T = previousTarget })
	collector := transport.Init(mock.NewStatsTracker(), nil, nil, false)
	done := make(chan struct{})
	go func() { collector.Run(); close(done) }()
	t.Cleanup(func() { collector.Stop(nil); <-done })
	srv := httptest.NewServer(http.HandlerFunc(transport.RxAnyStream))
	t.Cleanup(srv.Close)

	mgr := &Manager{}
	for _, tc := range []struct {
		name    string
		recv    transport.RecvObj
		opcode  int
		payload string
	}{
		{ReqStreamName, mgr.recvRequest, reqDel, ""},
		{RespStreamName, mgr.recvResponse, reqPut, "slice payload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleted := meta.NewBck("deleted", apc.AIS, cmn.NsGlobal, &cmn.Bprops{EC: cmn.ECConf{Enabled: true}})
			live := meta.NewBck("live", apc.AIS, cmn.NsGlobal, &cmn.Bprops{EC: cmn.ECConf{Enabled: true}})
			bmd := mock.NewBaseBownerMock(deleted, live)
			mock.NewTarget(bmd)
			received := make(chan string, 1)
			err := transport.Handle(tc.name, func(hdr *transport.ObjHdr, r io.Reader, err error) error {
				if hdr.Bck.Name == deleted.Name {
					// The first message has arrived, but its bucket is deleted before EC handles it.
					tassert.Errorf(t, bmd.Del(deleted), "bucket was already missing")
					return tc.recv(hdr, r, err)
				}
				// Check the next bucket's message without starting an EC job.
				defer transport.DrainAndFreeReader(r)
				if err != nil {
					return err
				}
				tassert.CheckError(t, meta.CloneBck(&hdr.Bck).Init(bmd))
				payload, err := io.ReadAll(r)
				received <- string(payload)
				return err
			})
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, transport.Unhandle(tc.name)) })
			stream := transport.NewObjStream(transport.NewIntraDataClient(), srv.URL+transport.ObjURLPath(tc.name), "peer", &transport.Extra{Config: cmn.GCO.Get()})
			t.Cleanup(stream.Stop)
			for _, bck := range []*meta.Bck{deleted, live} {
				tassert.CheckFatal(t, stream.Send(&transport.Obj{
					Hdr: transport.ObjHdr{
						Bck: *bck.Bucket(), ObjName: "object", Opcode: tc.opcode,
						Opaque: newIntraReq(tc.opcode, nil, bck).NewPack(nil), ObjAttrs: cmn.ObjAttrs{Size: int64(len(tc.payload))},
					},
					Reader: io.NopCloser(strings.NewReader(tc.payload)),
				}))
			}
			stream.Fin()
			tassert.Fatalf(t, len(received) == 1, "live bucket message was not received")
			payload := <-received
			tassert.Errorf(t, payload == tc.payload, "received %q, want %q", payload, tc.payload)
		})
	}
}
