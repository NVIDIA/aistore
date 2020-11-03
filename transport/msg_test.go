// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func Example_msg() {
	receive := func(w http.ResponseWriter, msg transport.Msg, err error) {
		fmt.Printf("%s...\n", string(msg.Body[:16]))
	}

	ts := httptest.NewServer(msgmux)
	defer ts.Close()

	trname := "dummy-msg"
	err := transport.HandleMsgStream(trname, receive)
	if err != nil {
		fmt.Println(err)
		return
	}
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.MsgURLPath(trname)
	stream := transport.NewMsgStream(httpclient, url)

	stream.Send(&transport.Msg{Body: []byte(lorem)})
	stream.Send(&transport.Msg{Body: []byte(duis)})
	stream.Send(&transport.Msg{Body: []byte(et)})
	stream.Send(&transport.Msg{Body: []byte(temporibus)})

	stream.Fin()

	// Output:
	// Lorem ipsum dolo...
	// Duis aute irure ...
	// Et harum quidem ...
	// Temporibus autem...
}

func Test_MsgDryRun(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	err := os.Setenv("AIS_STREAM_DRY_RUN", "true")
	defer os.Unsetenv("AIS_STREAM_DRY_RUN")
	tassert.CheckFatal(t, err)
	stream := transport.NewMsgStream(nil, "dry-msg")

	random := newRand(mono.NanoTime())
	size, num, prevsize := int64(0), 0, int64(0)

	for size < cmn.GiB {
		msg := &transport.Msg{Body: make([]byte, random.Intn(256)+1)}
		_, err = random.Read(msg.Body)
		tassert.CheckFatal(t, err)
		stream.Send(msg)
		num++
		size += int64(len(msg.Body))
		if size-prevsize >= cmn.MiB*100 {
			prevsize = size
			tutils.Logf("[dry]: %d MiB\n", size/cmn.MiB)
		}
	}
	stream.Fin()
	stats := stream.GetStats()

	fmt.Printf("[dry]: offset=%d, num=%d(%d)\n", stats.Offset.Load(), stats.Num.Load(), num)
}
