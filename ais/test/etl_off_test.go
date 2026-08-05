//go:build !etl

// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestETLNotBuiltInit(t *testing.T) {
	bp := tools.BaseAPIParams(tools.RandomProxyURL(t))
	msg := &etl.ETLSpecMsg{
		InitMsgBase: etl.InitMsgBase{EtlName: "not-built", CommTypeX: etl.Hpush},
		Runtime:     etl.RuntimeSpec{Image: "busybox:latest"},
	}

	for range 2 {
		_, err := api.ETLInit(bp, msg)
		assertETLNotBuilt(t, err)
	}
}

func TestETLNotBuiltInlineObject(t *testing.T) {
	m := ioContext{t: t, num: 1, fileSize: cos.KiB, fixedSize: true}
	m.init(false /*cleanup*/)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	bp := tools.BaseAPIParams(m.proxyURL)
	buf := bytes.NewBuffer(nil)
	_, err := api.ETLObject(bp, &api.ETL{ETLName: "not-built"}, m.bck, m.objNames[0], buf)
	assertETLNotBuilt(t, err)
	tassert.Fatalf(t, buf.Len() == 0, "expected no transformed output, got %d bytes", buf.Len())

	_, err = api.GetObject(bp, m.bck, m.objNames[0], &api.GetArgs{Writer: buf})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, buf.Len() == cos.KiB, "expected intact %d-byte object, got %d bytes", cos.KiB, buf.Len())
}

func assertETLNotBuilt(t *testing.T, err error) {
	t.Helper()
	tassert.Fatalf(t, err != nil, "expected ETL request to fail")
	herr := cmn.AsErrHTTP(err)
	tassert.Fatalf(t, herr != nil, "expected HTTP error, got %T: %v", err, err)
	tassert.Fatalf(t, herr.Status == http.StatusNotImplemented, "expected status %d, got %d", http.StatusNotImplemented, herr.Status)
	tassert.Fatalf(t, herr.TypeCode == "ErrUnsupp", "expected ErrUnsupp, got %q", herr.TypeCode)
	tassert.Fatalf(t, herr.Message == etl.ErrNotBuilt.Error(), "expected %q, got %q", etl.ErrNotBuilt, herr.Message)
}
