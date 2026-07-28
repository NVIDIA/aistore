// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/transport"
)

func TestMalformedTransportHeader(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	const hlen = 13*cos.SizeofI16 + 2*cos.SizeofI64
	smap := tools.GetClusterMap(t, tools.RandomProxyURL(t))
	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	frame := make([]byte, 2*cos.SizeofI64+hlen)
	binary.BigEndian.PutUint64(frame, uint64(hlen))
	binary.BigEndian.PutUint64(frame[cos.SizeofI64:], xoshiro256.Hash(uint64(hlen)))
	binary.BigEndian.PutUint16(frame[2*cos.SizeofI64:], uint16(hlen))

	url := target.URL(cmn.NetIntraControl) + transport.ObjURLPath(ec.ReqStreamName)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(frame))
	tassert.CheckFatal(t, err)
	req.Header.Set(apc.HdrSessID, "1")
	req.Header.Set(apc.HdrSenderID, smap.Primary.ID())

	resp, err := tools.BaseAPIParams(url).Client.Do(req)
	tassert.CheckFatal(t, err)
	defer resp.Body.Close()
	herr := cmn.AsErrHTTP(cmn.CheckResp(resp, req.Method, req.URL.Path))
	tassert.Fatalf(t, herr != nil && herr.Status == http.StatusBadRequest && herr.TypeCode == "ErrSBR",
		"unexpected error: %v", herr)
	expected := fmt.Sprintf("%s[%s<=%s] sbr_obj_hdr_inval:[ctx: hlen=%d err: "+
		"malformed object header: at offset 2: unexpected EOF]",
		ec.ReqStreamName, smap.Primary.ID(), target.ID(), hlen)
	tassert.Fatalf(t, herr.Message == expected, "unexpected error: %q", herr.Message)
	tassert.CheckFatal(t, api.Health(tools.BaseAPIParams(target.URL(cmn.NetPublic))))
}
