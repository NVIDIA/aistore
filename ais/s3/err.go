// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

type Error struct {
	Code      string
	Message   string
	Resource  string
	RequestID string `xml:"RequestId"`
}

func (e *Error) mustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(e)
	debug.AssertNoErr(err)
}

func WriteErr(w http.ResponseWriter, r *http.Request, err error, errCode int) {
	var (
		out       Error
		in        *cmn.ErrHTTP
		ok        bool
		allocated bool
	)
	if in, ok = err.(*cmn.ErrHTTP); !ok {
		in = cmn.InitErrHTTP(r, err, errCode)
		allocated = true
	}
	out.Code, out.Message = in.TypeCode, in.Message
	sgl := memsys.PageMM().NewSGL(0)
	out.mustMarshal(sgl)

	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	w.Header().Set("X-Content-Type-Options", "nosniff")

	w.WriteHeader(in.Status)
	sgl.WriteTo(w)
	sgl.Free()
	if allocated {
		cmn.FreeHterr(in)
	}
}
