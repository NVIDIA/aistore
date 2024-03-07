// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
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

// with user-friendly tip
func WriteMptErr(w http.ResponseWriter, r *http.Request, err error, errCode int, lom *core.LOM, uploadID string) {
	// specifically, for s3cmd example
	name := strings.Replace(lom.Cname(), apc.AISScheme+apc.BckProviderSeparator, apc.S3Scheme+apc.BckProviderSeparator, 1)
	s3cmd := "s3cmd abortmp " + name + " " + uploadID
	if len(s3cmd) > 50 {
		s3cmd = "\n  " + s3cmd
	}
	e := fmt.Errorf("%v\nUse upload ID %q to cleanup, e.g.: %s", err, uploadID, s3cmd)
	if errCode == 0 {
		errCode = http.StatusInternalServerError
	}
	WriteErr(w, r, e, errCode)
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
	out.Message = in.Message
	switch {
	case cmn.IsErrBucketAlreadyExists(err):
		out.Code = "BucketAlreadyExists"
	case cmn.IsErrBckNotFound(err):
		out.Code = "NoSuchBucket"
	case in.TypeCode != "":
		out.Code = in.TypeCode
	default:
		const awsErrPrefix = "aws-error" // TODO: dedup
		l := len(awsErrPrefix)
		// "aws-error[NotFound: blah]"
		if strings.HasPrefix(out.Message, awsErrPrefix) {
			if i := strings.Index(out.Message[l+1:], ":"); i > 0 {
				out.Code = out.Message[l+1 : l+i+1]
			}
		}
	}
	sgl := memsys.PageMM().NewSGL(0)
	out.mustMarshal(sgl)

	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	w.Header().Set(cos.HdrContentTypeOptions, "nosniff")

	w.WriteHeader(in.Status)
	sgl.WriteTo(w)
	sgl.Free()
	if allocated {
		cmn.FreeHterr(in)
	}
}
