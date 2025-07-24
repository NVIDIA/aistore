// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"errors"
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

const ErrPrefix = "aws-error"

// See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
// e.g. XML:
// <Error>
// <Code>NoSuchKey</Code>
// <Message>The resource you requested does not exist</Message>
// <Resource>/mybucket/myfoto.jpg</Resource>
// <RequestId>4442587FB7D0A2F9</RequestId>
// </Error>
type Error struct {
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	Resource  string `xml:"Resource"`
	RequestID string `xml:"RequestId"`
}

func (e *Error) mustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(e)
	debug.AssertNoErr(err)
}

// with user-friendly tip
func WriteMptErr(w http.ResponseWriter, r *http.Request, err error, ecode int, lom *core.LOM, uploadID string) {
	if isErrNoSuchUpload(err) {
		if ecode == 0 {
			ecode = http.StatusNotFound
		}
	} else {
		if ecode == 0 {
			ecode = http.StatusInternalServerError
		}
		name := strings.Replace(lom.Cname(), apc.AISScheme+apc.BckProviderSeparator, apc.S3Scheme+apc.BckProviderSeparator, 1)
		s3cmd := "s3cmd abortmp " + name + " " + uploadID
		if len(s3cmd) > 50 {
			s3cmd = "\n  " + s3cmd
		}
		err = fmt.Errorf("%w\nUse upload ID %q to cleanup, e.g.: %s", err, uploadID, s3cmd)
	}
	WriteErr(w, r, err, ecode)
}

func WriteErr(w http.ResponseWriter, r *http.Request, err error, ecode int) {
	var (
		out       Error
		in        *cmn.ErrHTTP
		ok        bool
		allocated bool
	)
	if in, ok = err.(*cmn.ErrHTTP); !ok {
		in = cmn.InitErrHTTP(r, err, ecode)
		allocated = true
	}
	out.Message = in.Message
	switch {
	case cmn.IsErrBucketAlreadyExists(err):
		out.Code = "BucketAlreadyExists"
	case cmn.IsErrBckNotFound(err):
		out.Code = "NoSuchBucket"
	case isErrNoSuchUpload(err):
		out.Code = "NoSuchUpload"
	case in.TypeCode != "":
		out.Code = in.TypeCode
	default:
		l := len(ErrPrefix)
		// e.g. "aws-error[NotFound: blah]" as per backend/aws.go _awsErr() formatting
		if strings.HasPrefix(out.Message, ErrPrefix) {
			if i := strings.Index(out.Message[l+1:], ":"); i > 4 {
				code := out.Message[l+1 : l+i+1]
				if cos.IsAlphaNice(code) && code[0] >= 'A' && code[0] <= 'Z' {
					out.Code = code
				}
			}
		}
	}
	sgl := memsys.PageMM().NewSGL(0)
	out.mustMarshal(sgl)

	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	w.Header().Set(cos.HdrContentTypeOptions, "nosniff")

	w.WriteHeader(in.Status)
	sgl.WriteTo2(w)
	sgl.Free()
	if allocated {
		cmn.FreeHterr(in)
	}
}

type errNoSuchUpload struct {
	uploadID string
}

func newErrNoSuchUpload(uploadID string) *errNoSuchUpload {
	return &errNoSuchUpload{uploadID: uploadID}
}

func (e *errNoSuchUpload) Error() string {
	return fmt.Sprintf("upload %q not found", e.uploadID)
}

func isErrNoSuchUpload(err error) bool {
	if _, ok := err.(*errNoSuchUpload); ok {
		return true
	}
	var errMpt *errNoSuchUpload
	return errors.As(err, &errMpt)
}
