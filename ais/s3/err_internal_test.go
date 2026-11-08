// Package s3 provides Amazon S3 compatibility layer.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
)

func TestMultipartWrappedHTTPError(t *testing.T) {
	for _, tc := range []struct {
		name, code, want string
		status           int
	}{
		{"conflict", cmn.TcodeCondWriteConflict, cmn.TcodeCondWriteConflict, http.StatusConflict},
		{"precondition", "ErrFailedTo", "PreconditionFailed", http.StatusPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := cmn.NewErrHTTP(nil, errors.New("native OCI rejected write"), tc.status)
			original.TypeCode = tc.code
			lom := &core.LOM{ObjName: "manifest"}
			lom.Bck().Name, lom.Bck().Provider = "bucket", apc.OCI
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/s3/bucket/manifest?uploadId=upload", http.NoBody)
			WriteMptErr(w, req, original, tc.status, lom, "upload")
			var response Error
			if err := xml.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.status || response.Code != tc.want {
				t.Fatalf("status=%d code=%q, want %d/%s", w.Code, response.Code, tc.status, tc.want)
			}
			if !strings.Contains(response.Message, "native OCI rejected write") || !strings.Contains(response.Message, `Use upload ID "upload"`) {
				t.Fatalf("lost original error or multipart cleanup hint: %q", response.Message)
			}
			if original.Message != "native OCI rejected write" || original.TypeCode != tc.code || original.Status != tc.status {
				t.Fatalf("error serialization mutated original: %+v", original)
			}
		})
	}
}

func TestWrappedHTTPErrorStatus(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		status, wrapped, want int
	}{
		{"outer-only", http.StatusConflict, 0, http.StatusConflict},
		{"wrapped-only", 0, http.StatusConflict, http.StatusConflict},
		{"wrapped-overrides", http.StatusBadRequest, http.StatusConflict, http.StatusConflict},
		{"both-zero", 0, 0, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := &cmn.ErrHTTP{Status: tc.wrapped, TypeCode: cmn.TcodeCondWriteConflict, Message: "native OCI rejected write"}
			err := fmt.Errorf("multipart completion: %w", original)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/s3/bucket/manifest", http.NoBody)
			WriteErr(w, req, ErrInfo{Err: err, Status: tc.status})
			var response Error
			if err := xml.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.want || response.Code != original.TypeCode {
				t.Fatalf("status=%d code=%q, want %d/%s", w.Code, response.Code, tc.want, original.TypeCode)
			}
			if response.Message != err.Error() {
				t.Fatalf("lost wrapper message: %q, want %q", response.Message, err.Error())
			}
			if original.Status != tc.wrapped {
				t.Fatalf("error serialization mutated original status: %d, want %d", original.Status, tc.wrapped)
			}
		})
	}
}
