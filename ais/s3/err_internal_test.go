// Package s3 provides Amazon S3 compatibility layer.
// no-copyright
package s3

import (
	"encoding/xml"
	"errors"
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
		{"conflict", "ConditionalRequestConflict", "ConditionalRequestConflict", http.StatusConflict},
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
