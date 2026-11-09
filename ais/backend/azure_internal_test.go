//go:build azure

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// client: at most cmn.BackendMaxAttempts; with cmn.WithNoRetry: exactly one
func TestAzureRetryAttempts(t *testing.T) {
	orig := sdkRetryDflt
	sdkRetryDflt.noBackoff = true
	t.Cleanup(func() { sdkRetryDflt = orig })

	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Ms-Error-Code", "ServerBusy")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	creds, err := azblob.NewSharedKeyCredential("account", base64.StdEncoding.EncodeToString([]byte("key")))
	tassert.CheckFatal(t, err)

	const (
		bucket = "bucket"
		key    = "obj"
	)
	var (
		cntURL = srv.URL + "/" + bucket
		blURL  = cntURL + "/" + key
	)
	ops := []struct {
		name string
		call func(ctx context.Context) error
	}{
		{"head-bucket", func(ctx context.Context) error {
			client, err := container.NewClientWithSharedKeyCredential(cntURL, creds, (*container.ClientOptions)(azOpts()))
			tassert.CheckFatal(t, err)
			_, err = client.GetProperties(azCtx(ctx), nil)
			return err
		}},
		{"list", func(ctx context.Context) error {
			client, err := container.NewClientWithSharedKeyCredential(cntURL, creds, (*container.ClientOptions)(azOpts()))
			tassert.CheckFatal(t, err)
			_, err = client.NewListBlobsFlatPager(nil).NextPage(azCtx(ctx))
			return err
		}},
		{"head", func(ctx context.Context) error {
			client, err := blockblob.NewClientWithSharedKeyCredential(blURL, creds, (*blockblob.ClientOptions)(azOpts()))
			tassert.CheckFatal(t, err)
			_, err = client.GetProperties(azCtx(ctx), nil)
			return err
		}},
		{"get", func(ctx context.Context) error {
			client, err := blockblob.NewClientWithSharedKeyCredential(blURL, creds, (*blockblob.ClientOptions)(azOpts()))
			tassert.CheckFatal(t, err)
			_, err = client.DownloadStream(azCtx(ctx), nil)
			return err
		}},
		{"put", func(ctx context.Context) error {
			client, err := azblob.NewClientWithSharedKeyCredential(srv.URL, creds, azOpts())
			tassert.CheckFatal(t, err)
			_, err = client.UploadStream(azCtx(ctx), bucket, key, strings.NewReader("value"), nil)
			return err
		}},
		{"delete", func(ctx context.Context) error {
			client, err := azblob.NewClientWithSharedKeyCredential(srv.URL, creds, azOpts())
			tassert.CheckFatal(t, err)
			_, err = client.DeleteBlob(azCtx(ctx), bucket, key, nil)
			return err
		}},
	}
	// multipart: through the backend
	var (
		bp  = &azbp{u: srv.URL, creds: creds}
		lom = &core.LOM{ObjName: key}
	)
	lom.Bck().Name, lom.Bck().Provider = bucket, apc.Azure
	ops = append(ops,
		struct {
			name string
			call func(ctx context.Context) error
		}{"put-mpt-part", func(ctx context.Context) error {
			_, _, err := bp.PutMptPart(ctx, lom, cos.NewByteReader([]byte("part")), nil, "upload", 4, 1)
			return err
		}},
		struct {
			name string
			call func(ctx context.Context) error
		}{"complete-mpt", func(ctx context.Context) error {
			_, _, _, err := bp.CompleteMpt(ctx, lom, nil, "upload", nil, apc.MptCompletedParts{{PartNumber: 1, ETag: "etag"}})
			return err
		}},
	)
	for _, op := range ops {
		for _, noRetry := range []bool{false, true} {
			ctx, want := context.Background(), int32(cmn.BackendMaxAttempts)
			if noRetry {
				ctx, want = cmn.WithNoRetry(ctx), 1
			}
			n.Store(0)
			err := op.call(ctx)
			tassert.Fatalf(t, err != nil, "%s(no-retry=%t): expected error", op.name, noRetry)
			got := n.Load()
			tassert.Fatalf(t, got == want, "%s(no-retry=%t): expected %d request(s), got %d", op.name, noRetry, want, got)
		}
	}
}
