//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/tools/tassert"

	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

func TestOCIRangeReadsPreserveObjectAttributes(t *testing.T) {
	client, err := ocios.NewObjectStorageClientWithConfigurationProvider(newTestOCIConfigProvider(t, "us-ashburn-1"))
	tassert.CheckFatal(t, err)
	server := httptest.NewServer(http.HandlerFunc(serveOCIMetadataRange))
	t.Cleanup(server.Close)
	client.Host = server.URL
	bp := &ocibp{namespace: "namespace", defaultRegion: "us-ashburn-1"}
	bp.clients.Store(bp.defaultRegion, &client)
	lom := &core.LOM{ObjName: "object"}
	lom.Bck().Name, lom.Bck().Provider = "bucket", apc.OCI
	lom.ObjAttrs().Size = 20
	lom.ObjAttrs().CustomMD = cos.StrKVs{"sentinel": "unchanged"}
	before := *lom.ObjAttrs()
	before.CustomMD = maps.Clone(before.CustomMD)
	// Check once before concurrency so a metadata mutation fails deterministically.
	checkOCIMetadataRange(t, bp, lom)
	tassert.Fatalf(t, reflect.DeepEqual(*lom.ObjAttrs(), before), "range GET changed object attributes")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { checkOCIMetadataRange(t, bp, lom) })
	}
	wg.Wait()
	tassert.Errorf(t, reflect.DeepEqual(*lom.ObjAttrs(), before), "concurrent range GETs changed object attributes")
}

func serveOCIMetadataRange(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("ETag", "remote-etag")
	w.Header().Set("Opc-Meta-Env", "remote")
	w.Header().Set("Content-Type", "application/x-range-test")
	w.Header().Set("Content-Length", "5")
	w.Header().Set("Content-Range", "bytes 2-6/20")
	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.WriteString(w, "range")
}

func checkOCIMetadataRange(t *testing.T, bp *ocibp, lom *core.LOM) {
	t.Helper()
	res := bp.GetObjReader(t.Context(), lom, 2, 5)
	if res.Err != nil {
		t.Error(res.Err)
		return
	}
	body, readErr := io.ReadAll(res.R)
	closeErr := res.R.Close()
	tassert.CheckError(t, readErr)
	tassert.CheckError(t, closeErr)
	tassert.Errorf(t, string(body) == "range" && res.Size == 5, "range GET returned size=%d, body=%q", res.Size, body)
	tassert.Errorf(t, res.ExpCksum == nil, "range GET returned a whole-object checksum")
}
