// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestReadCompleteMptBody(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString("<parts/>"))
		body, ecode, err := readCompleteMptBody(req)
		if err != nil || ecode != 0 || string(body) != "<parts/>" {
			t.Fatalf("unexpected result: body=%q, status=%d, err=%v", body, ecode, err)
		}
	})

	t.Run("max parts", func(t *testing.T) {
		etag := `"d41d8cd98f00b204e9800998ecf8427e"`
		parts := make([]types.CompletedPart, core.MaxChunkCount)
		for i := range parts {
			parts[i] = types.CompletedPart{ETag: &etag, PartNumber: apc.Ptr(int32(i + 1))}
		}
		payload, err := xml.Marshal(&s3.CompleteMptUpload{Parts: parts})
		if err != nil {
			t.Fatal(err)
		}
		if len(payload) > maxCompleteMptBodySize {
			t.Fatalf("completion XML exceeds safety budget: %d > %d", len(payload), maxCompleteMptBodySize)
		}

		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
		body, ecode, err := readCompleteMptBody(req)
		if err != nil || ecode != 0 || len(body) != len(payload) {
			t.Fatalf("unexpected result: body=%d bytes, status=%d, err=%v", len(body), ecode, err)
		}
		decoded, err := s3.DecodeXML[*s3.CompleteMptUpload](body)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded.Parts) != core.MaxChunkCount {
			t.Fatalf("unexpected decoded parts: %d", len(decoded.Parts))
		}
	})

	t.Run("declared too large", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
		req.ContentLength = maxCompleteMptBodySize + 1
		_, ecode, err := readCompleteMptBody(req)
		if err == nil || ecode != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected status %d, got %d (%v)", http.StatusRequestEntityTooLarge, ecode, err)
		}
	})

	t.Run("stream too large", func(t *testing.T) {
		payload := bytes.NewReader(make([]byte, maxCompleteMptBodySize+1))
		req := httptest.NewRequest(http.MethodPost, "/", io.LimitReader(payload, maxCompleteMptBodySize+1))
		_, ecode, err := readCompleteMptBody(req)
		if err == nil || ecode != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected status %d, got %d (%v)", http.StatusRequestEntityTooLarge, ecode, err)
		}
	})
}
