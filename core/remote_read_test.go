// Package core tests core metadata and in-cluster API helpers.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestRemoteSourceErrorPreservesStructuredContext(t *testing.T) {
	cause := errors.New("backend denied")
	src := &RemoteSource{
		Provider: "s3",
		Bucket:   "s3://src-bck",
		Object:   "path/object.parquet",
	}
	err := src.WrapErr("GetObjReader", http.StatusForbidden, cause)

	if !errors.Is(err, cause) {
		t.Fatalf("expected wrapped error to preserve cause")
	}
	var rerr *RemoteSourceError
	if !errors.As(err, &rerr) {
		t.Fatalf("expected RemoteSourceError, got %T", err)
	}
	if rerr.Provider != "s3" || rerr.Bucket != "s3://src-bck" ||
		rerr.Object != "path/object.parquet" || rerr.Operation != "GetObjReader" ||
		rerr.StatusCode != http.StatusForbidden {
		t.Fatalf("unexpected remote read context: %+v", rerr)
	}

	msg := err.Error()
	for _, want := range []string{"remote source s3 GetObjReader failed", "s3://src-bck/path/object.parquet", "status=403", "backend denied"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected %q in %q", want, msg)
		}
	}
}

func TestRemoteSourceWrapErrNilSafe(t *testing.T) {
	cause := errors.New("plain")
	if got := (*RemoteSource)(nil).WrapErr("copy", 0, cause); got != cause {
		t.Fatalf("expected nil source to preserve error, got %v", got)
	}
	src := &RemoteSource{Provider: "s3"}
	if got := src.WrapErr("copy", 0, nil); got != nil {
		t.Fatalf("expected nil error to stay nil, got %v", got)
	}
}
