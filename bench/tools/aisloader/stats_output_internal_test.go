// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestStatsOutputCloseAndUpload(t *testing.T) {
	const stats = "test benchmark stats\n"
	for _, test := range []struct {
		name   string
		status int
	}{
		{name: "success", status: http.StatusOK},
		{name: "failure", status: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "stats-*")
			tassert.CheckFatal(t, err)
			defer file.Close()
			_, err = file.WriteString(stats)
			tassert.CheckFatal(t, err)
			output := &statsOutput{
				file:     file,
				filePath: file.Name(),
				isTmp:    true,
				bck:      cmn.Bck{Name: "results", Provider: apc.AIS},
				objName:  "stats.txt",
			}

			var received atomic.Bool // test runs concurrently
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received.Store(true)
				data, err := io.ReadAll(r.Body)
				tassert.CheckError(t, err)
				tassert.Errorf(t, string(data) == stats, "received data = %q", data)
				w.WriteHeader(test.status)
			}))
			defer server.Close()

			err = output.closeAndUpload(api.BaseParams{Client: server.Client(), URL: server.URL})
			if test.status == http.StatusOK {
				tassert.CheckFatal(t, err)
			} else {
				tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), file.Name()), "missing recovery path: %v", err)
			}
			tassert.Fatal(t, received.Load(), "upload did not reach the server")
			tassert.CheckFatal(t, output.closeAndRemoveTemp()) // deferred cleanup

			data, err := os.ReadFile(file.Name())
			if test.status == http.StatusOK {
				tassert.Fatalf(t, os.IsNotExist(err), "temporary file remains: %v", err)
			} else {
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, string(data) == stats, "retained data = %q", data)
			}
		})
	}
}
