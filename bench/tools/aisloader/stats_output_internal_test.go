// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats/statsd"
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

func TestStatsOutputCloseAndRetain(t *testing.T) {
	const stats = "incomplete benchmark stats"
	file, err := os.CreateTemp(t.TempDir(), "stats-*")
	tassert.CheckFatal(t, err)
	defer file.Close()
	_, err = file.WriteString(stats)
	tassert.CheckFatal(t, err)

	output := statsOutput{file: file, filePath: file.Name(), isTmp: true}
	err = output.closeAndRetain(io.ErrShortWrite)
	tassert.Fatalf(t, errors.Is(err, io.ErrShortWrite), "expected write error: %v", err)
	tassert.Fatalf(t, strings.Contains(err.Error(), file.Name()), "missing recovery path: %v", err)
	tassert.CheckFatal(t, output.closeAndRemoveTemp()) // deferred cleanup

	data, err := os.ReadFile(file.Name())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == stats, "retained data = %q", data)
}

// showIntervalStats and finalizeStats with a failing writer from openStatsOutput.
func TestStatsOutputBufferedWrites(t *testing.T) {
	oldRunParams, oldStatsdC := runParams, statsdC
	oldAccumulated, oldInterval := accumulatedStats, intervalStats
	t.Cleanup(func() {
		runParams, statsdC = oldRunParams, oldStatsdC
		accumulatedStats, intervalStats = oldAccumulated, oldInterval
	})

	statsdC = &statsd.Client{}

	newOutput := func(t *testing.T, jsonFormat bool) *statsOutput {
		t.Helper()
		runParams = &params{statsParams: statsParams{
			jsonFormat:  jsonFormat,
			statsOutput: filepath.Join(t.TempDir(), "stats"),
		}}
		output, err := runParams.openStatsOutput()
		tassert.CheckFatal(t, err)
		t.Cleanup(func() { tassert.CheckError(t, output.close()) })
		now := time.Now()
		accumulatedStats, intervalStats = newStats(now), newStats(now)
		return output
	}

	t.Run("Write Error During Interval", func(t *testing.T) {
		output := newOutput(t, false)
		tassert.CheckFatal(t, output.close()) // early close before writing interval stats
		intervalStats.put.Add(1024, time.Millisecond)
		showIntervalStats(output.writer)
		intervalErr := output.writer.Flush()
		tassert.Fatalf(t, errors.Is(intervalErr, os.ErrClosed), "interval flush returned %v", intervalErr)

		intervalStats.put.Add(1024, time.Millisecond)
		finalizeStats(output.writer)
		err := output.writer.Flush()
		tassert.Fatalf(t, err == intervalErr, "final flush lost the interval error: %v", err)
		tassert.Fatalf(t, accumulatedStats.put.Total() == 2, "PUT total = %d", accumulatedStats.put.Total())
	})

	t.Run("Write Error During Finalization", func(t *testing.T) {
		output := newOutput(t, true)
		intervalStats.put.Add(1024, time.Millisecond)
		showIntervalStats(output.writer)
		tassert.CheckFatal(t, output.writer.Flush())
		tassert.CheckFatal(t, output.close()) // early close before writing final stats

		intervalStats.put.Add(1024, time.Millisecond)
		finalizeStats(output.writer)
		err := output.writer.Flush()
		tassert.Fatalf(t, errors.Is(err, os.ErrClosed), "final flush returned %v", err)
		tassert.Fatalf(t, accumulatedStats.put.Total() == 2, "PUT total = %d", accumulatedStats.put.Total())
	})
}
