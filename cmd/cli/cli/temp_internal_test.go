// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core/meta"

	urfave "github.com/urfave/cli"
)

func TestTemporaryOutputsIgnorePredictableSymlinks(t *testing.T) {
	t.Run("upload failures", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("TMPDIR", tmpDir)
		oldPath := filepath.Join(tmpDir, fmt.Sprintf(".ais-put-failures.%d.log", os.Getpid()))
		victim := preseedFileSymlink(t, tmpDir, oldPath)

		params := &uparams{
			wop:        &putargs{},
			fobjs:      []fobj{{path: filepath.Join(tmpDir, "missing")}},
			numWorkers: 1,
		}
		if err := params.do(newTempTestContext()); err == nil {
			t.Fatal("expected upload failure")
		}

		assertFileUnchanged(t, victim)
		matches, err := filepath.Glob(filepath.Join(tmpDir, ".ais-put-failures.*.log"))
		if err != nil {
			t.Fatal(err)
		}
		assertSecureTempFile(t, matches, oldPath)
	})

	t.Run("scrub", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Setenv("TMPDIR", tmpDir)
		const tag = "missing"
		oldPath := filepath.Join(tmpDir, fmt.Sprintf(".ais-scrub-%s.%x.log", tag, os.Getpid()))
		victim := preseedFileSymlink(t, tmpDir, oldPath)

		log := &_log{tag: tag}
		(&scrBp{})._create(log)
		t.Cleanup(func() { log.fh.Close() })

		assertFileUnchanged(t, victim)
		assertSecureTempFile(t, []string{oldPath, log.fn}, oldPath)
	})

	t.Run("archived logs", func(t *testing.T) {
		tmpDir, outside := t.TempDir(), t.TempDir()
		t.Setenv("TMPDIR", tmpDir)
		if err := os.Symlink(outside, filepath.Join(tmpDir, "aislogs")); err != nil {
			t.Skipf("cannot create symlink: %v", err)
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("archive"))
		}))
		defer server.Close()
		prevBP := apiBP
		apiBP.URL, apiBP.Client, apiBP.Token = server.URL, server.Client(), ""
		t.Cleanup(func() { apiBP = prevBP })

		node := &meta.Snode{}
		node.Init("test-target", apc.Target, nil)
		if err := _getAllNodeLogs(newTempTestContext(), node, "", "", node.StringEx()); err != nil {
			t.Fatal(err)
		}

		entries, err := os.ReadDir(outside)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("predictable symlink target contains %d file(s)", len(entries))
		}
		matches, err := filepath.Glob(filepath.Join(tmpDir, "aislogs-*"))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) != 1 {
			t.Fatalf("expected one private log directory, got %v", matches)
		}
		info, err := os.Lstat(matches[0])
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("expected private log directory, got mode %v", info.Mode())
		}
		content, err := os.ReadFile(filepath.Join(matches[0], apc.Target+"-"+node.ID()+".tar.gz"))
		if err != nil {
			t.Fatal(err)
		}
		if string(content) != "archive" {
			t.Fatalf("unexpected archived log content %q", content)
		}
	})
}

func TestWriteFailureLogErrors(t *testing.T) {
	for _, paths := range [][]string{{"failed"}, nil} {
		fh, err := os.CreateTemp(t.TempDir(), "failure-log.*")
		if err != nil {
			t.Fatal(err)
		}
		if err := fh.Close(); err != nil {
			t.Fatal(err)
		}
		failedPaths := make(chan string, len(paths))
		for _, path := range paths {
			failedPaths <- path
		}
		close(failedPaths)
		if err := writeFailureLog(fh, failedPaths); err == nil {
			t.Fatal("expected closed failure log to return an error")
		}
	}
}

func newTempTestContext() *urfave.Context {
	app := urfave.NewApp()
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	return urfave.NewContext(app, flag.NewFlagSet("test", flag.ContinueOnError), nil)
}

func preseedFileSymlink(t *testing.T, dir, link string) string {
	t.Helper()
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, link); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	return victim
}

func assertFileUnchanged(t *testing.T, path string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "unchanged" {
		t.Fatalf("predictable symlink overwrote %q", path)
	}
}

func assertSecureTempFile(t *testing.T, paths []string, predictable string) {
	t.Helper()
	for _, path := range paths {
		if path == predictable {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("expected private regular file, got %q mode %v", path, info.Mode())
		}
		return
	}
	t.Fatal("secure temporary file was not created")
}
