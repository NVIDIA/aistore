// Package main computes go.sum lines for an unpublished root module version.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/sumdb/dirhash"
	"golang.org/x/mod/zip"
)

// Usage:
// - print go.sum lines for the root module at a given Git revision, before publishing its tag
// - use gosum.sh for release checks; this helper only computes the checksums
// - see local README.md for details

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 4 {
		return errors.New("usage: gosum REPO_DIR MODULE_PATH GIT_TAG REVISION")
	}
	repo, modPath, ver, rev := args[0], args[1], args[2], args[3]
	if err := module.Check(modPath, ver); err != nil {
		return err
	}
	if module.CanonicalVersion(ver) != ver {
		return fmt.Errorf("non-canonical module version %q", ver)
	}

	// resolve both inputs once
	out, err := git(repo, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	repo = string(bytes.TrimSpace(out))
	out, err = git(repo, "rev-parse", "--verify", "--end-of-options", rev+"^{commit}")
	if err != nil {
		return err
	}
	rev = string(bytes.TrimSpace(out))
	gomod, err := git(repo, "show", rev+":go.mod")
	if err != nil {
		return err
	}
	parsed, err := modfile.Parse("go.mod", gomod, nil)
	if err != nil {
		return err
	}
	if parsed.Module == nil || parsed.Module.Mod.Path != modPath {
		return fmt.Errorf("go.mod at %s must declare module %q", rev, modPath)
	}

	tmp, err := os.CreateTemp("", "mod-*.zip")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	err = zip.CreateFromVCS(tmp, module.Version{Path: modPath, Version: ver}, repo, rev, "")
	if errClose := tmp.Close(); err == nil {
		err = errClose
	}
	if err != nil {
		return err
	}
	h1, err := dirhash.HashZip(tmp.Name(), dirhash.Hash1)
	if err != nil {
		return err
	}

	h1mod, err := dirhash.Hash1([]string{"go.mod"}, func(string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(gomod)), nil
	})
	if err != nil {
		return err
	}
	_, err = fmt.Printf("%s %s %s\n%s %s/go.mod %s\n", modPath, ver, h1, modPath, ver, h1mod)
	return err
}

// run git in the given directory; on failure, report git's own stderr
func git(dir string, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(context.Background(), "git", append([]string{"-C", dir}, args...)...).Output()
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(exitErr.Stderr))
		}
	}
	return out, err
}
