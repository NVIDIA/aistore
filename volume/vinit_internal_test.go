// Package volume provides volume (a.k.a. pool of disks) abstraction and methods to configure, store,
// and validate the corresponding metadata.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
)

func TestRestoreDisabledMountpath(t *testing.T) {
	const tid = "target01"

	root := t.TempDir()
	live, err := fs.NewMountpath(root, "")
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "disabled")
	vmd := newVMD(1)
	vmd.DaemonID = tid
	vmd.Mountpaths[path] = &fsMpathMD{
		Path:   path,
		Fs:     live.Fs,
		FsType: live.FsType,
		FsID:   live.FsID,
	}

	fs.NewTestMFS(mock.NewIOS())
	if err := initMPI(tid, cmn.GCO.Get(), nil, vmd, nil, false); err != nil {
		t.Fatal(err)
	}
	avail, disabled := fs.Get()
	saved := disabled[path]
	if len(avail) != 0 || saved == nil || saved.FS != live.FS {
		t.Fatalf("missing disabled mountpath or filesystem metadata: %v", saved)
	}
	if _, err := fs.Enable(path); err == nil {
		t.Fatal("enabled an absent mountpath")
	}

	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	enabled, err := fs.Enable(path)
	if err != nil {
		t.Fatal(err)
	}
	avail, disabled = fs.Get()
	if enabled == nil || avail[path] != enabled || len(disabled) != 0 ||
		enabled.FS != live.FS || enabled.PathDigest != saved.PathDigest {
		t.Fatalf("mountpath was not re-enabled correctly: %v", enabled)
	}

	// a replacement filesystem must leave the path disabled
	vmd.Mountpaths[path].FsType = "other-filesystem"
	fs.NewTestMFS(mock.NewIOS())
	if err := initMPI(tid, cmn.GCO.Get(), nil, vmd, nil, false); err != nil {
		t.Fatal(err)
	}
	_, err = fs.Enable(path)
	sie, ok := errors.AsType[*fs.ErrStorageIntegrity](err)
	if !ok || sie.Code != fs.SieFsDiffers {
		t.Fatalf("expected sie#50, got %v", err)
	}
	avail, disabled = fs.Get()
	if len(avail) != 0 || disabled[path] == nil {
		t.Fatal("failed re-enable changed mountpath registration")
	}
}
