//go:build linux

/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"errors"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestAddMpathPrepareFailure(t *testing.T) {
	const (
		existing  = "/existing"
		candidate = "/candidate"
		disk      = "sda"
	)
	state := &ios{
		mpath2disks: make(map[string]FsDisks),
		disk2mpath:  make(cos.StrKVs),
		disk2sysfn:  make(cos.StrKVs),
		blockStats:  make(allBlockStats),
	}
	config := &cmn.Config{}
	devs := BlockDevs{&blockDev{name: disk, info: DiskInfo{Size: 512}}}
	if _, err := state.AddMpath(existing, "/dev/sda", "", config, devs, nil); err != nil {
		t.Fatal(err)
	}
	stats, statPath := state.blockStats[disk], state.disk2sysfn[disk]

	// disk validation must precede the ownership stamp
	called := false
	_, err := state.AddMpath(candidate, "/dev/sda", "", config, devs, func() error {
		called = true
		return nil
	})
	if err == nil || called {
		t.Fatalf("unlabeled shared disk: err=%v, prepare called=%t", err, called)
	}

	// a label allows sharing, but a failed stamp must preserve the existing registration
	stampErr := errors.New("ownership stamp failed")
	_, err = state.AddMpath(candidate, "/dev/sda", "shared", config, devs, func() error { return stampErr })
	if !errors.Is(err, stampErr) {
		t.Fatalf("expected stamp error, got %v", err)
	}
	if _, registered := state.mpath2disks[candidate]; registered || state.disk2mpath[disk] != existing ||
		state.blockStats[disk] != stats || state.disk2sysfn[disk] != statPath {
		t.Fatal("failed registration changed IOS state")
	}

	if _, err := state.AddMpath(candidate, "/dev/sda", "shared", config, devs, func() error { return nil }); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if _, registered := state.mpath2disks[candidate]; !registered || state.disk2mpath[disk] != candidate {
		t.Fatal("retry did not register the mountpath")
	}
}
