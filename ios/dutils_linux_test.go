// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
)

func TestLsblk(t *testing.T) {
	var lsblk LsBlk
	rawJSON := jsoniter.RawMessage(
		`{
			"blockdevices": [
			{"name": "sda", "alignment": "0", "phy-sec": "512", "log-sec": "512", "rota": "1", "sched": "deadline",
			"children": [
			{"name": "sda1", "alignment": "0", "phy-sec": "512", "log-sec": "512", "rota": "1", "sched": "deadline"},
			{"name": "sda2", "alignment": "0", "phy-sec": "512", "log-sec": "512", "rota": "1", "sched": "deadline"}
			]
		},
		{"name": "sdb", "alignment": "0", "min-io": "262144", "opt-io": "1048576", "phy-sec": "1024", "log-sec": "512", "rota": "1", "sched": "deadline", "rq-size": "4096", "ra": "1024", "wsame": "0B"}]
	}`)
	out, _ := jsoniter.Marshal(&rawJSON)
	jsoniter.Unmarshal(out, &lsblk)
	if len(lsblk.BlockDevices) != 2 {
		t.Fatalf("expected 2 block devices, got %d", len(lsblk.BlockDevices))
	}
	if sec, _ := lsblk.BlockDevices[0].PhySec.Int64(); sec != 512 {
		t.Fatalf("expected 512 sector, got %d", sec)
	}
	if sec, _ := lsblk.BlockDevices[1].PhySec.Int64(); sec != 1024 {
		t.Fatalf("expected 1024 sector, got %d", sec)
	}
	if lsblk.BlockDevices[0].BlockDevices[1].Name != "sda2" {
		t.Fatalf("expected sda2 device, got %s", lsblk.BlockDevices[0].BlockDevices[1].Name)
	}
	if len(lsblk.BlockDevices[0].BlockDevices) != 2 {
		t.Fatalf("expected 2 block children devices, got %d", len(lsblk.BlockDevices[0].BlockDevices))
	}
}

// This test is similar to previous one but does not use strings to encode
// integers for fields like "phy-sec" or "log-sec". Such format should be
// parsed too.
func TestLsblkInt(t *testing.T) {
	var lsblk LsBlk
	rawJSON := jsoniter.RawMessage(
		`{
			"blockdevices": [
			{"name": "sda", "alignment": 0, "phy-sec": 512, "log-sec": 512, "rota": 1, "sched": "deadline",
			"children": [
			{"name": "sda1", "alignment": 0, "phy-sec": 512, "log-sec": 512, "rota": 1, "sched": "deadline"},
			{"name": "sda2", "alignment": 0, "phy-sec": 512, "log-sec": 512, "rota": 1, "sched": "deadline"}
			]
		},
		{"name": "sdb", "alignment": "0", "min-io": 262144, "opt-io": 1048576, "phy-sec": 1024, "log-sec": 512, "rota": 1, "sched": "deadline", "rq-size": 4096, "ra": 1024, "wsame": "0B"}]
	}`)
	out, _ := jsoniter.Marshal(&rawJSON)
	jsoniter.Unmarshal(out, &lsblk)
	if len(lsblk.BlockDevices) != 2 {
		t.Fatalf("expected 2 block devices, got %d", len(lsblk.BlockDevices))
	}
	if sec, _ := lsblk.BlockDevices[1].PhySec.Int64(); sec != 1024 {
		t.Fatalf("expected 1024 sector, got %d", sec)
	}
	if lsblk.BlockDevices[0].BlockDevices[1].Name != "sda2" {
		t.Fatalf("expected sda2 device, got %s", lsblk.BlockDevices[0].BlockDevices[1].Name)
	}
	if len(lsblk.BlockDevices[0].BlockDevices) != 2 {
		t.Fatalf("expected 2 block children devices, got %d", len(lsblk.BlockDevices[0].BlockDevices))
	}
}
