// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"strings"
	"testing"

	jsoniter "github.com/json-iterator/go"
)

func TestLsblk_LVM(t *testing.T) {
	var lsblk LsBlk
	rawJSON := jsoniter.RawMessage(`
{
   "blockdevices": [
      {"name":"loop0", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"loop1", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"loop2", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"loop3", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"loop4", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"loop5", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"loop6", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"loop7", "alignment":0, "min-io":512, "opt-io":0, "phy-sec":512, "log-sec":512, "rota":false, "sched":"mq-deadline", "rq-size":256, "ra":128, "wsame":"0B"},
      {"name":"nvme0n1", "alignment":0, "min-io":512, "opt-io":512, "phy-sec":512, "log-sec":512, "rota":false, "sched":"none", "rq-size":1023, "ra":128, "wsame":"0B",
         "children": [
            {"name":"nvme0n1p1", "alignment":0, "min-io":512, "opt-io":512, "phy-sec":512, "log-sec":512, "rota":false, "sched":"none", "rq-size":1023, "ra":128, "wsame":"0B"},
            {"name":"nvme0n1p2", "alignment":0, "min-io":512, "opt-io":512, "phy-sec":512, "log-sec":512, "rota":false, "sched":"none", "rq-size":1023, "ra":128, "wsame":"0B",
               "children": [
                  {"name":"ubuntu-root", "alignment":0, "min-io":512, "opt-io":512, "phy-sec":512, "log-sec":512, "rota":false, "sched":null, "rq-size":128, "ra":128, "wsame":"0B"}
               ]
            }
         ]
      }
   ]
}`)
	if err := jsoniter.Unmarshal(rawJSON, &lsblk); err != nil {
		t.Fatalf("Failed to unmarshal %s: %v", string(rawJSON), err)
	}
	var (
		fs        = "/dev/mapper/ubuntu-root"
		trimmedFS string
	)
	if strings.HasPrefix(fs, devPrefixLVM) {
		trimmedFS = strings.TrimPrefix(fs, devPrefixLVM)
	} else {
		trimmedFS = strings.TrimPrefix(fs, devPrefixReg)
	}
	disks := make(FsDisks, 4)
	findDevs(lsblk.BlockDevices, trimmedFS, disks)
	if len(disks) == 0 {
		t.Fatal("No disks")
	} else {
		t.Logf("Found %s => %v\n", fs, disks)
	}
}

func TestLsblk_Reg(t *testing.T) {
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
func TestLsblk_Int(t *testing.T) {
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
