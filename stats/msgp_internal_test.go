// Package stats: internal unit test to check NodeStatus dual encoding
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/sys"

	"github.com/tinylib/msgp/msgp"
)

func marshalMsgp(v interface {
	EncodeMsg(*msgp.Writer) error
}) ([]byte, error) {
	var buf bytes.Buffer
	w := msgp.NewWriter(&buf)
	if err := v.EncodeMsg(w); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalMsgp[T interface {
	DecodeMsg(*msgp.Reader) error
}](b []byte, v T) error {
	r := msgp.NewReader(bytes.NewReader(b))
	return v.DecodeMsg(r)
}

func compareNodeStatus(t *testing.T, exp, got *NodeStatus) {
	t.Helper()
	if !reflect.DeepEqual(exp, got) {
		eb, _ := json.MarshalIndent(exp, "", "  ") //nolint:musttag // must be ok
		gb, _ := json.MarshalIndent(got, "", "  ") //nolint:musttag // must be ok
		t.Fatalf("mismatch\nexpected:\n%s\n\ngot:\n%s", eb, gb)
	}
}

func makeRichNodeStatus() NodeStatus {
	return NodeStatus{
		Status:         "online",
		DeploymentType: "k8s",
		Version:        "4.5.0",
		BuildTime:      "2026-04-12 12:00:00-04:00",
		K8sPodName:     "ais-target-0",
		SmapVersion:    123,
		Node: Node{
			Snode: &meta.Snode{
				DaeType: "target",
				DaeID:   "t1",
				PubNet: meta.NetInfo{
					Hostname: "10.0.0.1",
					Port:     "8080",
					URL:      "http://10.0.0.1:8080",
				},
				DataNet: meta.NetInfo{
					Hostname: "10.0.0.2",
					Port:     "9080",
					URL:      "http://10.0.0.2:9080",
				},
				ControlNet: meta.NetInfo{
					Hostname: "10.0.0.3",
					Port:     "10080",
					URL:      "http://10.0.0.3:10080",
				},
				IDDigest: 42,
			},
			// exercise the hand-written copyValue/copyTracker marshal
			Tracker: copyTracker{
				"get.n":         {Value: 12345},
				"get.ns.total":  {Value: 67890123},
				"get.size":      {Value: 9876543210},
				"put.n":         {Value: 42},
				"put.ns.total":  {Value: 1111111},
				"err.n":         {Value: 0},
				"dsk.read.bps":  {Value: 18251776},
				"dsk.write.bps": {Value: 4517888},
			},
			Tcdf: fs.Tcdf{
				Mountpaths: map[string]*fs.CDF{
					"/mp/1": {
						Label: "mp1",
						FS: cos.FS{
							Fs:     "/dev/nvme0n1p1",
							FsType: "xfs",
							FsID:   cos.FsID{1, 2},
						},
						Disks:    []string{"nvme0n1"},
						Capacity: fs.Capacity{Used: 100, Avail: 900, PctUsed: 10},
					},
					"/mp/2": {
						Label: "mp2",
						FS: cos.FS{
							Fs:     "/dev/nvme1n1p1",
							FsType: "xfs",
							FsID:   cos.FsID{3, 4},
						},
						Disks:    []string{"nvme1n1"},
						Capacity: fs.Capacity{Used: 200, Avail: 800, PctUsed: 20},
					},
				},
				TotalUsed:  300,
				TotalAvail: 1700,
				PctMax:     20,
				PctAvg:     15,
				PctMin:     10,
			},
		},
		Cluster: cos.NodeStateInfo{
			Flags: 7,
		},
		MemCPUInfo: apc.MemCPUInfo{
			MemUsed:      111,
			MemAvail:     222,
			PctMemUsed:   12.5,
			PctCPUUsed:   7.5,
			LoadAvg:      sys.LoadAvg{One: 0.5, Five: 0.7, Fifteen: 0.9},
			CPUUtil:      33,
			CPUThrottled: 2,
			Mem: &sys.MemStat{
				Total:      1000,
				Used:       400,
				Free:       600,
				BuffCache:  100,
				ActualFree: 650,
				ActualUsed: 350,
				SwapTotal:  200,
				SwapFree:   150,
				SwapUsed:   50,
			},
			Proc: &sys.ProcStats{
				CPU: sys.ProcCPUStats{
					User:     10,
					System:   20,
					Total:    30,
					LastTime: 123456789,
					Percent:  2.5,
				},
				Mem: sys.ProcMemStats{
					Size:     100,
					Resident: 80,
					Share:    10,
				},
			},
		},
	}
}

func TestNodeStatusMsgpRoundTrip(t *testing.T) {
	in := makeRichNodeStatus()

	b, err := marshalMsgp(&in)
	if err != nil {
		t.Fatal(err)
	}

	var out NodeStatus
	if err := unmarshalMsgp(b, &out); err != nil {
		t.Fatal(err)
	}

	compareNodeStatus(t, &in, &out)
}

// equivalence holds only for fields that both encoders serialize;
// fields marked `json:"-"` or `msg:"-"` (e.g. Snode.LocalNet) are skipped
// by both, and unexported fields are skipped by both as well
func TestNodeStatusJSONMsgpEquivalent(t *testing.T) {
	in := makeRichNodeStatus()

	jb, err := json.Marshal(&in) //nolint:musttag // must be ok
	if err != nil {
		t.Fatal(err)
	}
	var fromJSON NodeStatus
	if err := json.Unmarshal(jb, &fromJSON); err != nil { //nolint:musttag // must be ok
		t.Fatal(err)
	}

	mb, err := marshalMsgp(&in)
	if err != nil {
		t.Fatal(err)
	}
	var fromMsgp NodeStatus
	if err := unmarshalMsgp(mb, &fromMsgp); err != nil {
		t.Fatal(err)
	}

	compareNodeStatus(t, &fromJSON, &fromMsgp)
}
