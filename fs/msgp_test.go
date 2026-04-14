// Package fs_test provides tests for fs package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"

	"github.com/tinylib/msgp/msgp"
)

// See scripts/msgp/README.md for the existing msgpack coverage and notes.

// note: identical marshalMsgp and unmarshalMsgp helpers in stats/msgp_internal_test.go
// (to keep each source self-contained)

func marshalMsgp(v interface{ EncodeMsg(*msgp.Writer) error }) ([]byte, error) {
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
func unmarshalMsgp[T interface{ DecodeMsg(*msgp.Reader) error }](b []byte, v T) error {
	r := msgp.NewReader(bytes.NewReader(b))
	return v.DecodeMsg(r)
}

func compareTcdfExt(t *testing.T, exp, got *fs.TcdfExt) {
	t.Helper()
	if !reflect.DeepEqual(exp, got) {
		eb, _ := json.MarshalIndent(exp, "", "  ") //nolint:musttag // test-only debug formatting
		gb, _ := json.MarshalIndent(got, "", "  ") //nolint:musttag // test-only debug formatting
		t.Fatalf("mismatch\nexpected:\n%s\n\ngot:\n%s", eb, gb)
	}
}

func makeRichTcdfExt() fs.TcdfExt {
	return fs.TcdfExt{
		AllDiskStats: cos.AllDiskStats{
			"nvme0n1": {RBps: 1000, Ravg: 10, WBps: 2000, Wavg: 20, Util: 55},
			"nvme1n1": {RBps: 3000, Ravg: 30, WBps: 4000, Wavg: 40, Util: 77},
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
			CsErr:      "some error",
			TotalUsed:  300,
			TotalAvail: 1700,
			PctMax:     20,
			PctAvg:     15,
			PctMin:     10,
		},
	}
}

func TestTcdfExtMsgpRoundTrip(t *testing.T) {
	in := makeRichTcdfExt()

	b, err := marshalMsgp(&in)
	if err != nil {
		t.Fatal(err)
	}

	var out fs.TcdfExt
	if err := unmarshalMsgp(b, &out); err != nil {
		t.Fatal(err)
	}

	compareTcdfExt(t, &in, &out)
}

func TestTcdfExtJSONMsgpEquivalent(t *testing.T) {
	in := makeRichTcdfExt()

	jb, err := json.Marshal(&in) //nolint:musttag // test-only debug formatting
	if err != nil {
		t.Fatal(err)
	}
	var fromJSON fs.TcdfExt
	if err := json.Unmarshal(jb, &fromJSON); err != nil { //nolint:musttag // test-only debug formatting
		t.Fatal(err)
	}

	mb, err := marshalMsgp(&in)
	if err != nil {
		t.Fatal(err)
	}
	var fromMsgp fs.TcdfExt
	if err := unmarshalMsgp(mb, &fromMsgp); err != nil {
		t.Fatal(err)
	}

	compareTcdfExt(t, &fromJSON, &fromMsgp)
}
