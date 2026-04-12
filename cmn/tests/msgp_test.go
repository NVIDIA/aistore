package tests_test

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/stats"
	"github.com/tinylib/msgp/msgp"
)

func TestNodeStatusMsgpRoundTrip(t *testing.T) {
	orig := stats.NodeStatus{ /* populate a few fields including Tracker, Tcdf.Mountpaths, Cluster.Flags, MemCPUInfo.Mem */ }
	var buf bytes.Buffer
	w := msgp.NewWriter(&buf)
	if err := orig.EncodeMsg(w); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	var decoded stats.NodeStatus
	r := msgp.NewReader(&buf)
	if err := decoded.DecodeMsg(r); err != nil {
		t.Fatal(err)
	}
	// compare
}
