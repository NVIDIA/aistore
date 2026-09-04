// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/core"
)

func TestLritValidatesSourceName(t *testing.T) {
	r := &lrit{}
	for _, objName := range []string{
		"",
		"../escape",
		"dir/../../escape",
		"~/escape",
		"../../../../../../../../tmp/pwned_32114.txt",
	} {
		done, err := r.do(&core.LOM{ObjName: objName}, nil /*wi*/, nil /*smap*/)
		expErr := fmt.Sprintf("%s: invalid object name %q", badLrRequest, objName)
		if err == nil || err.Error() != expErr {
			t.Fatalf("expected list-range source %q to fail with %q, got: %v", objName, expErr, err)
		}
		if done {
			t.Fatalf("expected list-range source %q to not be done", objName)
		}
	}
}
