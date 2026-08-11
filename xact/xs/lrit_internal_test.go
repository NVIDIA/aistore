// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
)

func TestListRangeRejectsUnboundedTemplate(t *testing.T) {
	r := &lrit{}
	if err := r._inipr(&apc.ListRange{Template: "%0800000d"}); err == nil {
		t.Fatal("expected unbounded template to be rejected")
	}
}
