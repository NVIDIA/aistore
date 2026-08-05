//go:build !etl

// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestRuntimeOff(t *testing.T) {
	xctn, podInfo, err := Init(nil, "", "")
	tassert.Fatalf(t, xctn == nil, "expected nil xaction, got %T", xctn)
	tassert.Fatalf(t, podInfo == (PodInfo{}), "expected empty pod info, got %+v", podInfo)
	tassert.Fatalf(t, err == ErrNotBuilt, "expected %v, got %v", ErrNotBuilt, err)

	getROC, xetl, session, err := GetOfflineTransform("", nil)
	tassert.Fatalf(t, getROC == nil, "expected nil reader, got %T", getROC)
	tassert.Fatalf(t, xetl == nil, "expected nil ETL xaction, got %T", xetl)
	tassert.Fatalf(t, session == nil, "expected nil session, got %T", session)
	tassert.Fatalf(t, err == ErrNotBuilt, "expected %v, got %v", ErrNotBuilt, err)

	comm, err := GetCommunicator("")
	tassert.Fatalf(t, comm == nil, "expected nil communicator, got %T", comm)
	tassert.Fatalf(t, err == ErrNotBuilt, "expected %v, got %v", ErrNotBuilt, err)
	list := List()
	tassert.Fatalf(t, len(list) == 0, "expected no ETL instances, got %d", len(list))

	xetl = &XactETL{}
	xetl.InitBase("runtime-off-test", apc.ActETLInline, nil)
	tassert.Fatalf(t, xetl.CtlMsg() == "", "expected an empty control message")
	tassert.Fatalf(t, xetl.Snap() != nil, "expected a valid xaction snapshot")
	objErr := &ObjErr{ObjName: "object", Message: "failed", Ecode: 500}
	xetl.AddObjErr("offline-xid", objErr)
	errs := xetl.GetObjErrs("offline-xid")
	tassert.Fatalf(t, errs == nil, "expected no recorded object errors, got %v", errs)
	tassert.Fatalf(t, xetl.offlineObjErrs == nil, "expected no object-error state, got %v", xetl.offlineObjErrs)
	tassert.Fatalf(t, xetl.m.TryLock(), "expected object-error mutex to remain unlocked")
	xetl.m.Unlock()
}
