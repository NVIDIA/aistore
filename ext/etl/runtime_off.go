//go:build !etl

// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package etl

// Keep shared callers buildable while failing every ETL runtime operation
// closed, independently of the HTTP handler selected by the ais package.

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

func Tinit() {
	nlog.Infoln("ETL runtime disabled (not built)")
}
func StopAll() {}

func Init(InitMsg, string, string) (core.Xact, PodInfo, error) {
	return nil, PodInfo{}, ErrNotBuilt
}

func CleanupEntities(*cmn.ETLErrCtx, string, string) error { return ErrNotBuilt }
func StopByXid(string, error) error                        { return ErrNotBuilt }
func Stop(string, error) error                             { return ErrNotBuilt }
func Delete(string) error                                  { return ErrNotBuilt }
func GetCommunicator(string) (Communicator, error)         { return nil, ErrNotBuilt }
func GetInitMsg(string) (InitMsg, error)                   { return nil, ErrNotBuilt }
func ValidateSecret(string, string) error                  { return ErrNotBuilt }

func GetPipeline([]string) (apc.ETLPipeline, error) { return nil, ErrNotBuilt }
func GetOfflineTransform(string, core.Xact) (core.GetROC, *XactETL, Session, error) {
	return nil, nil, nil, ErrNotBuilt
}

func List() []Info                           { return nil }
func PodLogs(string) (Logs, error)           { return Logs{}, ErrNotBuilt }
func PodHealth(string) (string, error)       { return "", ErrNotBuilt }
func PodMetrics(string) (*CPUMemUsed, error) { return nil, ErrNotBuilt }
func (*XactETL) AddObjErr(string, *ObjErr)   {}
func (*XactETL) GetObjErrs(string) []error   { return nil }
