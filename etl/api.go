// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/etl/runtime"
)

type (
	InitSpecMsg struct {
		ID          string       `json:"id"`
		Spec        []byte       `json:"spec"`
		CommType    string       `json:"communication_type"`
		WaitTimeout cos.Duration `json:"wait_timeout"`
	}

	InitCodeMsg struct {
		ID          string       `json:"id"`
		Code        []byte       `json:"code"`
		Deps        []byte       `json:"dependencies"`
		Runtime     string       `json:"runtime"`
		CommType    string       `json:"communication_type"`
		WaitTimeout cos.Duration `json:"wait_timeout"`
	}

	InfoList []Info
	Info     struct {
		ID string `json:"id"`

		ObjCount int64 `json:"obj_count"`
		InBytes  int64 `json:"in_bytes"`
		OutBytes int64 `json:"out_bytes"`
	}

	PodsLogsMsg []PodLogsMsg
	PodLogsMsg  struct {
		TargetID string `json:"target_id"`
		Logs     []byte `json:"logs"`
	}

	PodsHealthMsg []*PodHealthMsg
	// TODO: Extend with additional fields like Status.
	PodHealthMsg struct {
		TargetID string  `json:"target_id"`
		CPU      float64 `json:"cpu"`
		Mem      int64   `json:"mem"`
	}

	OfflineMsg struct {
		ID     string `json:"id"`      // ETL ID
		Prefix string `json:"prefix"`  // Prefix added to each resulting object.
		DryRun bool   `json:"dry_run"` // Don't perform any PUT

		// New objects names will have this extension. Warning: if in a source
		// bucket exist two objects with the same base name, but different
		// extension, specifying this field might cause object overriding.
		// This is because of resulting name conflict.
		Ext string `json:"ext"`
	}
)

func (m *InitCodeMsg) Validate() error {
	if len(m.Code) == 0 {
		return fmt.Errorf("source code is empty")
	}
	if m.Runtime == "" {
		return fmt.Errorf("runtime is not specified")
	}
	if _, ok := runtime.Runtimes[m.Runtime]; !ok {
		return fmt.Errorf("unsupported runtime provided: %s", m.Runtime)
	}
	if m.CommType == "" {
		m.CommType = PushCommType
	}
	if !cos.StringInSlice(m.CommType, commTypes) {
		return fmt.Errorf("unsupported communication type provided: %s", m.CommType)
	}
	return nil
}

func (p PodsLogsMsg) Len() int           { return len(p) }
func (p PodsLogsMsg) Less(i, j int) bool { return p[i].TargetID < p[j].TargetID }
func (p PodsLogsMsg) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p *PodLogsMsg) String(maxLen ...int) string {
	msg := string(p.Logs)
	orgLen := len(msg)

	if len(maxLen) > 0 && maxLen[0] > 0 && maxLen[0] < len(msg) {
		msg = msg[:maxLen[0]]
	}

	str := fmt.Sprintf("Target ID: %s; Logs:\n%s", p.TargetID, msg)
	if len(msg) < orgLen {
		str += fmt.Sprintf("\nand %d bytes more...", orgLen-len(msg))
	}
	return str
}

func (il InfoList) Len() int           { return len(il) }
func (il InfoList) Less(i, j int) bool { return il[i].ID < il[j].ID }
func (il InfoList) Swap(i, j int)      { il[i], il[j] = il[j], il[i] }
