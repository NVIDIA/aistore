// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"encoding/json"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl/runtime"
	jsoniter "github.com/json-iterator/go"
)

type (
	InitMsgBase struct {
		IDX         string       `json:"id"`
		CommTypeX   string       `json:"communication"`
		WaitTimeout cos.Duration `json:"timeout"`
	}
	InitSpecMsg struct {
		InitMsgBase
		Spec []byte `json:"spec"`
	}

	InitCodeMsg struct {
		InitMsgBase
		Code    []byte `json:"code"`
		Deps    []byte `json:"dependencies"`
		Runtime string `json:"runtime"`
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

// interface guard
var (
	_ InitMsg = (*InitCodeMsg)(nil)
	_ InitMsg = (*InitSpecMsg)(nil)
)

func (m InitMsgBase) CommType() string { return m.CommTypeX }
func (m InitMsgBase) ID() string       { return m.IDX }

func (m *InitCodeMsg) Validate() error {
	if err := cos.ValidateEtlID(m.IDX); err != nil {
		return fmt.Errorf("invalid etl ID: %v", err)
	}
	if len(m.Code) == 0 {
		return fmt.Errorf("source code is empty")
	}
	if m.Runtime == "" {
		return fmt.Errorf("runtime is not specified")
	}
	if _, ok := runtime.Runtimes[m.Runtime]; !ok {
		return fmt.Errorf("unsupported runtime provided: %s", m.Runtime)
	}
	if m.CommTypeX == "" {
		m.CommTypeX = PushCommType
	}
	if !cos.StringInSlice(m.CommTypeX, commTypes) {
		return fmt.Errorf("unsupported communication type provided: %s", m.CommTypeX)
	}
	return nil
}

func (*InitCodeMsg) InitType() string {
	return apc.ETLInitCode
}

func (*InitSpecMsg) InitType() string {
	return apc.ETLInitSpec
}

func (m *InitSpecMsg) Validate() (err error) {
	errCtx := &cmn.ETLErrorContext{}
	pod, err := ParsePodSpec(errCtx, m.Spec)
	if err != nil {
		return err
	}
	errCtx.ETLName = m.ID()

	if err := cos.ValidateEtlID(m.IDX); err != nil {
		return fmt.Errorf("invalid pod name: %v", err)
	}

	if err := validateCommType(m.CommType()); err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}
	if m.CommType() == "" {
		m.CommTypeX = PushCommType
	}

	// Check pod specification constraints.
	if len(pod.Spec.Containers) != 1 {
		err = cmn.NewErrETL(errCtx, "unsupported number of containers (%d), expected: 1", len(pod.Spec.Containers))
		return
	}
	container := pod.Spec.Containers[0]
	if len(container.Ports) != 1 {
		return cmn.NewErrETL(errCtx, "unsupported number of container ports (%d), expected: 1", len(container.Ports))
	}
	if container.Ports[0].Name != k8s.Default {
		return cmn.NewErrETL(errCtx, "expected port name: %q, got: %q", k8s.Default, container.Ports[0].Name)
	}

	// Validate that user container supports health check.
	// Currently we need the `default` port (on which the application runs) to
	// be same as the `readiness` probe port.
	if container.ReadinessProbe == nil {
		return cmn.NewErrETL(errCtx, "readinessProbe section is required in a container spec")
	}
	// TODO: Add support for other health checks.
	if container.ReadinessProbe.HTTPGet == nil {
		return cmn.NewErrETL(errCtx, "httpGet missing in the readinessProbe")
	}
	if container.ReadinessProbe.HTTPGet.Path == "" {
		return cmn.NewErrETL(errCtx, "expected non-empty path for readinessProbe")
	}
	// Currently we need the `default` port (on which the application runs)
	// to be same as the `readiness` probe port in the pod spec.
	if container.ReadinessProbe.HTTPGet.Port.StrVal != k8s.Default {
		return cmn.NewErrETL(errCtx, "readinessProbe port must be the %q port", k8s.Default)
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

func UnmarshalInitMsg(b []byte) (msg InitMsg, err error) {
	var msgInf map[string]json.RawMessage
	if err = jsoniter.Unmarshal(b, &msgInf); err != nil {
		return
	}
	if _, ok := msgInf["code"]; ok {
		msg = &InitCodeMsg{}
		err = jsoniter.Unmarshal(b, msg)
		return
	}
	if _, ok := msgInf["spec"]; ok {
		msg = &InitSpecMsg{}
		err = jsoniter.Unmarshal(b, msg)
		return
	}
	err = fmt.Errorf("invalid response body: %s", b)
	return
}
