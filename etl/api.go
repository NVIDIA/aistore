// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl/runtime"
	jsoniter "github.com/json-iterator/go"
)

type (
	InitMsgBase struct {
		IDX       string       `json:"id"`
		CommTypeX string       `json:"communication"`
		Timeout   cos.Duration `json:"timeout"`
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
		// ========================================================================================
		// `InitCodeMsg` carries the names of the transforming and, optionally, other functions;
		// - only the `Transform` function is mandatory and cannot be "" (empty) - it _will_ be called
		//   by the `Runtime` container (see etl/runtime/all.go for all supported pre-built runtimes);
		// - `Filter` receives object-name (string) and a MIN(4KB, Content-Length) bytes of the payload;
		//   returns true (to go ahead and transform) or false (to skip);
		// - if specified, `Before` and `After` are called only once - before and, respectively,
		//   after each transforming transaction.
		// =========================================================================================
		Funcs struct {
			Filter    string `json:"filter,omitempty"`
			Before    string `json:"before,omitempty"`
			After     string `json:"after,omitempty"`
			Transform string `json:"transform"` // cannot be omitted
		}
		// 0 (zero) - read the entire payload in memory and then transform it in one shot;
		// > 0 - use chunk-size buffering and transform incrementally, one chunk at a time
		ChunkSize int64 `json:"chunk_size"`
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
	PodHealthMsg  struct {
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

const (
	// ETL container receives POST request from target with the data. It
	// must read the data and return response to the target which then will be
	// transferred to the client.
	Hpush = "hpush://"
	// Target redirects the GET request to the ETL container. Then ETL container
	// contacts the target via `AIS_TARGET_URL` env variable to get the data.
	// The data is then transformed and returned to the client.
	Hpull = "hpull://"
	// Similar to redirection strategy but with usage of reverse proxy.
	Hrev = "hrev://"
	// Stdin/stdout communication.
	HpushStdin = "io://"
)

var commTypes = []string{Hpush, Hpull, Hrev, HpushStdin} // NOTE: must contain all

////////////////
// InitMsg*** //
////////////////

// interface guards
var (
	_ InitMsg = (*InitCodeMsg)(nil)
	_ InitMsg = (*InitSpecMsg)(nil)
)

func (m InitMsgBase) CommType() string { return m.CommTypeX }
func (m InitMsgBase) ID() string       { return m.IDX }

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

func (m *InitCodeMsg) Validate() error {
	if err := cos.ValidateEtlID(m.IDX); err != nil {
		return fmt.Errorf("invalid etl ID: %v (%q, comm-type %q)", err, m.Runtime, m.CommTypeX)
	}
	if len(m.Code) == 0 {
		return fmt.Errorf("source code is empty (%q)", m.Runtime)
	}
	if m.Runtime == "" {
		return fmt.Errorf("runtime is not specified (comm-type %q)", m.CommTypeX)
	}
	if _, ok := runtime.Get(m.Runtime); !ok {
		return fmt.Errorf("unsupported runtime %q (comm-type %q)", m.Runtime, m.CommTypeX)
	}
	if m.CommTypeX == "" {
		cos.Warningf("empty comm-type, defaulting to %q (%q)", Hpush, m.Runtime)
		m.CommTypeX = Hpush
	} else if !cos.StringInSlice(m.CommTypeX, commTypes) {
		return fmt.Errorf("unsupported comm-type %q (%q)", m.CommTypeX, m.Runtime)
	}
	if m.Funcs.Transform == "" {
		return fmt.Errorf("transform function cannot be empty (comm-type %q, funcs %+v)", m.CommTypeX, m.Funcs)
	}
	if m.ChunkSize < 0 || m.ChunkSize > cos.MiB {
		return fmt.Errorf("chunk-size %d is invalid, expecting 0 <= chunk-size <= MiB (%q, comm-type %q)",
			m.ChunkSize, m.CommTypeX, m.Runtime)
	}
	return nil
}

func (*InitCodeMsg) InitType() string { return apc.ETLInitCode }
func (*InitSpecMsg) InitType() string { return apc.ETLInitSpec }

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
		m.CommTypeX = Hpush
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

/////////////////
// PodsLogsMsg //
/////////////////

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

//////////////
// InfoList //
//////////////

var _ sort.Interface = (*InfoList)(nil)

func (il InfoList) Len() int           { return len(il) }
func (il InfoList) Less(i, j int) bool { return il[i].ID < il[j].ID }
func (il InfoList) Swap(i, j int)      { il[i], il[j] = il[j], il[i] }
