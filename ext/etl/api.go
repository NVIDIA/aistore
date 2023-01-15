// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/ext/etl/runtime"
	jsoniter "github.com/json-iterator/go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	Spec = "spec"
	Code = "code"
)

type (
	InitMsg interface {
		Name() string
		Type() string // Code or Spec
		CommType() string
		Validate() error
		String() string
	}

	// and implementations
	InitMsgBase struct {
		IDX       string       `json:"id"`
		CommTypeX string       `json:"communication"`
		Timeout   cos.Duration `json:"timeout"`
	}
	InitSpecMsg struct {
		InitMsgBase
		Spec []byte `json:"spec"` // NOTE: eq. `Spec`
	}

	InitCodeMsg struct {
		InitMsgBase
		Code    []byte `json:"code"` // NOTE: eq. `Code`
		Deps    []byte `json:"dependencies"`
		Runtime string `json:"runtime"`
		// ========================================================================================
		// `InitCodeMsg` carries the name of the transforming function;
		// the `Transform` function is mandatory and cannot be "" (empty) - it _will_ be called
		//   by the `Runtime` container (see etl/runtime/all.go for all supported pre-built runtimes);
		// =========================================================================================
		// TODO -- FIXME: decide if we need to remove nested struct for funcs
		Funcs struct {
			Transform string `json:"transform"` // cannot be omitted
		}
		// 0 (zero) - read the entire payload in memory and then transform it in one shot;
		// > 0 - use chunk-size buffering and transform incrementally, one chunk at a time
		ChunkSize int64 `json:"chunk_size"`
		// bitwise flags: (streaming | debug | strict | ...)
		Flags int64 `json:"flags"`
	}
)

type (
	InfoList []Info
	Info     struct {
		Name     string `json:"id"`
		ObjCount int64  `json:"obj_count"`
		InBytes  int64  `json:"in_bytes"`
		OutBytes int64  `json:"out_bytes"`
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
func (m InitMsgBase) Name() string     { return m.IDX }

func (*InitCodeMsg) Type() string { return Code }
func (*InitSpecMsg) Type() string { return Spec }

func (m *InitCodeMsg) String() string {
	return fmt.Sprintf("init-%s[%s-%s-%s]", Code, m.IDX, m.CommTypeX, m.Runtime)
}

func (m *InitSpecMsg) String() string {
	return fmt.Sprintf("init-%s[%s-%s]", Spec, m.IDX, m.CommTypeX)
}

// TODO: double-take, unmarshaling-wise. To avoid, include (`Spec`, `Code`) in API calls
func UnmarshalInitMsg(b []byte) (msg InitMsg, err error) {
	var msgInf map[string]json.RawMessage
	if err = jsoniter.Unmarshal(b, &msgInf); err != nil {
		return
	}
	if _, ok := msgInf[Code]; ok {
		msg = &InitCodeMsg{}
		err = jsoniter.Unmarshal(b, msg)
		return
	}
	if _, ok := msgInf[Spec]; ok {
		msg = &InitSpecMsg{}
		err = jsoniter.Unmarshal(b, msg)
		return
	}
	err = fmt.Errorf("invalid etl.InitMsg: %+v", msgInf)
	return
}

func (m *InitCodeMsg) Validate() error {
	if err := k8s.ValidateEtlName(m.IDX); err != nil {
		return fmt.Errorf("%v (%q, comm-type %q)", err, m.Runtime, m.CommTypeX)
	}
	if len(m.Code) == 0 {
		return fmt.Errorf("source code is empty (%q)", m.Runtime)
	}
	if m.Runtime == "" {
		return fmt.Errorf("runtime is not specified (comm-type %q)", m.CommTypeX)
	}
	if _, ok := runtime.Get(m.Runtime); !ok {
		return fmt.Errorf("unsupported runtime %q (supported: %v)", m.Runtime, runtime.GetNames())
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

func ParsePodSpec(errCtx *cmn.ETLErrCtx, spec []byte) (*corev1.Pod, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(spec, nil, nil)
	if err != nil {
		return nil, cmn.NewErrETL(errCtx, "failed to parse pod spec: %v\n%q", err, string(spec))
	}

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		kind := obj.GetObjectKind().GroupVersionKind().Kind
		return nil, cmn.NewErrETL(errCtx, "expected pod spec, got: %s", kind)
	}
	return pod, nil
}

func (m *InitSpecMsg) Validate() (err error) {
	if err := k8s.ValidateEtlName(m.IDX); err != nil {
		return err
	}
	errCtx := &cmn.ETLErrCtx{ETLName: m.Name()}
	pod, err := ParsePodSpec(errCtx, m.Spec)
	if err != nil {
		return err
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
func (il InfoList) Less(i, j int) bool { return il[i].Name < il[j].Name }
func (il InfoList) Swap(i, j int)      { il[i], il[j] = il[j], il[i] }
