// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/ext/etl/runtime"

	jsoniter "github.com/json-iterator/go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

const PrefixXactID = "etl-"

const (
	Spec = "spec"
	Code = "code"

	// additional environment variables to set in ETL container
	ArgType   = "ARG_TYPE"
	DirectPut = "DIRECT_PUT"
)

// consistent with rfc2396.txt "Uniform Resource Identifiers (URI): Generic Syntax"
const CommTypeSeparator = "://"

const (
	CommTypeAnnotation         = "communication_type" // communication type to use if not explicitly set in the init message
	SupportDirectPutAnnotation = "support_direct_put" // indicates whether the ETL supports direct PUT; affects how the target interacts with it
	WaitTimeoutAnnotation      = "wait_timeout"       // timeout duration to wait for the ETL pod to become ready
)

const (
	DefaultTimeout    = 45 * time.Second
	DefaultReqTimeout = 10 * time.Second
)

// enum ETL lifecycle status (see docs/etl.md#etl-pod-lifecycle for details)
type Stage int

const (
	Unknown Stage = iota
	Initializing
	Running
	Stopped
)

// enum communication types (`commTypes`)
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
	HpushStdin = "io://"
	// WebSocket communication.
	WebSocket = "ws://"
)

// enum arg types (`argTypes`)
const (
	ArgTypeDefault = ""
	ArgTypeURL     = "url"
	ArgTypeFQN     = "fqn"
)

type (
	InitMsg interface {
		Name() string
		MsgType() string // Code or Spec
		CommType() string
		ArgType() string
		Validate() error
		IsDirectPut() bool
		String() string
	}

	// and implementations
	InitMsgBase struct {
		EtlName          string       `json:"id"`
		CommTypeX        string       `json:"communication"` // enum commTypes
		ArgTypeX         string       `json:"argument"`      // enum argTypes
		Timeout          cos.Duration `json:"timeout"`
		SupportDirectPut bool         `json:"support_direct_put"`
	}
	InitSpecMsg struct {
		Spec []byte `json:"spec"`
		InitMsgBase
	}

	// ========================================================================================
	// InitCodeMsg carries the name of the transforming function;
	// the `Transform` function is mandatory and cannot be "" (empty) - it _will_ be called
	// by the `Runtime` container (see etl/runtime/all.go for all supported pre-built runtimes);
	// ChunkSize:
	//     0 (zero) - read the entire payload in memory and then transform it in one shot;
	//     > 0      - use chunk-size buffering and transform incrementally, one chunk at a time
	// Flags:
	//     bitwise flags: (streaming | debug | strict | ...) future enhancements
	// =========================================================================================
	InitCodeMsg struct {
		Runtime string `json:"runtime"`
		Funcs   struct {
			Transform string `json:"transform"`
		}
		Code []byte `json:"code"` // cannot be omitted

		Deps []byte `json:"dependencies"`
		InitMsgBase
		ChunkSize int64 `json:"chunk_size"`
		Flags     int64 `json:"flags"`
	}
)

type (
	InfoList []Info
	Info     struct {
		Name     string `json:"id"`
		Stage    string `json:"stage"`
		XactID   string `json:"xaction_id"`
		ObjCount int64  `json:"obj_count"`
		InBytes  int64  `json:"in_bytes"`
		OutBytes int64  `json:"out_bytes"`
	}

	LogsByTarget []Logs
	Logs         struct {
		TargetID string `json:"target_id"`
		Logs     []byte `json:"logs"`
	}

	HealthByTarget []*HealthStatus
	HealthStatus   struct {
		TargetID string `json:"target_id"`
		Status   string `json:"health_status"` // enum { HealthStatusRunning, ... } above
	}

	CPUMemByTarget []*CPUMemUsed
	CPUMemUsed     struct {
		TargetID string  `json:"target_id"`
		CPU      float64 `json:"cpu"`
		Mem      int64   `json:"mem"`
	}
)

var (
	commTypes = []string{Hpush, Hpull, HpushStdin, WebSocket}    // NOTE: must contain all
	argTypes  = []string{ArgTypeDefault, ArgTypeURL, ArgTypeFQN} // ditto
)

////////////////
// InitMsg*** //
////////////////

// interface guard
var (
	_ InitMsg = (*InitCodeMsg)(nil)
	_ InitMsg = (*InitSpecMsg)(nil)
)

func (m InitMsgBase) CommType() string  { return m.CommTypeX }
func (m InitMsgBase) ArgType() string   { return m.ArgTypeX }
func (m InitMsgBase) Name() string      { return m.EtlName }
func (m InitMsgBase) IsDirectPut() bool { return m.SupportDirectPut }
func (*InitCodeMsg) MsgType() string    { return Code }
func (*InitSpecMsg) MsgType() string    { return Spec }

func (m *InitCodeMsg) String() string {
	return fmt.Sprintf("init-%s[%s-%s-%s-%s]", Code, m.EtlName, m.CommTypeX, m.ArgTypeX, m.Runtime)
}

func (m *InitSpecMsg) String() string {
	return fmt.Sprintf("init-%s[%s-%s-%s]", Spec, m.EtlName, m.CommTypeX, m.ArgTypeX)
}

func (m *InitSpecMsg) errInvalidArg() error {
	return fmt.Errorf("%s: unexpected argument type %q", m, m.ArgTypeX)
}

func UnmarshalInitMsg(b []byte) (msg InitMsg, err error) {
	var msgInf map[string]json.RawMessage
	if err = jsoniter.Unmarshal(b, &msgInf); err != nil {
		return
	}

	_, hasCode := msgInf[Code]
	_, hasSpec := msgInf[Spec]

	if hasCode && hasSpec {
		return nil, fmt.Errorf("invalid etl.InitMsg: both '%s' and '%s' fields are present", Code, Spec)
	}

	if hasCode {
		msg = &InitCodeMsg{}
		err = jsoniter.Unmarshal(b, msg)
		return
	}
	if hasSpec {
		msg = &InitSpecMsg{}
		err = jsoniter.Unmarshal(b, msg)
		return
	}
	err = fmt.Errorf("invalid etl.InitMsg: %+v", msgInf)
	return
}

func (m *InitMsgBase) validate(detail string) error {
	const ferr = "%v [%s]"

	if err := k8s.ValidateEtlName(m.EtlName); err != nil {
		return fmt.Errorf(ferr, err, detail)
	}

	errCtx := &cmn.ETLErrCtx{ETLName: m.Name()}
	if m.CommTypeX != "" && !cos.StringInSlice(m.CommTypeX, commTypes) {
		err := fmt.Errorf("unknown comm-type %q", m.CommTypeX)
		return cmn.NewErrETLf(errCtx, ferr, err, detail)
	}

	if !cos.StringInSlice(m.ArgTypeX, argTypes) {
		err := fmt.Errorf("unsupported arg-type %q", m.ArgTypeX)
		return cmn.NewErrETLf(errCtx, ferr, err, detail)
	}

	//
	// not-implemented-yet type limitations:
	//
	if m.ArgTypeX == ArgTypeURL && m.CommTypeX != Hpull {
		err := fmt.Errorf("arg-type %q requires comm-type %q (%q is not supported yet)", m.ArgTypeX, Hpull, m.CommTypeX)
		return cmn.NewErrETLf(errCtx, ferr, err, detail)
	}
	if m.ArgTypeX == ArgTypeFQN && m.CommTypeX != Hpull && m.CommTypeX != Hpush && m.CommTypeX != WebSocket {
		err := fmt.Errorf("arg-type %q requires comm-type (%q or %q or %q) - %q is not supported yet",
			m.ArgTypeX, Hpull, Hpush, WebSocket, m.CommTypeX)
		return cmn.NewErrETLf(errCtx, ferr, err, detail)
	}

	//
	// ArgTypeFQN ("fqn") can also be globally disallowed
	//
	if m.ArgTypeX == ArgTypeFQN && cmn.Rom.Features().IsSet(feat.DontAllowPassingFQNtoETL) {
		err := fmt.Errorf("arg-type %q is not permitted by the configured feature flags (%s)",
			m.ArgTypeX, cmn.Rom.Features().String())
		return cmn.NewErrETLf(errCtx, ferr, err, detail)
	}

	// NOTE: default comm-type
	if m.CommType() == "" {
		cos.Infoln("Warning: empty comm-type, defaulting to", Hpush)
		m.CommTypeX = Hpush
	}
	if m.CommType() == WebSocket && !m.IsDirectPut() {
		err := errors.New("WebSocket without direct put is not supported yet. " +
			"Ensure that the `metadata.annotations.support_direct_put` annotation is set to `true` " +
			"and that your ETL server properly implements the direct put mechanism")
		return cmn.NewErrUnsuppErr(err)
	}
	// NOTE: default timeout
	if m.Timeout == 0 {
		m.Timeout = cos.Duration(DefaultTimeout)
	}
	return nil
}

func (m *InitCodeMsg) Validate() error {
	if err := m.InitMsgBase.validate(m.String()); err != nil {
		return err
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

	if m.Funcs.Transform == "" {
		return fmt.Errorf("transform function cannot be empty (comm-type %q, funcs %+v)", m.CommTypeX, m.Funcs)
	}
	if m.ChunkSize < 0 || m.ChunkSize > cos.MiB {
		return fmt.Errorf("chunk-size %d is invalid, expecting 0 <= chunk-size <= MiB (%q, comm-type %q)",
			m.ChunkSize, m.CommTypeX, m.Runtime)
	}
	return nil
}

func (m *InitSpecMsg) Validate() error {
	errCtx := &cmn.ETLErrCtx{ETLName: m.Name()}

	// Check pod specification constraints.
	pod, err := ParsePodSpec(errCtx, m.Spec)
	if err != nil {
		return err
	}
	if len(pod.Spec.Containers) != 1 {
		return cmn.NewErrETLf(errCtx, "unsupported number of containers (%d), expected: 1", len(pod.Spec.Containers))
	}
	container := pod.Spec.Containers[0]
	if len(container.Ports) != 1 {
		return cmn.NewErrETLf(errCtx, "unsupported number of container ports (%d), expected: 1", len(container.Ports))
	}
	if container.Ports[0].Name != k8s.Default {
		return cmn.NewErrETLf(errCtx, "expected port name: %q, got: %q", k8s.Default, container.Ports[0].Name)
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
		return cmn.NewErrETLf(errCtx, "readinessProbe port must be the %q port", k8s.Default)
	}

	if m.CommTypeX == "" {
		comm, found := pod.ObjectMeta.Annotations[CommTypeAnnotation]
		if !found {
			return cmn.NewErrETLf(errCtx, "annotations.communication_type must be provided, or specified in the init message")
		}
		m.CommTypeX = comm
	}

	if dp, found := pod.ObjectMeta.Annotations[SupportDirectPutAnnotation]; found {
		m.SupportDirectPut, err = cos.ParseBool(dp)
		if err != nil {
			return err
		}
	}

	if !strings.HasSuffix(m.CommTypeX, CommTypeSeparator) {
		m.CommTypeX += CommTypeSeparator
	}

	return m.InitMsgBase.validate(m.String())
}

func ParsePodSpec(errCtx *cmn.ETLErrCtx, spec []byte) (*corev1.Pod, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(spec, nil, nil)
	if err != nil {
		return nil, cmn.NewErrETLf(errCtx, "failed to parse pod spec: %v\n%q", err, string(spec))
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		kind := obj.GetObjectKind().GroupVersionKind().Kind
		return nil, cmn.NewErrETL(errCtx, "expected pod spec, got: "+kind)
	}
	return pod, nil
}

func (s Stage) String() string {
	switch s {
	case Initializing:
		return "Initializing"
	case Running:
		return "Running"
	case Stopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

//////////////
// InfoList //
//////////////

var _ sort.Interface = (*InfoList)(nil)

func (il InfoList) Len() int           { return len(il) }
func (il InfoList) Less(i, j int) bool { return il[i].Name < il[j].Name }
func (il InfoList) Swap(i, j int)      { il[i], il[j] = il[j], il[i] }
func (il *InfoList) Append(i Info)     { *il = append(*il, i) }
