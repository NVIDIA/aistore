// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"

	jsoniter "github.com/json-iterator/go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
)

const PrefixXactID = "etl-"

const (
	// init message types
	SpecType    = "spec"
	CodeType    = "code"
	ETLSpecType = "etl-spec"

	// common fields
	Name              = "name"
	CommunicationType = "communication_type"
	DirectPut         = "direct_put"

	// `InitSpecMsg` fields
	Spec = "spec"

	// `ETLSpecMsg` fields
	Runtime = "runtime"
	Image   = "image"
	Command = "command"
	Env     = "env"

	// consts for unmarshalling ETL details
	InitMsgType = "init_msg"
	ObjErrsType = "obj_errors"
)

// consistent with rfc2396.txt "Uniform Resource Identifiers (URI): Generic Syntax"
const CommTypeSeparator = "://"

const (
	CommTypeAnnotation         = "communication_type" // communication type to use if not explicitly set in the init message
	SupportDirectPutAnnotation = "support_direct_put" // indicates whether the ETL supports direct PUT; affects how the target interacts with it
	WaitTimeoutAnnotation      = "wait_timeout"       // timeout duration to wait for the ETL pod to become ready
)

const (
	DefaultInitTimeout   = 45 * time.Second
	DefaultObjTimeout    = 10 * time.Second
	DefaultAbortTimeout  = 2 * time.Second
	DefaultContainerPort = 8000
)

// enum ETL lifecycle status (see docs/etl.md#etl-pod-lifecycle for details)
type Stage int

const (
	Unknown Stage = iota
	Initializing
	Running
	Aborted
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

type (
	InitMsg interface {
		Name() string
		Cname() string
		PodName(tid string) string // ETL pod name on the given target
		MsgType() string
		CommType() string
		Validate() error
		IsDirectPut() bool
		ParsePodSpec() (*corev1.Pod, error)
		Timeouts() (initTimeout, objTimeout cos.Duration)
		GetEnv() []corev1.EnvVar
		String() string
	}

	// and implementations
	// swagger:model
	InitMsgBase struct {
		EtlName          string          `json:"name" yaml:"name"`
		CommTypeX        string          `json:"communication" yaml:"communication"`
		Env              []corev1.EnvVar `json:"env,omitempty" yaml:"env,omitempty" swaggertype:"array,object"`
		InitTimeout      cos.Duration    `json:"init_timeout,omitempty" yaml:"init_timeout,omitempty" swaggertype:"primitive,string"`
		ObjTimeout       cos.Duration    `json:"obj_timeout,omitempty" yaml:"obj_timeout,omitempty" swaggertype:"primitive,string"`
		SupportDirectPut bool            `json:"support_direct_put,omitempty" yaml:"support_direct_put,omitempty"`
	}

	// swagger:model
	InitSpecMsg struct {
		Spec        []byte `json:"spec"`
		InitMsgBase `yaml:",inline"`
	}

	// ETLSpecMsg is a YAML representation of the ETL pod spec.
	// swagger:model
	ETLSpecMsg struct {
		InitMsgBase `yaml:",inline"`            // included all optional fields from InitMsgBase
		Runtime     RuntimeSpec                 `json:"runtime" yaml:"runtime"`
		Resources   corev1.ResourceRequirements `json:"resources,omitempty" yaml:"resources,omitempty" swaggertype:"object"`
	}

	// swagger:model
	RuntimeSpec struct {
		Image   string          `json:"image" yaml:"image"`
		Command []string        `json:"command,omitempty" yaml:"command,omitempty"`
		Env     []corev1.EnvVar `json:"env,omitempty" yaml:"env,omitempty" swaggertype:"array,object"`
	}

	WebsocketCtrlMsg struct {
		Pipeline string `json:"pipeline,omitempty"`
		Targs    string `json:"etl_args,omitempty"`
		FQN      string `json:"fqn,omitempty"`
		Path     string `json:"path,omitempty"`
	}

	// ETLObjDownloadCtx contains ETL download job parameters
	ETLObjDownloadCtx struct {
		ObjName string // Target object name
		Link    string // Source URL to download from
		ETLArgs string // Transform arguments
	}

	// used by 2PC initialization
	PodMap  map[string]PodInfo // target ID to ETL pod info
	PodInfo struct {
		URI     string `json:"uri"`      // ETL pod URI
		PodName string `json:"pod_name"` // ETL pod name
		SvcName string `json:"svc_name"` // ETL service name
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

	Details struct {
		InitMsg InitMsg  `json:"init_msg"`
		ObjErrs []ObjErr `json:"obj_errors,omitempty"`
	}
	ObjErrs []ObjErr
	ObjErr  struct {
		ObjName string `json:"obj_name"` // object name
		Message string `json:"msg"`      // error message
		Ecode   int    `json:"ecode"`    // error code
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

var commTypes = []string{Hpush, Hpull, HpushStdin, WebSocket} // NOTE: must contain all

////////////////
// InitMsg*** //
////////////////

// interface guard
var (
	_ InitMsg = (*InitSpecMsg)(nil)
	_ InitMsg = (*ETLSpecMsg)(nil)
)

func (m *InitMsgBase) CommType() string          { return m.CommTypeX }
func (m *InitMsgBase) Name() string              { return m.EtlName }
func (m *InitMsgBase) Cname() string             { return "ETL[" + m.EtlName + "]" }
func (m *InitMsgBase) PodName(tid string) string { return m.EtlName + "-" + strings.ToLower(tid) }
func (m *InitMsgBase) IsDirectPut() bool         { return m.SupportDirectPut }

func (m *InitMsgBase) GetEnv() []corev1.EnvVar { return m.Env }
func (m *InitMsgBase) Timeouts() (initTimeout, objTimeout cos.Duration) {
	return m.InitTimeout, m.ObjTimeout
}

func (*InitSpecMsg) MsgType() string { return SpecType }
func (*ETLSpecMsg) MsgType() string  { return ETLSpecType }

func (m *InitSpecMsg) String() string {
	return fmt.Sprintf("init-%s[%s-%s], timeout=(%v, %v)", SpecType, m.Name(), m.CommType(), m.InitTimeout.D(), m.ObjTimeout.D())
}

func (e *ETLSpecMsg) String() string {
	return fmt.Sprintf("init-%s[%s-%s], env=%s, timeout=(%v, %v)", ETLSpecType, e.Name(), e.CommType(), e.FormatEnv(), e.InitTimeout.D(), e.ObjTimeout.D())
}

func UnmarshalInitMsg(b []byte) (InitMsg, error) {
	var err1, err2 error
	// try parsing it as ETLSpecMsg first
	var etlSpec ETLSpecMsg
	if err1 = jsoniter.Unmarshal(b, &etlSpec); err1 == nil {
		if err1 = etlSpec.Validate(); err1 == nil {
			return &etlSpec, nil
		}
	}

	// if fail, try parsing it as InitSpecMsg
	var podSpec InitSpecMsg
	if err2 = jsoniter.Unmarshal(b, &podSpec); err2 == nil {
		if err2 = podSpec.Validate(); err2 == nil {
			return &podSpec, nil
		}
	}

	return nil, fmt.Errorf("invalid etl.InitMsg: ETLSpecMsg error: %v; InitSpecMsg error: %v", err1, err2)
}

func (m *InitMsgBase) Validate(detail string) error {
	const ferr = "%v [%s]"

	if err := k8s.ValidateEtlName(m.EtlName); err != nil {
		return fmt.Errorf(ferr, err, detail)
	}

	errCtx := &cmn.ETLErrCtx{ETLName: m.Name()}
	if m.CommTypeX != "" && !slices.Contains(commTypes, m.CommTypeX) {
		err := fmt.Errorf("unknown comm-type %q", m.CommTypeX)
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

	if !strings.HasSuffix(m.CommTypeX, CommTypeSeparator) {
		m.CommTypeX += CommTypeSeparator
	}

	// NOTE: default timeout
	if m.InitTimeout == 0 {
		m.InitTimeout = cos.Duration(DefaultInitTimeout)
	}
	if m.ObjTimeout == 0 {
		m.ObjTimeout = cos.Duration(DefaultObjTimeout)
	}
	return nil
}

func (m *InitSpecMsg) Validate() error {
	errCtx := &cmn.ETLErrCtx{ETLName: m.Name()}

	// Check pod specification constraints.
	pod, err := m.ParsePodSpec()
	if err != nil {
		return cmn.NewErrETLf(errCtx, "failed to parse pod spec: %v\n%q", err, string(m.Spec))
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

	if dp, found := pod.ObjectMeta.Annotations[SupportDirectPutAnnotation]; found {
		m.SupportDirectPut, err = cos.ParseBool(dp)
		if err != nil {
			return err
		}
	}

	return m.InitMsgBase.Validate(m.String())
}

func (e *ETLSpecMsg) Validate() error {
	errCtx := &cmn.ETLErrCtx{ETLName: e.Name()}
	if e.Runtime.Image == "" {
		return cmn.NewErrETLf(errCtx, "runtime.image must be specified")
	}
	return e.InitMsgBase.Validate(e.String())
}

// ParsePodSpec parses `m.Spec` into a Kubernetes Pod object.
func (m *InitSpecMsg) ParsePodSpec() (*corev1.Pod, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(m.Spec, nil, nil)
	if err != nil {
		return nil, err
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		kind := obj.GetObjectKind().GroupVersionKind().Kind
		return nil, errors.New("expected pod spec, got: " + kind)
	}
	return pod, nil
}

func (e *ETLSpecMsg) ParsePodSpec() (*corev1.Pod, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:            e.Name(),
				Image:           e.Runtime.Image,
				ImagePullPolicy: corev1.PullAlways,
				Ports:           []corev1.ContainerPort{{Name: k8s.Default, ContainerPort: DefaultContainerPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/" + apc.ETLHealth,
							Port: intstr.FromString(k8s.Default),
						},
					},
				},
				Command:   e.Runtime.Command,
				Env:       e.Runtime.Env,
				Resources: e.Resources,
			}},
		},
	}
	return pod, nil
}

func (e *ETLSpecMsg) FormatEnv() string {
	var b cos.SB
	b.WriteString("[")
	for i, env := range e.Runtime.Env {
		b.WriteString(fmt.Sprintf("{\"%s\":\"%s\"}", env.Name, env.Value))
		if i < len(e.Runtime.Env)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	return b.String()
}

// UnmarshalYAML works around the fact that resource.Quantity can't unmarshal from YAML directly.
// We decode the YAML node into a map, marshal it to JSON, and unmarshal again to parse resource.Quantity correctly.
func (e *ETLSpecMsg) UnmarshalYAML(node *yaml.Node) error {
	var intermediate map[string]any
	if err := node.Decode(&intermediate); err != nil {
		return fmt.Errorf("yaml node decode: %w", err)
	}
	data, err := jsoniter.Marshal(intermediate)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	if err := jsoniter.Unmarshal(data, e); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}
	return nil
}

func (s Stage) String() string {
	switch s {
	case Initializing:
		return "Initializing"
	case Running:
		return "Running"
	case Aborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

func (eo ObjErr) Error() string {
	return fmt.Sprintf("ETL object %s transform error (%d): %s", eo.ObjName, eo.Ecode, eo.Message)
}

//////////////
// InfoList //
//////////////

var _ sort.Interface = (*InfoList)(nil)

func (il InfoList) Len() int           { return len(il) }
func (il InfoList) Less(i, j int) bool { return il[i].Name < il[j].Name }
func (il InfoList) Swap(i, j int)      { il[i], il[j] = il[j], il[i] }
func (il *InfoList) Append(i Info)     { *il = append(*il, i) }
