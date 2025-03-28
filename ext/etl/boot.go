// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact/xreg"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const appLabel = "app"

type etlBootstrapper struct {
	// construction
	errCtx *cmn.ETLErrCtx
	config *cmn.Config
	msg    InitSpecMsg
	env    map[string]string

	// runtime
	xctn            core.Xact
	pod             *corev1.Pod
	svc             *corev1.Service
	uri             string
	originalPodName string
	podAddr         string
	originalCommand []string
}

func (b *etlBootstrapper) createPodSpec() (err error) {
	if b.pod, err = ParsePodSpec(b.errCtx, b.msg.Spec); err != nil {
		return
	}
	b.originalPodName = b.pod.GetName()
	b.errCtx.ETLName = b.originalPodName
	return b._prepSpec()
}

func (b *etlBootstrapper) _prepSpec() (err error) {
	// Override pod name: append target ID
	// (K8s doesn't allow `_` and uppercase)
	b.pod.SetName(k8s.CleanName(b.msg.Name() + "-" + core.T.SID()))
	b.errCtx.PodName = b.pod.GetName()
	b.pod.APIVersion = "v1"

	// The following combination of Affinity and Anti-Affinity provides for:
	// 1. The ETL container is always scheduled on the target invoking it.
	// 2. No more than a single ETL container with the same target is scheduled on
	//    the same node at any given point in time.
	if err = b._setAffinity(); err != nil {
		return
	}
	if err = b._setAntiAffinity(); err != nil {
		return
	}

	b._updPodCommand()
	b._updPodLabels()
	b._updReady()

	b._setPodEnv()

	if cmn.Rom.FastV(4, cos.SmoduleETL) {
		nlog.Infof("prep pod spec: %s, %+v", b.msg.String(), b.errCtx)
	}
	return
}

func (b *etlBootstrapper) createServiceSpec() {
	b.svc = &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: b.pod.GetName(),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Port: b.pod.Spec.Containers[0].Ports[0].ContainerPort},
			},
			Selector: map[string]string{
				podNameLabel: b.pod.Labels[podNameLabel],
				appLabel:     b.pod.Labels[appLabel],
			},
			Type: corev1.ServiceTypeNodePort,
		},
	}
	b._setSvcLabels()
	b.errCtx.SvcName = b.svc.Name
}

func (b *etlBootstrapper) setupConnection(schema string) (err error) {
	// Retrieve host IP of the pod.
	var hostIP string
	if hostIP, err = b._getHost(); err != nil {
		return
	}

	// Retrieve assigned port by the service.
	var nodePort int
	if nodePort, err = b._getPort(); err != nil {
		return
	}

	// the pod must be reachable via its tcp addr
	var ecode int
	b.podAddr = hostIP + ":" + strconv.Itoa(nodePort)
	if ecode, err = b.dial(); err != nil {
		if cmn.Rom.FastV(4, cos.SmoduleETL) {
			nlog.Warningf("%s: failed to dial %s [%+v, %s]", b.msg, b.podAddr, b.errCtx, b.uri)
		}
		err = cmn.NewErrETL(b.errCtx, err.Error(), ecode)
		return
	}

	// TODO -- FIXME: versus HTTPS deployment
	b.uri = schema + b.podAddr

	if cmn.Rom.FastV(4, cos.SmoduleETL) {
		nlog.Infof("%s: setup connection to %s [%+v]", b.msg, b.uri, b.errCtx)
	}
	return nil
}

// TODO -- FIXME: hardcoded (time, error counts) tunables
func (b *etlBootstrapper) dial() (int, error) {
	action := "dial POD " + b.pod.Name + " at " + b.podAddr
	ecode, err := cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call:      b.call,
		SoftErr:   10,
		HardErr:   2,
		Sleep:     3 * time.Second,
		Verbosity: cmn.RetryLogOff,
		Action:    action,
	})
	if err != nil {
		return ecode, fmt.Errorf("failed to wait for ETL Service/Pod %q to respond: %v", b.pod.Name, err)
	}
	return 0, nil
}

func (b *etlBootstrapper) call() (int, error) {
	conn, err := net.DialTimeout("tcp", b.podAddr, cmn.Rom.MaxKeepalive())
	if err != nil {
		return 0, err
	}
	cos.Close(conn)
	return 0, nil
}

func (b *etlBootstrapper) createEntity(entity string) error {
	client, err := k8s.GetClient()
	if err != nil {
		return err
	}
	switch entity {
	case k8s.Pod:
		err = client.Create(b.pod)
	case k8s.Svc:
		err = client.Create(b.svc)
	default:
		err = fmt.Errorf("invalid K8s entity %q", entity)
		debug.AssertNoErr(err)
		nlog.Errorln(err)
	}

	if err != nil {
		err = cmn.NewErrETLf(b.errCtx, "failed to create %s (err: %v)", entity, err)
	}
	return err
}

// waitPodReady waits until ETL Pod becomes `Ready`. This happens
// only after the Pod's containers will have started and the Pod's `readinessProbe`
// request (made by the Kubernetes itself) returns OK. If the Pod doesn't have
// `readinessProbe` config specified the last step gets skipped.
//
// NOTE: currently, we do require readinessProbe config in the ETL spec.
func (b *etlBootstrapper) waitPodReady(podCtx context.Context) error {
	var (
		timeout     = b.msg.Timeout.D()
		interval    = cos.ProbingFrequency(timeout)
		client, err = k8s.GetClient()
	)
	if err != nil {
		return cmn.NewErrETL(b.errCtx, err.Error())
	}
	if cmn.Rom.FastV(4, cos.SmoduleETL) {
		nlog.Infof("waiting pod %q ready (%+v, %s) timeout=%v ival=%v",
			b.pod.Name, b.msg.String(), b.errCtx, timeout, interval)
	}
	// wait
	err = wait.PollUntilContextTimeout(podCtx, interval, timeout, false, /*immediate*/
		func(context.Context) (ready bool, err error) {
			return checkPodReady(client, b.pod.Name)
		},
	)

	if err == nil {
		return nil
	}
	pod, _ := client.Pod(b.pod.Name)
	if pod == nil {
		return cmn.NewErrETL(b.errCtx, err.Error())
	}
	err = cmn.NewErrETLf(b.errCtx,
		`%v (pod phase: %q, pod conditions: %s`,
		err, pod.Status.Phase, podConditionsToString(pod.Status.Conditions),
	)
	return err
}

func (b *etlBootstrapper) setupXaction(xid string) core.Xact {
	rns := xreg.RenewETL(&b.msg, xid)
	debug.AssertNoErr(rns.Err)
	b.xctn = rns.Entry.Get()
	debug.Assertf(b.xctn.ID() == xid, "%s vs %s", b.xctn.ID(), xid)
	return b.xctn
}

func (b *etlBootstrapper) _updPodCommand() {
	if b.msg.CommTypeX != HpushStdin {
		return
	}

	b.originalCommand = b.pod.Spec.Containers[0].Command
	b.pod.Spec.Containers[0].Command = []string{"sh", "-c", "/server"}
}

// Sets pods node affinity, so pod will be scheduled on the same node as a target creating it.
func (b *etlBootstrapper) _setAffinity() error {
	if b.pod.Spec.Affinity == nil {
		b.pod.Spec.Affinity = &corev1.Affinity{}
	}
	if b.pod.Spec.Affinity.NodeAffinity == nil {
		b.pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	reqAffinity := b.pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAffinity := b.pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if reqAffinity != nil && len(reqAffinity.NodeSelectorTerms) > 0 || len(prefAffinity) > 0 {
		return cmn.NewErrETL(b.errCtx, "error in YAML spec: pod should not have any NodeAffinities defined")
	}

	nodeSelector := &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      nodeNameLabel,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{k8s.NodeName},
				}},
			},
		},
	}
	// RequiredDuringSchedulingIgnoredDuringExecution means that ETL container will be placed on the same machine as
	// target which creates it. This guarantee holds only during scheduling - initial pod start-up sequence.
	// However, a target removes its ETL pod when it goes down, so this guarantee is sufficient.
	// Additionally, if other targets notice that another target went down, they all stop all running ETL pods.
	b.pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
	return nil
}

// Sets pods node anti-affinity, so no two pods with the matching criteria is scheduled on the same node
// at the same time.
func (b *etlBootstrapper) _setAntiAffinity() error {
	if b.pod.Spec.Affinity == nil {
		b.pod.Spec.Affinity = &corev1.Affinity{}
	}
	if b.pod.Spec.Affinity.PodAntiAffinity == nil {
		b.pod.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}

	reqAntiAffinities := b.pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAntiAffinity := b.pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	if len(reqAntiAffinities) > 0 || len(prefAntiAffinity) > 0 {
		return cmn.NewErrETL(b.errCtx, "error in YAML spec, pod should not have any NodeAntiAffinities defined")
	}

	return nil
}

func (b *etlBootstrapper) _updPodLabels() {
	if b.pod.Labels == nil {
		b.pod.Labels = make(map[string]string, 6)
	}

	b.pod.Labels[appLabel] = "ais"
	b.pod.Labels[podNameLabel] = b.pod.GetName()
	b.pod.Labels[podNodeLabel] = k8s.NodeName
	b.pod.Labels[podTargetLabel] = core.T.SID()
	b.pod.Labels[appK8sNameLabel] = "etl"
	b.pod.Labels[appK8sComponentLabel] = "server"
}

func (b *etlBootstrapper) _setSvcLabels() {
	if b.svc.Labels == nil {
		b.svc.Labels = make(map[string]string, 4)
	}
	b.svc.Labels[appLabel] = "ais"
	b.svc.Labels[svcNameLabel] = b.svc.GetName()
	b.svc.Labels[appK8sNameLabel] = "etl"
	b.svc.Labels[appK8sComponentLabel] = "server"
}

func (b *etlBootstrapper) _updReady() {
	probe := b.pod.Spec.Containers[0].ReadinessProbe

	// If someone already set these values, we don't to touch them.
	if probe.TimeoutSeconds != 0 || probe.PeriodSeconds != 0 {
		return
	}

	// Set default values.
	probe.TimeoutSeconds = 5
	probe.PeriodSeconds = 10
}

// Sets environment variables that can be accessed inside the container.
func (b *etlBootstrapper) _setPodEnv() {
	containers := b.pod.Spec.Containers
	debug.Assert(len(containers) > 0)
	for idx := range containers {
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  "AIS_TARGET_URL",
			Value: core.T.Snode().URL(cmn.NetIntraData) + apc.URLPathETLObject.Join(reqSecret),
		})
		for k, v := range b.env {
			containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
				Name:  k,
				Value: v,
			})
		}
	}
	for idx := range b.pod.Spec.InitContainers {
		for k, v := range b.env {
			b.pod.Spec.InitContainers[idx].Env = append(b.pod.Spec.InitContainers[idx].Env, corev1.EnvVar{
				Name:  k,
				Value: v,
			})
		}
	}
}

func (b *etlBootstrapper) _getHost() (string, error) {
	client, err := k8s.GetClient()
	if err != nil {
		return "", cmn.NewErrETL(b.errCtx, err.Error())
	}
	p, err := client.Pod(b.pod.Name)
	if err != nil {
		return "", err
	}
	return p.Status.HostIP, nil
}

func (b *etlBootstrapper) _getPort() (int, error) {
	client, err := k8s.GetClient()
	if err != nil {
		return 0, cmn.NewErrETL(b.errCtx, err.Error())
	}

	s, err := client.Service(b.svc.Name)
	if err != nil {
		return 0, cmn.NewErrETL(b.errCtx, err.Error())
	}

	nodePort := int(s.Spec.Ports[0].NodePort)
	port, err := cmn.ValidatePort(nodePort)
	if err != nil {
		return 0, cmn.NewErrETL(b.errCtx, err.Error())
	}
	return port, nil
}
