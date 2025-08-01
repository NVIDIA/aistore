// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const appLabel = "app"

// etlBootstrapper is responsible for bootstrapping Kubernetes resources (pod/svc/volume) for the ETL
type etlBootstrapper struct {
	// construction
	errCtx *cmn.ETLErrCtx
	config *cmn.Config
	msg    InitMsg
	secret string

	// runtime
	k8sClient       k8s.Client
	pw              *podWatcher
	pod             *corev1.Pod
	svc             *corev1.Service
	targetPodSpec   *corev1.PodSpec
	targetPodName   string
	originalPodName string
	originalCommand []string
}

func (b *etlBootstrapper) createPodSpec() (err error) {
	if b.pod, err = b.msg.ParsePodSpec(); err != nil {
		return cmn.NewErrETLf(b.errCtx, "failed to parse: %v", err)
	}
	b.originalPodName = b.pod.GetName()
	b.errCtx.ETLName = b.originalPodName
	return b._prepSpec()
}

func (b *etlBootstrapper) _prepSpec() (err error) {
	// Override pod name: append target ID
	// (K8s doesn't allow `_` and uppercase)
	b.pod.SetName(b.msg.PodName(core.T.SID()))
	b.errCtx.PodName = b.pod.GetName()
	b.pod.APIVersion = "v1"
	b.pod.Kind = "Pod"

	// Initialize target pod name from environment
	b.targetPodName = os.Getenv(env.AisK8sPod)

	// Get target pod spec and cache it
	if b.targetPodSpec, err = b._getTargetPodSpec(); err != nil {
		return err
	}

	// The following combination of Affinity and Anti-Affinity provides for:
	// 1. The ETL container is always scheduled on the target invoking it.
	// 2. No more than a single ETL container with the same target is scheduled on
	//    the same node at any given point in time.
	if err = b._setAffinity(); err != nil {
		return err
	}
	if err = b._setAntiAffinity(); err != nil {
		return err
	}

	if b.msg.ArgType() == ArgTypeFQN {
		if err = b._setVol(); err != nil {
			nlog.Errorln(err)
			return err
		}
	}

	if err = b._setImagePullSecrets(); err != nil {
		return err
	}

	b._updPodCommand()
	b._updPodLabels()
	b._updReady()

	b._setPodEnv()

	if cmn.Rom.FastV(4, cos.SmoduleETL) {
		nlog.Infof("prep pod spec: %s, %+v", b.msg.String(), b.errCtx)
	}
	return err
}

func (b *etlBootstrapper) _setVol() (err error) {
	debug.Assert(len(b.targetPodSpec.Containers) > 0)
	mounts := make([]corev1.VolumeMount, 0, len(b.targetPodSpec.Containers[0].VolumeMounts))
	for _, vol := range b.targetPodSpec.Containers[0].VolumeMounts {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      vol.Name,
			MountPath: vol.MountPath,
			ReadOnly:  true, // restrict access from ETL Pods to read-only
		})
	}

	debug.Assertf(len(mounts) > 0, "target pod %q has no volume mounts for container %q", b.targetPodName, b.targetPodSpec.Containers[0].Name)
	debug.Assertf(len(b.targetPodSpec.Volumes) > 0, "target pod %q has no volumes with PVCs", b.targetPodName)

	for i := range b.pod.Spec.Containers {
		b.pod.Spec.Containers[i].VolumeMounts = mounts
	}
	b.pod.Spec.Volumes = b.targetPodSpec.Volumes
	return nil
}

func (b *etlBootstrapper) _setImagePullSecrets() (err error) {
	// Inherit imagePullSecrets from target pod spec
	if len(b.targetPodSpec.ImagePullSecrets) > 0 {
		b.pod.Spec.ImagePullSecrets = b.targetPodSpec.ImagePullSecrets
	}
	return nil
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

func (b *etlBootstrapper) getPodAddr() (string, error) {
	// Retrieve host IP of the pod.
	var (
		hostIP string
		err    error
	)
	if hostIP, err = b._getHost(); err != nil {
		return "", err
	}

	// Retrieve assigned port by the service.
	var nodePort int
	if nodePort, err = b._getPort(); err != nil {
		return "", err
	}

	return hostIP + ":" + strconv.Itoa(nodePort), nil
}

func (b *etlBootstrapper) createEntity(entity string) (err error) {
	switch entity {
	case k8s.Pod:
		err = b.k8sClient.Create(b.pod)
	case k8s.Svc:
		err = b.k8sClient.Create(b.svc)
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
	initTimeout, _ := b.msg.Timeouts()
	interval := cos.ProbingFrequency(initTimeout.D())
	if cmn.Rom.FastV(4, cos.SmoduleETL) {
		nlog.Infof("waiting pod %q ready (%+v, %s) initTimeout=%v ival=%v",
			b.pod.Name, b.msg.String(), b.errCtx, initTimeout, interval)
	}
	// wait
	return wait.PollUntilContextTimeout(podCtx, interval, initTimeout.D(), false, /*immediate*/
		func(context.Context) (ready bool, err error) {
			return checkPodReady(b.k8sClient, b.pod.Name)
		},
	)
}

func initComm(msg InitMsg, xid, secret string, boot *etlBootstrapper) (comm Communicator, err error) {
	if comm, _ = mgr.getByName(msg.Name()); comm != nil {
		return nil, cos.NewErrAlreadyExists(core.T, msg.Name())
	}
	if comm, err = newCommunicator(msg, secret, cmn.GCO.Get()); err != nil {
		return nil, err
	}

	if err = mgr.add(msg.Name(), comm, boot); err != nil {
		return nil, err
	}

	if err = comm.setupXaction(xid); err != nil {
		return nil, err
	}

	return comm, nil
}

func (b *etlBootstrapper) _updPodCommand() {
	if b.msg.CommType() != HpushStdin {
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
			Value: core.T.Snode().URL(cmn.NetIntraData) + apc.URLPathETLObject.Join(b.msg.Name(), b.secret),
		})
		if b.msg.ArgType() == ArgTypeFQN {
			containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
				Name:  strings.ToUpper(ArgType), // transformers expect upper case `ARG_TYPE` env var
				Value: ArgTypeFQN,
			})
		}
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  DirectPut,
			Value: strconv.FormatBool(b.msg.IsDirectPut()),
		})
		for _, v := range b.msg.GetEnv() {
			containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
				Name:  v.Name,
				Value: v.Value,
			})
		}
		// Reference: https://kubernetes.io/docs/tasks/debug/debug-application/determine-reason-pod-failure/#customizing-the-termination-message
		containers[idx].TerminationMessagePolicy = "FallbackToLogsOnError"
	}
	for idx := range b.pod.Spec.InitContainers {
		for _, v := range b.msg.GetEnv() {
			b.pod.Spec.InitContainers[idx].Env = append(b.pod.Spec.InitContainers[idx].Env, corev1.EnvVar{
				Name:  v.Name,
				Value: v.Value,
			})
		}
		b.pod.Spec.InitContainers[idx].TerminationMessagePolicy = "FallbackToLogsOnError"
	}
}

func (b *etlBootstrapper) _getHost() (string, error) {
	p, err := b.k8sClient.Pod(b.pod.Name)
	if err != nil {
		return "", err
	}
	return p.Status.HostIP, nil
}

func (b *etlBootstrapper) _getPort() (int, error) {
	s, err := b.k8sClient.Service(b.svc.Name)
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

func (b *etlBootstrapper) _getTargetPodSpec() (*corev1.PodSpec, error) {
	targetPod, err := b.k8sClient.Pod(b.targetPodName)
	if err != nil {
		return nil, fmt.Errorf("failed to get target pod %q: %w", b.targetPodName, err)
	}
	return &targetPod.Spec, nil
}
