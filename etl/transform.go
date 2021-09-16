// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl/runtime"
	"github.com/NVIDIA/aistore/xreg"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// Built-in label: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#built-in-node-labels.
	nodeNameLabel = "kubernetes.io/hostname"

	// Recommended labels: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/.
	appK8sNameLabel      = "app.kubernetes.io/name"
	appK8sComponentLabel = "app.kubernetes.io/component"

	// ETL Custom labels.
	podNameLabel = "nvidia.com/ais-etl-name"
	svcNameLabel = "nvidia.com/ais-etl-name"

	// ETL Pod's label describing which target ETL is associated with.
	podNodeLabel   = "nvidia.com/ais-etl-node"
	podTargetLabel = "nvidia.com/ais-etl-target"
)

// Definitions:
//
// ETL:
//     Refers to Extract-Transform-Load, which allows a user to do transformation
//     of the objects. Transformation is defined by an ETL spec, which is a K8S
//     yaml spec file. The operations of an ETL are executed on the ETL container.
//
// ETL container:
//     The user's K8S pod which runs the container doing the transformation of
//     the objects. It is initiated by a target and runs on the same K8S node
//     running the target.
//
// On-the-fly transformation flow:
// 1. User initiates a custom ETL workload by executing one of the documented APIs
//    and providing either the corresponding docker image or a *transforming function* -
//    a piece of code that we further run using one of the pre-built `runtimes`
//    (see https://github.com/NVIDIA/aistore/blob/master/docs/etl.md).
// 2. The API call results in deploying multiple ETL containers (K8s pods)
//    simultaneously: one container per storage target.
// 3. Each target creates a local `Communicator` instance that is based on the specified
//    `communication type`.
// 4. Client-side application (e.g., PyTorch or TensorFlow based training model)
//    starts (randomly) reading the data from a given dataset.
// 5. User-defined transformation is then performed using `Communicator.Do()`
//    on each read objects, on a per-object (or shard) basis.
// 6. Finally, the ETL container is stopped using the `Stop` API. In response,
//    each ais target in the cluster deletes its local ETL container (K8s pod).
//
// Limitations of the current implementation (soon to be removed):
//
// * No idle timeout for a ETL container. It keeps running unless explicitly
//   stopped by invoking the `Stop` API.
//
// * Delete of an ETL container is done in two stages. First we gracefully try to
//   terminate the pod with a 30s timeout. Upon failure to do so, we perform
//   a force delete.
//
// * Recreating an ETL container with the same name will delete all running
//   containers with the same name.

type (
	// Aborter listens to smap changes and aborts the ETL on the target when
	// there is any change in targets membership. Aborter should be registered
	// on ETL init. It is unregistered by Stop function. The is no
	// synchronization between aborters on different targets. It is assumed that
	// if one target received smap with changed targets membership, eventually
	// each of the targets will receive it as well. Hence, all ETL containers
	// will be stopped.
	Aborter struct {
		t           cluster.Target
		currentSmap *cluster.Smap
		uuid        string
		mtx         sync.Mutex
	}

	StartOpts struct {
		Env map[string]string
	}
)

// interface guard
var _ cluster.Slistener = (*Aborter)(nil)

func newAborter(t cluster.Target, uuid string) *Aborter {
	return &Aborter{
		uuid:        uuid,
		t:           t,
		currentSmap: t.Sowner().Get(),
	}
}

func (e *Aborter) String() string {
	return fmt.Sprintf("etl-aborter-%s", e.uuid)
}

func (e *Aborter) ListenSmapChanged() {
	// New goroutine as kubectl calls can take a lot of time,
	// making other listeners wait.
	go func() {
		e.mtx.Lock()
		defer e.mtx.Unlock()
		newSmap := e.t.Sowner().Get()

		if newSmap.Version <= e.currentSmap.Version {
			return
		}

		if !newSmap.CompareTargets(e.currentSmap) {
			glog.Warning(cmn.NewErrETL(&cmn.ETLErrorContext{
				TID:  e.t.SID(),
				UUID: e.uuid,
			}, "targets have changed, aborting..."))
			// Stop will unregister `e` from smap listeners.
			if err := Stop(e.t, e.uuid); err != nil {
				glog.Error(err.Error())
			}
		}

		e.currentSmap = newSmap
	}()
}

func Start(t cluster.Target, msg InitSpecMsg, opts ...StartOpts) (err error) {
	errCtx, podName, svcName, err := tryStart(t, msg, opts...)
	if err != nil {
		glog.Warning(cmn.NewErrETL(errCtx, "Performing cleanup after unsuccessful Start"))
		if err := cleanupEntities(errCtx, podName, svcName); err != nil {
			glog.Error(err)
		}
	}

	return err
}

func InitCode(t cluster.Target, msg InitCodeMsg) error {
	// Initialize runtime.
	r, exists := runtime.Runtimes[msg.Runtime]
	cos.Assert(exists) // Runtime should be checked in proxy during validation.

	var (
		// We clean up the `msg.ID` as K8s doesn't allow `_` and uppercase
		// letters in the names.
		name    = k8s.CleanName(msg.ID)
		podSpec = r.PodSpec()
	)

	podSpec = strings.ReplaceAll(podSpec, "<NAME>", name)
	switch msg.CommType {
	case PushCommType, RedirectCommType, RevProxyCommType:
		podSpec = strings.ReplaceAll(podSpec, "<COMMAND>", "['sh', '-c', 'python /server.py']")
		podSpec = strings.ReplaceAll(podSpec, "<PROBES>", `
      readinessProbe:
        httpGet:
          path: /health
          port: default
`)
	case IOCommType:
		podSpec = strings.ReplaceAll(podSpec, "<COMMAND>", "['python /code/code.py']")
		podSpec = strings.ReplaceAll(podSpec, "<PROBES>", "")
	default:
		cos.AssertMsg(false, msg.CommType)
	}

	// Finally, start the ETL with declared Pod specification.
	return Start(t, InitSpecMsg{
		ID:          msg.ID,
		Spec:        []byte(podSpec),
		CommType:    msg.CommType,
		WaitTimeout: msg.WaitTimeout,
	}, StartOpts{Env: map[string]string{
		r.CodeEnvName(): string(msg.Code),
		r.DepsEnvName(): string(msg.Deps),
	}})
}

// cleanupEntities removes provided entities. It tries its best to remove all
// entities so it doesn't stop when encountering an error.
func cleanupEntities(errCtx *cmn.ETLErrorContext, podName, svcName string) (err error) {
	if svcName != "" {
		if deleteErr := deleteEntity(errCtx, k8s.Svc, svcName); deleteErr != nil {
			err = deleteErr
		}
	}

	if podName != "" {
		if deleteErr := deleteEntity(errCtx, k8s.Pod, podName); deleteErr != nil {
			err = deleteErr
		}
	}

	return
}

// Returns:
// * errCtx - gathered information about ETL context
// * podName - non-empty if at least one attempt of creating pod was executed
// * svcName - non-empty if at least one attempt of creating service was executed
// * err - any error occurred which should be passed further.
func tryStart(t cluster.Target, msg InitSpecMsg, opts ...StartOpts) (errCtx *cmn.ETLErrorContext,
	podName, svcName string, err error) {
	cos.Assert(k8s.NodeName != "") // Corresponding 'if' done at the beginning of the request.

	var customEnv map[string]string
	if len(opts) > 0 {
		customEnv = opts[0].Env
	}

	errCtx = &cmn.ETLErrorContext{
		TID:  t.SID(),
		UUID: msg.ID,
	}

	b := &etlBootstraper{
		errCtx: errCtx,
		t:      t,
		msg:    msg,
		env:    customEnv,
	}

	// Parse spec template and fill Pod object with necessary fields.
	if err = b.createPodSpec(); err != nil {
		return
	}

	b.createServiceSpec()

	// 1. Cleanup previously started entities (if any).
	errCleanup := cleanupEntities(errCtx, b.pod.Name, b.svc.Name)
	debug.AssertNoErr(errCleanup)

	if msg.CommType != IOCommType {
		// 2. Creating service.
		svcName = b.svc.GetName()
		if err = b.createEntity(k8s.Svc); err != nil {
			return
		}
	}

	// 3. Creating pod.
	podName = b.pod.GetName()
	if err = b.createEntity(k8s.Pod); err != nil {
		return
	}

	if err = b.waitPodReady(); err != nil {
		return
	}

	if err = b.setupConnection(); err != nil {
		return
	}

	c := makeCommunicator(commArgs{
		listener:    newAborter(t, msg.ID),
		bootstraper: b,
	})
	// NOTE: Communicator is put to registry only if the whole tryStart was successful.
	if err = reg.put(msg.ID, c); err != nil {
		return
	}
	t.Sowner().Listeners().Reg(c)
	return
}

func (b *etlBootstraper) checkETLConnection(socketAddr string) error {
	config := cmn.GCO.Get()
	probeInterval := config.Timeout.MaxKeepalive.D()
	err := cmn.NetworkCallWithRetry(&cmn.CallWithRetryArgs{
		Call: func() (int, error) {
			conn, err := net.DialTimeout("tcp", socketAddr, probeInterval)
			if err != nil {
				return 0, err
			}
			cos.Close(conn)
			return 0, nil
		},
		SoftErr: 10,
		HardErr: 2,
		Sleep:   3 * time.Second,
		Action:  fmt.Sprintf("dial POD %q at %s", b.pod.Name, socketAddr),
	})
	if err != nil {
		return fmt.Errorf("failed to wait for ETL Service/Pod %q to respond, err: %v", b.pod.Name, err)
	}
	return nil
}

func (b *etlBootstraper) setupConnection() (err error) {
	// Retrieve host IP of the pod.
	var hostIP string
	if hostIP, err = b.getPodHostIP(); err != nil {
		return
	}

	if b.msg.CommType != IOCommType {
		// Retrieve assigned port by the service.
		var nodePort uint
		if nodePort, err = b.getServiceNodePort(); err != nil {
			return
		}

		// Make sure we can access the pod via TCP socket address to ensure that
		// it is accessible from target.
		etlSocketAddr := fmt.Sprintf("%s:%d", hostIP, nodePort)
		if err = b.checkETLConnection(etlSocketAddr); err != nil {
			err = cmn.NewErrETL(b.errCtx, err.Error())
			return
		}

		b.uri = "http://" + etlSocketAddr
	}

	return nil
}

func (b *etlBootstraper) createServiceSpec() {
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

	b.updateServiceLabels()

	b.errCtx.SvcName = b.svc.Name
}

// Stop deletes all occupied by the ETL resources, including Pods and Services.
// It unregisters ETL smap listener.
func Stop(t cluster.Target, id string) error {
	errCtx := &cmn.ETLErrorContext{
		TID:  t.SID(),
		UUID: id,
	}

	// Abort any running offline ETLs.
	xreg.AbortAll(cmn.ActETLBck)

	c, err := GetCommunicator(id, t.Snode())
	if err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}
	errCtx.PodName = c.PodName()
	errCtx.SvcName = c.SvcName()

	if err := cleanupEntities(errCtx, c.PodName(), c.SvcName()); err != nil {
		return err
	}

	if c := reg.removeByUUID(id); c != nil {
		t.Sowner().Listeners().Unreg(c)
	}

	return nil
}

// StopAll deletes all running ETLs.
func StopAll(t cluster.Target) {
	// Skip if K8s isn't even available.
	if k8s.Detect() != nil {
		return
	}

	for _, e := range List() {
		if err := Stop(t, e.ID); err != nil {
			glog.Error(err)
		}
	}
}

func GetCommunicator(transformID string, lsnode *cluster.Snode) (Communicator, error) {
	c, exists := reg.getByUUID(transformID)
	if !exists {
		return nil, cmn.NewErrNotFound("%s: ETL %q", lsnode, transformID)
	}
	return c, nil
}

func List() []Info { return reg.list() }

func PodLogs(t cluster.Target, transformID string) (logs PodLogsMsg, err error) {
	c, err := GetCommunicator(transformID, t.Snode())
	if err != nil {
		return logs, err
	}
	client, err := k8s.GetClient()
	if err != nil {
		return logs, err
	}
	b, err := client.Logs(c.PodName())
	if err != nil {
		return logs, err
	}
	return PodLogsMsg{
		TargetID: t.SID(),
		Logs:     b,
	}, nil
}

func PodHealth(t cluster.Target, etlID string) (stats *PodHealthMsg, err error) {
	var (
		c      Communicator
		client k8s.Client
	)
	if c, err = GetCommunicator(etlID, t.Snode()); err != nil {
		return
	}
	if client, err = k8s.GetClient(); err != nil {
		return
	}

	cpuUsed, memUsed, err := client.Health(c.PodName())
	if err != nil {
		if metricsErr := client.CheckMetricsAvailability(); metricsErr != nil {
			err = fmt.Errorf("%v; failed to fetch metrics from Kubernetes: %v", metricsErr, err)
		}
		return nil, err
	}
	return &PodHealthMsg{
		TargetID: t.SID(),
		CPU:      cpuUsed,
		Mem:      memUsed,
	}, nil
}

// Sets pods node affinity, so pod will be scheduled on the same node as a target creating it.
func (b *etlBootstraper) setTransformAffinity() error {
	if b.pod.Spec.Affinity == nil {
		b.pod.Spec.Affinity = &corev1.Affinity{}
	}
	if b.pod.Spec.Affinity.NodeAffinity == nil {
		b.pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	reqAffinity := b.pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAffinity := b.pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	if reqAffinity != nil && len(reqAffinity.NodeSelectorTerms) > 0 || len(prefAffinity) > 0 {
		return cmn.NewErrETL(b.errCtx, "error in YAML spec, pod should not have any NodeAffinities defined")
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
func (b *etlBootstraper) setTransformAntiAffinity() error {
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

func (b *etlBootstraper) updatePodLabels() {
	if b.pod.Labels == nil {
		b.pod.Labels = make(map[string]string, 6)
	}

	b.pod.Labels[appLabel] = "ais"
	b.pod.Labels[podNameLabel] = b.pod.GetName()
	b.pod.Labels[podNodeLabel] = k8s.NodeName
	b.pod.Labels[podTargetLabel] = b.t.SID()
	b.pod.Labels[appK8sNameLabel] = "etl"
	b.pod.Labels[appK8sComponentLabel] = "server"
}

func (b *etlBootstraper) updateServiceLabels() {
	if b.svc.Labels == nil {
		b.svc.Labels = make(map[string]string, 4)
	}

	b.svc.Labels[appLabel] = "ais"
	b.svc.Labels[svcNameLabel] = b.svc.GetName()
	b.svc.Labels[appK8sNameLabel] = "etl"
	b.svc.Labels[appK8sComponentLabel] = "server"
}

func (b *etlBootstraper) updateReadinessProbe() {
	probe := b.pod.Spec.Containers[0].ReadinessProbe
	if b.msg.CommType == IOCommType {
		return
	}

	// If someone already set these values, we don't to touch them.
	if probe.TimeoutSeconds != 0 || probe.PeriodSeconds != 0 {
		return
	}

	// Set default values.
	probe.TimeoutSeconds = 5
	probe.PeriodSeconds = 10
}

// Sets environment variables that can be accessed inside the container.
func (b *etlBootstraper) setPodEnvVariables() {
	containers := b.pod.Spec.Containers
	debug.Assert(len(containers) > 0)
	for idx := range containers {
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  "AIS_TARGET_URL",
			Value: b.t.Snode().URL(cmn.NetworkPublic) + cmn.URLPathETLObject.Join(reqSecret),
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

// waitPodReady waits until ETL Pod becomes `Ready`. This happens
// only after the Pod's containers will have started and the Pod's `readinessProbe`
// request (made by the Kubernetes itself) returns OK. If the Pod doesn't have
// `readinessProbe` config specified the last step gets skipped.
// NOTE: However, currently, we do require readinessProbe config in the ETL spec.
func (b *etlBootstraper) waitPodReady() error {
	client, err := k8s.GetClient()
	if err != nil {
		return cmn.NewErrETL(b.errCtx, "%v", err)
	}

	err = wait.PollImmediate(time.Second, time.Duration(b.msg.WaitTimeout), func() (ready bool, err error) {
		ready, err = checkPodReady(client, b.pod.Name)
		return ready, err
	})
	if err != nil {
		pod, _ := client.Pod(b.pod.Name)
		if pod == nil {
			return cmn.NewErrETL(b.errCtx, "%v", err)
		}

		return cmn.NewErrETL(b.errCtx,
			`%v (pod phase: %q, pod conditions: %s; expected condition: %s)`,
			err, pod.Status.Phase, podConditionsToString(pod.Status.Conditions),
			podConditionToString(corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}),
		)
	}
	return nil
}

// Pod conditions include enumerated lifecycle states, such as `PodScheduled`,
// `ContainersReady`, `Initialized`, `Ready`
// (see https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle).
// First, we check that the Pod is still running (neither succeeded, nor failed),
// and secondly, whether it contains `Ready` condition.
func checkPodReady(client k8s.Client, podName string) (ready bool, err error) {
	var p *corev1.Pod
	if p, err = client.Pod(podName); err != nil {
		return false, err
	}

	// Pod has run to completion, either by failing or by succeeding. We don't
	// expect any of these to happen, as ETL containers are supposed to constantly
	// listen to upcoming requests and never terminate.
	switch p.Status.Phase {
	case corev1.PodFailed, corev1.PodSucceeded:
		return false, fmt.Errorf(
			"pod ran to completion (phase: %s), state message: %q",
			p.Status.Phase, p.Status.Message,
		)
	}

	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true, nil
		}
	}

	return false, nil
}

func (b *etlBootstraper) getPodHostIP() (string, error) {
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

func deleteEntity(errCtx *cmn.ETLErrorContext, entityType, entityName string) error {
	client, err := k8s.GetClient()
	if err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}

	// Remove entity immediately (ignoring not found).
	if err = client.Delete(entityType, entityName); err != nil {
		if k8sErrors.IsNotFound(err) {
			return nil
		}
		return cmn.NewErrETL(errCtx, err.Error())
	}

	err = wait.PollImmediate(time.Second, time.Minute, func() (done bool, err error) {
		exists, err := client.CheckExists(entityType, entityName)
		if err != nil {
			return false, err
		}
		return !exists, nil
	})
	if err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}
	return nil
}

func (b *etlBootstraper) createEntity(entity string) error {
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
		panic(entity)
	}

	if err != nil {
		return cmn.NewErrETL(b.errCtx, "failed to create %s (err: %v)", entity, err)
	}
	return nil
}

func (b *etlBootstraper) getServiceNodePort() (uint, error) {
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
	return uint(port), nil
}

func podConditionsToString(conditions []corev1.PodCondition) string {
	parts := make([]string, 0, len(conditions))
	for _, cond := range conditions {
		parts = append(parts, podConditionToString(cond))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func podConditionToString(cond corev1.PodCondition) string {
	parts := []string{
		fmt.Sprintf("type: %q", cond.Type),
		fmt.Sprintf("status: %q", cond.Status),
	}
	if cond.Reason != "" {
		parts = append(parts, fmt.Sprintf("reason: %q", cond.Reason))
	}
	if cond.Message != "" {
		parts = append(parts, fmt.Sprintf("msg: %q", cond.Message))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
