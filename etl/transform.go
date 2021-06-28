// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/xaction/xreg"
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
// * Delete of a ETL container is done in two stages. First we gracefully try to
//   terminate the pod with a 30s timeout. Upon failure to do so, we perform
//   a force delete.
//
// * A single ETL container runs per target at any point of time.
//
// * Recreating a ETL container with the same name will delete all running
//   containers with the same name.

type (
	// Aborter listens to smap changes and aborts the ETL on the target when
	// there is any change in targets membership. Aborter should be registered
	// on ETL init. It is unregistered by Stop function. The is no
	// synchronization between aborters on different targets. It is assumed that
	// if one target received smap with changed targets membership, eventually
	// each of the targets will receive it as well. Hence all ETL containers
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
			glog.Warning(cmn.NewETLError(&cmn.ETLErrorContext{
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

func Start(t cluster.Target, msg InitMsg, opts ...StartOpts) (err error) {
	errCtx, podName, svcName, err := tryStart(t, msg, opts...)
	if err != nil {
		glog.Warning(cmn.NewETLError(errCtx, "Performing cleanup after unsuccessful Start"))
		if err := cleanupEntities(errCtx, podName, svcName); err != nil {
			glog.Error(err)
		}
	}

	return err
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
func tryStart(t cluster.Target, msg InitMsg, opts ...StartOpts) (errCtx *cmn.ETLErrorContext,
	podName, svcName string, err error) {
	cos.Assert(k8s.NodeName != "") // Corresponding 'if' done at the beginning of the request.
	var (
		pod             *corev1.Pod
		svc             *corev1.Service
		hostIP          string
		originalPodName string
		nodePort        uint

		customEnv map[string]string
	)

	if len(opts) > 0 {
		customEnv = opts[0].Env
	}

	// Make sure only one ETL is running.
	if len(List()) > 0 {
		err = cmn.ErrETLOnlyOne
		return
	}

	errCtx = &cmn.ETLErrorContext{
		TID:  t.SID(),
		UUID: msg.ID,
	}

	// Parse spec template and fill Pod object with necessary fields.
	if pod, originalPodName, err = createPodSpec(errCtx, t, msg.Spec, customEnv); err != nil {
		return
	}

	svc = createServiceSpec(pod)
	errCtx.SvcName = svc.Name

	// 1. Cleanup previously started entities (if any).
	errCleanup := cleanupEntities(errCtx, pod.Name, svc.Name)
	debug.AssertNoErr(errCleanup)

	// 2. Creating service.
	svcName = svc.GetName()
	if err = createEntity(errCtx, k8s.Svc, svc); err != nil {
		return
	}

	// 3. Creating pod.
	podName = pod.GetName()
	if err = createEntity(errCtx, k8s.Pod, pod); err != nil {
		return
	}

	if err = waitPodReady(errCtx, pod, msg.WaitTimeout); err != nil {
		return
	}

	// Retrieve host IP of the pod.
	if hostIP, err = getPodHostIP(errCtx, pod); err != nil {
		return
	}

	// Retrieve assigned port by the service.
	if nodePort, err = getServiceNodePort(errCtx, svc); err != nil {
		return
	}

	// Make sure we can access the pod via TCP socket address to ensure that
	// it is accessible from target.
	etlSocketAddr := fmt.Sprintf("%s:%d", hostIP, nodePort)
	if err = checkETLConnection(etlSocketAddr, pod.GetName()); err != nil {
		err = cmn.NewETLError(errCtx, err.Error())
		return
	}

	c := makeCommunicator(commArgs{
		listener:       newAborter(t, msg.ID),
		t:              t,
		pod:            pod,
		name:           originalPodName,
		commType:       msg.CommType,
		transformerURL: "http://" + etlSocketAddr,
	})
	// NOTE: Communicator is put to registry only if the whole tryStart was successful.
	if err = reg.put(msg.ID, c); err != nil {
		return
	}
	t.Sowner().Listeners().Reg(c)
	return
}

func checkETLConnection(socketAddr, podName string) error {
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
		Action:  fmt.Sprintf("dial POD %q at %s", podName, socketAddr),
	})
	if err != nil {
		return fmt.Errorf("failed to wait for ETL Service/Pod %q to respond, err: %v", podName, err)
	}
	return nil
}

func createServiceSpec(pod *corev1.Pod) *corev1.Service {
	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.GetName(),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Port: pod.Spec.Containers[0].Ports[0].ContainerPort},
			},
			Selector: map[string]string{
				podNameLabel: pod.Labels[podNameLabel],
				appLabel:     pod.Labels[appLabel],
			},
			Type: corev1.ServiceTypeNodePort,
		},
	}
	updateServiceLabels(svc)
	return svc
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
		return cmn.NewETLError(errCtx, err.Error())
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
		return nil, cmn.NewNotFoundError("%s: ETL %q", lsnode, transformID)
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
func setTransformAffinity(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) error {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	reqAffinity := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAffinity := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	if reqAffinity != nil && len(reqAffinity.NodeSelectorTerms) > 0 || len(prefAffinity) > 0 {
		return cmn.NewETLError(errCtx, "error in YAML spec, pod should not have any NodeAffinities defined")
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
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
	return nil
}

// Sets pods node anti-affinity, so no two pods with the matching criteria is scheduled on the same node
// at the same time.
func setTransformAntiAffinity(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) error {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.PodAntiAffinity == nil {
		pod.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}

	reqAntiAffinities := pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAntiAffinity := pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	if len(reqAntiAffinities) > 0 || len(prefAntiAffinity) > 0 {
		return cmn.NewETLError(errCtx, "error in YAML spec, pod should not have any NodeAntiAffinities defined")
	}

	// Don't create anti-affinity limitation, to be able to run multiple ETL pods on a single machine.
	// NOTE: This change allows deploying multiple different ETLs at the same time.
	if !k8s.AllowOneNodeManyETLs {
		reqAntiAffinities = []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					podNodeLabel: k8s.NodeName,
				},
			},
			TopologyKey: nodeNameLabel,
		}}
		pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = reqAntiAffinities
	}

	return nil
}

func updatePodLabels(t cluster.Target, pod *corev1.Pod) {
	if pod.Labels == nil {
		pod.Labels = make(map[string]string, 6)
	}

	pod.Labels[appLabel] = "ais"
	pod.Labels[podNameLabel] = pod.GetName()
	pod.Labels[podNodeLabel] = k8s.NodeName
	pod.Labels[podTargetLabel] = t.SID()
	pod.Labels[appK8sNameLabel] = "etl"
	pod.Labels[appK8sComponentLabel] = "server"
}

func updateServiceLabels(svc *corev1.Service) {
	if svc.Labels == nil {
		svc.Labels = make(map[string]string, 4)
	}

	svc.Labels[appLabel] = "ais"
	svc.Labels[svcNameLabel] = svc.GetName()
	svc.Labels[appK8sNameLabel] = "etl"
	svc.Labels[appK8sComponentLabel] = "server"
}

func updateReadinessProbe(pod *corev1.Pod) {
	probe := pod.Spec.Containers[0].ReadinessProbe

	// If someone already set these values, we don't to touch them.
	if probe.TimeoutSeconds != 0 || probe.PeriodSeconds != 0 {
		return
	}

	// Set default values.
	probe.TimeoutSeconds = 5
	probe.PeriodSeconds = 10
}

// Sets environment variables that can be accessed inside the container.
func setPodEnvVariables(t cluster.Target, pod *corev1.Pod, customEnv map[string]string) {
	containers := pod.Spec.Containers
	debug.Assert(len(containers) > 0)
	for idx := range containers {
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  "AIS_TARGET_URL",
			Value: t.Snode().URL(cmn.NetworkPublic) + cmn.URLPathETLObject.Join(reqSecret),
		})
		for k, v := range customEnv {
			containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
				Name:  k,
				Value: v,
			})
		}
	}

	for idx := range pod.Spec.InitContainers {
		for k, v := range customEnv {
			pod.Spec.InitContainers[idx].Env = append(pod.Spec.InitContainers[idx].Env, corev1.EnvVar{
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
func waitPodReady(errCtx *cmn.ETLErrorContext, pod *corev1.Pod, waitTimeout cos.Duration) error {
	var (
		condition   *corev1.PodCondition
		client, err = k8s.GetClient()
	)
	if err != nil {
		return cmn.NewETLError(errCtx, "%v", err)
	}

	err = wait.PollImmediate(time.Second, time.Duration(waitTimeout), func() (ready bool, err error) {
		ready, condition, err = checkPodReady(client, pod.Name)
		return ready, err
	})
	if err != nil {
		if condition == nil {
			return cmn.NewETLError(errCtx, "%v", err)
		}
		conditionStr := fmt.Sprintf("%s, reason: %s, msg: %s", condition.Type, condition.Reason, condition.Message)
		return cmn.NewETLError(errCtx, "%v (pod condition: %s; expected status Ready)",
			err, conditionStr)
	}
	return nil
}

// Pod conditions include enumerated lifecycle states, such as `PodScheduled`,
// `ContainersReady`, `Initialized`, `Ready`
// (see https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle).
// First, we check that the Pod is still running (neither succeeded, nor failed),
// and secondly, whether the last (chronologically) transition is `Ready`.
func checkPodReady(client k8s.Client, podName string) (ready bool, latestCond *corev1.PodCondition, err error) {
	var p *corev1.Pod
	if p, err = client.Pod(podName); err != nil {
		return false, nil, err
	}

	// Pod has run to completion, either by failing or by succeeding. We don't
	// expect any of these to happen, as ETL containers are supposed to constantly
	// listen to upcoming requests and never terminate.
	switch p.Status.Phase {
	case corev1.PodFailed, corev1.PodSucceeded:
		err = fmt.Errorf("pod ran to completion (phase: %s), state message: %q",
			p.Status.Phase, p.Status.Message)
		if cond, exists := latestCondition(p.Status.Conditions); exists {
			return false, &cond, err
		}
		return false, nil, err
	}

	if cond, exists := latestCondition(p.Status.Conditions); exists {
		return cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue, &cond, nil
	}
	return false, nil, nil
}

func getPodHostIP(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	client, err := k8s.GetClient()
	if err != nil {
		return "", cmn.NewETLError(errCtx, err.Error())
	}
	p, err := client.Pod(pod.GetName())
	if err != nil {
		return "", err
	}
	return p.Status.HostIP, nil
}

func deleteEntity(errCtx *cmn.ETLErrorContext, entityType, entityName string) error {
	client, err := k8s.GetClient()
	if err != nil {
		return cmn.NewETLError(errCtx, err.Error())
	}

	// Remove entity immediately (ignoring not found).
	if err = client.Delete(entityType, entityName); err != nil {
		if k8sErrors.IsNotFound(err) {
			return nil
		}
		return cmn.NewETLError(errCtx, err.Error())
	}

	err = wait.PollImmediate(time.Second, time.Minute, func() (done bool, err error) {
		exists, err := client.CheckExists(entityType, entityName)
		if err != nil {
			return false, err
		}
		return !exists, nil
	})
	if err != nil {
		return cmn.NewETLError(errCtx, err.Error())
	}
	return nil
}

func createEntity(errCtx *cmn.ETLErrorContext, entity string, spec interface{}) error {
	client, err := k8s.GetClient()
	if err != nil {
		return err
	}
	if err = client.Create(spec); err != nil {
		return cmn.NewETLError(errCtx, "failed to create %s (err: %v)", entity, err)
	}
	return nil
}

func getServiceNodePort(errCtx *cmn.ETLErrorContext, svc *corev1.Service) (uint, error) {
	client, err := k8s.GetClient()
	if err != nil {
		return 0, cmn.NewETLError(errCtx, err.Error())
	}

	s, err := client.Service(svc.GetName())
	if err != nil {
		return 0, cmn.NewETLError(errCtx, err.Error())
	}

	nodePort := int(s.Spec.Ports[0].NodePort)
	port, err := cmn.ValidatePort(nodePort)
	if err != nil {
		return 0, cmn.NewETLError(errCtx, err.Error())
	}
	return uint(port), nil
}

func latestCondition(conditions []corev1.PodCondition) (latestCondition corev1.PodCondition, exists bool) {
	if len(conditions) == 0 {
		return latestCondition, false
	}

	latestCondition = conditions[0]
	for _, c := range conditions[1:] {
		if c.LastTransitionTime.After(latestCondition.LastTransitionTime.Time) {
			latestCondition = c
		}
	}
	return latestCondition, true
}
