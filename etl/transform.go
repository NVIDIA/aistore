// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"bytes"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// built-in label https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#built-in-node-labels
	nodeNameLabel = "kubernetes.io/hostname"
	targetNode    = "target_node"

	tfProbeRetries = 5
)

var (
	tfProbeClient = &http.Client{}
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
// Flow:
// 1. User initiates an ETL container, using the `Start` method (via proxy).
// 2. The ETL container starts on the same node as the target. It creates
//    `Communicator` instance based on provided communication type.
// 3. The transformation is done using `Communicator.Do()`
// 4. The ETL container is stopped using `Stop` (via proxy). All targets delete
//    their ETL containers (K8s pods).
//
// Limitations of the current implementation (soon to be removed):
//
// * No idle timeout for a ETL container. It keeps running unless explicitly
//   stopped by invoking the `Stop` API.
//
// * `kubectl delete` of a ETL container is done in two stages. First we
//    gracefully try to terminate the pod with a 30s timeout. Upon failure to do
//    so, we perform a force delete.
//
// * A single ETL container runs per target at any point of time.
//
// * Recreating a ETL container with the same name will delete all running
//   containers with the same name.
//
// * TODO: replace `kubectl` calls with proper go-sdk calls.

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
		ConfigMapName string
	}
)

var _ cluster.Slistener = &Aborter{}

func newAborter(t cluster.Target, uuid string) *Aborter {
	return &Aborter{
		uuid:        uuid,
		t:           t,
		currentSmap: t.GetSowner().Get(),
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
		newSmap := e.t.GetSowner().Get()

		if newSmap.Version <= e.currentSmap.Version {
			return
		}

		if !newSmap.CompareTargets(e.currentSmap) {
			glog.Warning(cmn.NewETLError(&cmn.ETLErrorContext{
				TID:  e.t.Snode().DaemonID,
				UUID: e.uuid,
			}, "targets have changed, aborting..."))
			// Stop will unregister e from smap listeners.
			if err := Stop(e.t, e.uuid); err != nil {
				glog.Error(err.Error())
			}
		}

		e.currentSmap = newSmap
	}()
}

func Start(t cluster.Target, msg InitMsg, opts ...StartOpts) (err error) {
	errCtx, podName, svcName, configMapName, err := tryStart(t, msg, opts...)
	if err != nil {
		glog.Warning(cmn.NewETLError(errCtx, "Doing cleanup after unsuccessful Start"))
		cleanupStart(errCtx, podName, svcName, configMapName)
	}

	return err
}

// Should be called if tryStart was unsuccessful.
// It cleans up any possibly created resources.
func cleanupStart(errCtx *cmn.ETLErrorContext, podName, svcName, configMapName string) {
	if err := cleanupEntities(errCtx, podName, svcName, configMapName); err != nil {
		glog.Error(err)
	}
}

// cleanupEntities removes provided entities. It tries its best to remove all
// entities so it doesn't stop when encountering an error.
func cleanupEntities(errCtx *cmn.ETLErrorContext, podName, svcName, configMapName string) (err error) {
	if svcName != "" {
		if deleteErr := deleteEntity(errCtx, cmn.KubeSvc, svcName); deleteErr != nil {
			err = deleteErr
		}
	}

	if configMapName != "" {
		if deleteErr := deleteEntity(errCtx, cmn.KubeConfigMap, configMapName); deleteErr != nil {
			err = deleteErr
		}
	}

	if podName != "" {
		if deleteErr := deleteEntity(errCtx, cmn.KubePod, podName); deleteErr != nil {
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
func tryStart(t cluster.Target, msg InitMsg, opts ...StartOpts) (errCtx *cmn.ETLErrorContext, podName, svcName, configMapName string, err error) {
	var (
		pod             *corev1.Pod
		svc             *corev1.Service
		hostIP          string
		podIP           string
		originalPodName string
		nodePort        uint
	)

	if len(opts) > 0 {
		configMapName = opts[0].ConfigMapName
	}

	errCtx = &cmn.ETLErrorContext{
		TID:           t.Snode().DaemonID,
		UUID:          msg.ID,
		ConfigMapName: configMapName,
	}

	cmn.Assert(t.K8sNodeName() != "") // Corresponding 'if' done at the beginning of the request.
	// Parse spec template.
	if pod, err = ParsePodSpec(errCtx, msg.Spec); err != nil {
		return
	}
	errCtx.ETLName = pod.GetName()
	originalPodName = pod.GetName()

	// Override the name (add target's daemon ID and node ID to its name).
	pod.SetName(pod.GetName() + "-" + t.Snode().DaemonID + "-" + t.K8sNodeName())
	errCtx.PodName = pod.GetName()
	if pod.Labels == nil {
		pod.Labels = make(map[string]string, 1)
	}
	pod.Labels[targetNode] = t.K8sNodeName()

	// Create service spec
	svc = createServiceSpec(pod)
	errCtx.SvcName = svc.Name

	// The following combination of Affinity and Anti-Affinity allows one to
	// achieve the following:
	//  1. The ETL container is always scheduled on the target invoking it.
	//  2. Not more than one ETL container with the same target, is scheduled on
	//     the same node, at a given point of time.
	if err = setTransformAffinity(errCtx, t, pod); err != nil {
		return
	}

	if err = setTransformAntiAffinity(errCtx, t, pod); err != nil {
		return
	}

	setPodEnvVariables(pod, t)

	// 1. Doing cleanup of any pre-existing entities.
	// NOTE: `configMapName` is empty because it was already created and we
	//  definitely don't want to clean it up here.
	cleanupStart(errCtx, pod.Name, svc.Name, "")

	// 2. Creating pod.
	podName = pod.GetName()
	if err = createEntity(errCtx, cmn.KubePod, pod); err != nil {
		return
	}

	if err = waitPodReady(errCtx, pod, msg.WaitTimeout); err != nil {
		return
	}

	// Retrieve host IP of the pod.
	if hostIP, err = getPodHostIP(errCtx, pod); err != nil {
		return
	}

	// Retrieve pod IP.
	if podIP, err = getPodIP(errCtx, pod); err != nil {
		return
	}

	// 3. Creating service.
	svcName = svc.GetName()
	if err = createEntity(errCtx, cmn.KubeSvc, svc); err != nil {
		return
	}

	if nodePort, err = getServiceNodePort(errCtx, svc); err != nil {
		return
	}

	transformerURL := fmt.Sprintf("http://%s:%d", hostIP, nodePort)

	// TODO: Temporary workaround. Debug this further to find the root cause.
	//  (not waiting sometimes causes the first `Do()` to fail).
	readinessPath := pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Path
	if waitErr := waitTransformerReady(transformerURL, readinessPath); waitErr != nil {
		err = cmn.NewETLError(errCtx, waitErr.Error())
		return
	}

	c := makeCommunicator(commArgs{
		listener:       newAborter(t, msg.ID),
		t:              t,
		pod:            pod,
		commType:       msg.CommType,
		podIP:          podIP,
		transformerURL: transformerURL,
		name:           originalPodName,
		configMapName:  configMapName,
	})
	// NOTE: communicator is put to registry only if the whole tryStart was successful.
	if err = reg.put(msg.ID, c); err != nil {
		return
	}
	t.GetSowner().Listeners().Reg(c)
	return
}

func waitTransformerReady(url, path string) (err error) {
	var resp *http.Response
	tfProbeSleep := cmn.GCO.Get().Timeout.MaxKeepalive
	tfProbeClient.Timeout = tfProbeSleep
	for i := 0; i < tfProbeRetries; i++ {
		resp, err = tfProbeClient.Get(cmn.JoinPath(url, path))
		if err != nil {
			glog.Errorf("failing to GET %s, err: %v, cnt: %d", cmn.JoinPath(url, path), err, i+1)
			if cmn.IsReqCanceled(err) || cmn.IsErrConnectionRefused(err) {
				time.Sleep(tfProbeSleep)
				continue
			}
			return
		}
		err = cmn.DrainReader(resp.Body)
		resp.Body.Close()
		break
	}
	return
}

func createServiceSpec(pod *corev1.Pod) *corev1.Service {
	return &corev1.Service{
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
				"app": pod.Labels["app"],
			},
			Type: corev1.ServiceTypeNodePort,
		},
	}
}

// Stop deletes all occupied by the ETL resources, including Pods and Services.
// It unregisters ETL smap listener.
func Stop(t cluster.Target, id string) error {
	var (
		errCtx = &cmn.ETLErrorContext{
			TID:  t.Snode().DaemonID,
			UUID: id,
		}
	)

	// Abort any running offline ETLs.
	t.GetXactRegistry().AbortAll(cmn.ActETLBucket)

	c, err := GetCommunicator(id)
	if err != nil {
		return cmn.NewETLError(errCtx, err.Error())
	}
	errCtx.PodName = c.PodName()
	errCtx.SvcName = c.SvcName()
	errCtx.ConfigMapName = c.ConfigMapName()

	if err := cleanupEntities(errCtx, c.PodName(), c.SvcName(), c.ConfigMapName()); err != nil {
		return err
	}

	if c := reg.removeByUUID(id); c != nil {
		t.GetSowner().Listeners().Unreg(c)
	}

	return nil
}

func GetCommunicator(transformID string) (Communicator, error) {
	c, exists := reg.getByUUID(transformID)
	if !exists {
		return nil, cmn.NewNotFoundError("ETL with %q id", transformID)
	}
	return c, nil
}

func List() []Info { return reg.list() }

// Sets pods node affinity, so pod will be scheduled on the same node as a target creating it.
func setTransformAffinity(errCtx *cmn.ETLErrorContext, t cluster.Target, pod *corev1.Pod) error {
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
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      nodeNameLabel,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{t.K8sNodeName()},
			}}},
		},
	}
	// TODO: RequiredDuringSchedulingIgnoredDuringExecution means that ETL container
	//  will be placed on the same machine as target which creates it. However,
	//  if 2 targets went down and up again at the same time, they may switch nodes,
	//  leaving ETL containers running on the wrong machines.
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
	return nil
}

// Sets pods node anti-affinity, so no two pods with the matching criteria is scheduled on the same node
// at the same time.
func setTransformAntiAffinity(errCtx *cmn.ETLErrorContext, t cluster.Target, pod *corev1.Pod) error {
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

	reqAntiAffinities = []corev1.PodAffinityTerm{{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				targetNode: t.K8sNodeName(),
			},
		},
		TopologyKey: nodeNameLabel,
	}}
	pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = reqAntiAffinities
	return nil
}

// Sets environment variables that can be accessed inside the container.
func setPodEnvVariables(pod *corev1.Pod, t cluster.Target) {
	containers := pod.Spec.Containers
	debug.Assert(len(containers) > 0)
	for idx := range containers {
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  "AIS_TARGET_URL",
			Value: t.Snode().URL(cmn.NetworkPublic),
		})
	}
}

// waitPodReady waits until Kubernetes marks a pod's state READY. This happens
// only after the pod's containers have started and the pods readinessProbe
// request (made by kubernetes itself) is successful. If pod doesn't have
// readinessProbe config specified the last step is skipped. Currently
// readinessProbe config is required by us in ETL spec.
func waitPodReady(errCtx *cmn.ETLErrorContext, pod *corev1.Pod, waitTimeout cmn.DurationJSON) error {
	args := []string{"wait"}
	if !waitTimeout.IsZero() {
		args = append(args, "--timeout", waitTimeout.String())
	}
	args = append(args, "--for", "condition=ready", "pod", pod.GetName())
	cmd := exec.Command(cmn.Kubectl, args...)
	if b, err := cmd.CombinedOutput(); err != nil {
		return cmn.NewETLError(errCtx, "failed waiting for pod to get ready (err: %v; out: %s)", err, string(b))
	}
	return nil
}

func getPodIP(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	// Retrieve host IP of the pod.
	output, err := exec.Command(cmn.Kubectl, []string{"get", "pod", pod.GetName(), "--template={{.status.podIP}}"}...).CombinedOutput()
	if err != nil {
		return "", cmn.NewETLError(errCtx, "failed to get IP of pod (err: %v; output: %s)", err, string(output))
	}
	return string(output), nil
}

func getPodHostIP(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	// Retrieve host IP of the pod.
	output, err := exec.Command(cmn.Kubectl, []string{"get", "pod", pod.GetName(), "--template={{.status.hostIP}}"}...).CombinedOutput()
	if err != nil {
		return "", cmn.NewETLError(errCtx, "failed to get host IP of pod (err: %v; output: %s)", err, string(output))
	}
	return string(output), nil
}

func deleteEntity(errCtx *cmn.ETLErrorContext, entity, entityName string) error {
	args := []string{"delete", entity, entityName, "--ignore-not-found"}

	// Doing graceful delete
	output, err := exec.Command(cmn.Kubectl, args...).CombinedOutput()
	if err == nil {
		return nil
	}

	etlErr := cmn.NewETLError(errCtx, "failed to delete %s, err: %v, out: %s. Retrying with --force", entity, err, string(output))
	glog.Errorf(etlErr.Error())

	// Doing force delete
	args = append(args, "--force")
	if output, err := exec.Command(cmn.Kubectl, args...).CombinedOutput(); err != nil {
		return cmn.NewETLError(errCtx, "force delete failed. %q %s, err: %v, out: %s",
			entity, entityName, err, string(output))
	}
	return nil
}

func createEntity(errCtx *cmn.ETLErrorContext, entity string, spec interface{}) error {
	var (
		b    = cmn.MustMarshal(spec)
		args = []string{"create", "-f", "-"}
		cmd  = exec.Command(cmn.Kubectl, args...)
	)

	cmd.Stdin = bytes.NewBuffer(b)
	if b, err := cmd.CombinedOutput(); err != nil {
		return cmn.NewETLError(errCtx, "failed to create %s (err: %v; output: %s)", entity, err, string(b))
	}
	return nil
}

func getServiceNodePort(errCtx *cmn.ETLErrorContext, svc *corev1.Service) (uint, error) {
	output, err := exec.Command(cmn.Kubectl, []string{"get", "-o", "jsonpath=\"{.spec.ports[0].nodePort}\"", "svc", svc.GetName()}...).CombinedOutput()
	if err != nil {
		return 0, cmn.NewETLError(errCtx, "failed to get nodePort for service %q (err: %v; output: %s)", svc.GetName(), err, string(output))
	}
	outputStr, _ := strconv.Unquote(string(output))
	nodePort, err := strconv.Atoi(outputStr)
	if err != nil {
		return 0, cmn.NewETLError(errCtx, "failed to parse nodePort for pod-svc %q (err: %v; output: %s)", svc.GetName(), err, string(output))
	}

	port, err := cmn.ValidatePort(nodePort)
	if err != nil {
		return 0, cmn.NewETLError(errCtx, err.Error())
	}
	return uint(port), nil
}
