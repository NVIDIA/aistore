// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
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

var _ cluster.Slistener = &Aborter{}

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
	errCtx, podName, svcName, err := tryStart(t, msg, opts...)
	if err != nil {
		glog.Warning(cmn.NewETLError(errCtx, "Doing cleanup after unsuccessful Start"))
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
		if deleteErr := deleteEntity(errCtx, cmn.KubeSvc, svcName); deleteErr != nil {
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
func tryStart(t cluster.Target, msg InitMsg, opts ...StartOpts) (errCtx *cmn.ETLErrorContext, podName, svcName string, err error) {
	var (
		pod             *corev1.Pod
		svc             *corev1.Service
		hostIP          string
		podIP           string
		originalPodName string
		nodePort        uint

		customEnv map[string]string
	)

	if len(opts) > 0 {
		customEnv = opts[0].Env
	}

	errCtx = &cmn.ETLErrorContext{
		TID:  t.Snode().DaemonID,
		UUID: msg.ID,
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

	setPodEnvVariables(t, pod, customEnv)

	// 1. Doing cleanup of any pre-existing entities.
	cleanupEntities(errCtx, pod.Name, svc.Name)

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
	})
	// NOTE: communicator is put to registry only if the whole tryStart was successful.
	if err = reg.put(msg.ID, c); err != nil {
		return
	}
	t.Sowner().Listeners().Reg(c)
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
	t.XactRegistry().AbortAll(cmn.ActETLBucket)

	c, err := GetCommunicator(id)
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
func setPodEnvVariables(t cluster.Target, pod *corev1.Pod, customEnv map[string]string) {
	containers := pod.Spec.Containers
	debug.Assert(len(containers) > 0)
	for idx := range containers {
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  "AIS_TARGET_URL",
			Value: t.Snode().URL(cmn.NetworkPublic),
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

// waitPodReady waits until Kubernetes marks a pod's state READY. This happens
// only after the pod's containers have started and the pods readinessProbe
// request (made by kubernetes itself) is successful. If pod doesn't have
// readinessProbe config specified the last step is skipped. Currently
// readinessProbe config is required by us in ETL spec.
func waitPodReady(errCtx *cmn.ETLErrorContext, pod *corev1.Pod, waitTimeout cmn.DurationJSON) error {
	client, err := newK8sClient()
	if err != nil {
		return cmn.NewETLError(errCtx, err.Error())
	}

	// TODO: find out more about failure: `kubectl get logs`, `kubectl describe pod`, ...
	err = wait.PollImmediate(time.Second, time.Duration(waitTimeout), func() (done bool, err error) {
		p, err := client.Pod(pod.Name)
		if err != nil {
			return false, err
		}

		for _, condition := range p.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}

		switch p.Status.Phase {
		case corev1.PodFailed, corev1.PodSucceeded:
			if len(p.Status.Conditions) > 0 {
				message := p.Status.Conditions[len(p.Status.Conditions)-1].Message
				return false, fmt.Errorf("pod ran to completion, last message: %s", message)
			}
			return false, errors.New("pod ran to completion")
		default:
			return false, nil
		}
	})
	if err != nil {
		return cmn.NewETLError(errCtx, err.Error())
	}
	return nil
}

func getPodIP(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	client, err := newK8sClient()
	if err != nil {
		return "", cmn.NewETLError(errCtx, err.Error())
	}
	p, err := client.Pod(pod.GetName())
	if err != nil {
		return "", err
	}
	return p.Status.PodIP, nil
}

func getPodHostIP(errCtx *cmn.ETLErrorContext, pod *corev1.Pod) (string, error) {
	client, err := newK8sClient()
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
	client, err := newK8sClient()
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
	client, err := newK8sClient()
	if err != nil {
		return err
	}
	if err = client.Create(spec); err != nil {
		return cmn.NewETLError(errCtx, "failed to create %s (err: %v)", entity, err)
	}
	return nil
}

func getServiceNodePort(errCtx *cmn.ETLErrorContext, svc *corev1.Service) (uint, error) {
	client, err := newK8sClient()
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
