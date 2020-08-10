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
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
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
// ETL -> This refers to Extract-Transform-Load, which allows a user to do transformation
//        of the objects. It is defined by an ETL spec which is a K8S yaml spec file.
//        The operations of an ETL are executed on the ETL container.
//
// ETL container -> The user's K8S pod which runs the container doing the transformation of the
//                  objects.  It is initiated by a target and runs on the same K8S node running
//                  the target.
// Flow:
// 1. User initiates an ETL container, using the `Start` method.
// 2. The ETL container starts on the same node as the target.
// 3. The transformation is done using `Communicator.Do()`
// 4. The ETL container is stopped using `Stop`, which deletes the K8S pod.
//
// Limitations of the current implementation (soon to be removed):
//
// * No idle timeout for a ETL container. It keeps running unless explicitly
//   stopped by invoking the `Stop` API.
//
// * `kubectl delete` of a ETL container is done in two stages. First we gracefully try to terminate
//   the pod with a 30s timeout. Upon failure to do so, we perform a force delete.
//
// * A single ETL container runs per target at any point of time.
//
// * Recreating a ETL container with the same name, will delete any containers running with
//   the same name.
func Start(t cluster.Target, nodeName string, msg Msg) (err error) {
	var (
		pod    *corev1.Pod
		svc    *corev1.Service
		hostIP string
	)
	cmn.Assert(nodeName != "")
	// Parse spec template.
	if pod, err = ParsePodSpec(msg.Spec); err != nil {
		return err
	}
	// Override the name (add target's daemon ID and node ID to its name).
	pod.SetName(pod.GetName() + "-" + t.Snode().DaemonID + "-" + nodeName)
	if pod.Labels == nil {
		pod.Labels = make(map[string]string, 1)
	}
	pod.Labels[targetNode] = nodeName

	// Create service spec
	svc = createServiceSpec(pod)

	// The following combination of Affinity and Anti-Affinity allows one to
	// achieve the following:
	// 1. The ETL container is always scheduled on the target invoking it.
	// 2. Not more than one ETL container with the same target, is scheduled on the same node,
	//    at a given point of time
	if err := setTransformAffinity(pod); err != nil {
		return err
	}

	if err := setTransformAntiAffinity(pod); err != nil {
		return err
	}

	if err := setPodEnvVariables(pod, t); err != nil {
		return err
	}
	// 1. Doing cleanup of any pre-existing entities
	if err := deleteEntity(cmn.KubePod, pod.Name); err != nil {
		return err
	}

	if err := deleteEntity(cmn.KubeSvc, svc.Name); err != nil {
		return err
	}

	// 2. Creating pod
	if err := createEntity(cmn.KubePod, pod); err != nil {
		// Ignoring the error for deletion as it is best effort.
		glog.Errorf("Failed creation of pod %q. Doing cleanup.", pod.Name)
		deleteEntity(cmn.KubePod, pod.Name)
		return err
	}

	if err := waitPodReady(pod, msg.WaitTimeout); err != nil {
		return err
	}

	// Retrieve host IP of the pod.
	if hostIP, err = getPodHostIP(pod); err != nil {
		return err
	}

	// 3. Creating service
	if err := createEntity(cmn.KubeSvc, svc); err != nil {
		// Ignoring the error for deletion as it is best effort.
		glog.Errorf("Failed creation of svc %q. Doing cleanup.", svc.Name)
		deleteEntity(cmn.KubeSvc, svc.Name)
		deleteEntity(cmn.KubePod, pod.Name)
		return err
	}

	nodePort, err := getServiceNodePort(svc)
	if err != nil {
		return err
	}

	transformerURL := fmt.Sprintf("http://%s:%d", hostIP, nodePort)

	// TODO: Temporary workaround. Debug this further to find the root cause.
	// Not waiting causes the first Do() request to fail
	// sometimes.
	readinessPath := pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Path
	if waitErr := waitTransformerReady(transformerURL, readinessPath); waitErr != nil {
		if err := deleteEntity(cmn.KubePod, pod.Name); err != nil {
			glog.Errorf("Unable to delete pod: %s, err: %s", pod.Name, err.Error())
		}
		if err := deleteEntity(cmn.KubePod, svc.Name); err != nil {
			glog.Errorf("Unable to delete svc: %s, err: %s", svc.Name, err.Error())
		}
		return waitErr
	}

	c := makeCommunicator(t, pod, msg.CommType, transformerURL, nodeName)
	reg.put(msg.ID, c)
	return nil
}

// TODO: remove the `kubectl` calls with proper go-sdk calls.

func waitTransformerReady(url, path string) error {
	tfProbeSleep := cmn.GCO.Get().Timeout.MaxKeepalive
	tfProbeClient.Timeout = tfProbeSleep
	for i := 0; i < tfProbeRetries; i++ {
		resp, err := tfProbeClient.Get(cmn.JoinPath(url, path))
		if err != nil {
			if cmn.IsReqCanceled(err) || cmn.IsErrConnectionRefused(err) {
				glog.Errorf("checking transformer endpoint: %s  failed, err: %s, retryCount: %d",
					url, err.Error(), i+1)
				time.Sleep(tfProbeSleep)
				continue
			}
			return err
		}
		err = cmn.DrainReader(resp.Body)
		resp.Body.Close()
		return err
	}
	return fmt.Errorf("waiting for transformer readiness failed for endpoint: %s", url)
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

func Stop(id string) error {
	c, err := GetCommunicator(id)
	if err != nil {
		return err
	}

	if err := deleteEntity(cmn.KubePod, c.PodName()); err != nil {
		return err
	}

	if err := deleteEntity(cmn.KubeSvc, c.SvcName()); err != nil {
		return err
	}

	reg.removeByUUID(id)
	return nil
}

func GetCommunicator(transformID string) (Communicator, error) {
	c, exists := reg.getByUUID(transformID)
	if !exists {
		return nil, cmn.NewNotFoundError("transformation with %q id", transformID)
	}
	return c, nil
}

func List() []Info { return reg.list() }

// Sets pods node affinity, so pod will be scheduled on the same node as a target creating it.
func setTransformAffinity(pod *corev1.Pod) error {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	reqAffinity := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAffinity := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	if reqAffinity != nil && len(reqAffinity.NodeSelectorTerms) > 0 || len(prefAffinity) > 0 {
		return fmt.Errorf("error in spec, pod: %q should not have any NodeAffinities defined", pod)
	}

	cmn.Assert(cmn.GetK8sNodeName() != "")
	nodeSelector := &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      nodeNameLabel,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{cmn.GetK8sNodeName()},
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
func setTransformAntiAffinity(pod *corev1.Pod) error {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.PodAntiAffinity == nil {
		pod.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}

	reqAntiAffinities := pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	prefAntiAffinity := pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	if len(reqAntiAffinities) > 0 || len(prefAntiAffinity) > 0 {
		return fmt.Errorf("error in spec, pod: %q should not have any NodeAntiAffinities defined", pod)
	}

	cmn.Assert(cmn.GetK8sNodeName() != "")
	reqAntiAffinities = []corev1.PodAffinityTerm{{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				targetNode: pod.Labels[targetNode],
			},
		},
		TopologyKey: nodeNameLabel,
	}}
	pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = reqAntiAffinities
	return nil
}

// Sets environment variables that can be accessed inside the container.
func setPodEnvVariables(pod *corev1.Pod, t cluster.Target) error {
	containers := pod.Spec.Containers
	for idx := range containers {
		containers[idx].Env = append(containers[idx].Env, corev1.EnvVar{
			Name:  "AIS_TARGET_URL",
			Value: t.Snode().URL(cmn.NetworkPublic),
		})
	}
	return nil
}

func waitPodReady(pod *corev1.Pod, waitTimeout cmn.DurationJSON) error {
	args := []string{"wait"}
	if !waitTimeout.IsZero() {
		args = append(args, "--timeout", waitTimeout.String())
	}
	args = append(args, "--for", "condition=ready", "pod", pod.GetName())
	cmd := exec.Command(cmn.Kubectl, args...)
	if b, err := cmd.CombinedOutput(); err != nil {
		handlePodFailure(pod, "pod start failure")
		return fmt.Errorf("failed to wait for %q pod to be ready (err: %v; output: %s)", pod.GetName(), err, string(b))
	}
	return nil
}

func getPodHostIP(pod *corev1.Pod) (string, error) {
	// Retrieve host IP of the pod.
	args := []string{"get", "pod", pod.GetName(), "--template={{.status.hostIP}}"}
	output, err := exec.Command(cmn.Kubectl, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to get IP of %q pod (err: %v; output: %s)", pod.GetName(), err, string(output))
	}
	return string(output), nil
}

func deleteEntity(entity, entityName string) error {
	args := []string{"delete", entity, entityName, "--ignore-not-found"}
	// Doing graceful delete
	output, err := exec.Command(cmn.Kubectl, args...).CombinedOutput()
	if err != nil {
		glog.Errorf("graceful delete failed. %q %s, err: %v, out: %s",
			entity, entityName, err, string(output))
		// Doing force delete
		args = append(args, "--force")
		output, err = exec.Command(cmn.Kubectl, args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("force delete failed. %q %s, err: %v, out: %s",
				entity, entityName, err, string(output))
		}
	}
	return nil
}

func createEntity(entity string, spec interface{}) error {
	b, err := jsoniter.Marshal(spec)
	if err != nil {
		return err
	}
	args := []string{"create", "-f", "-"}
	cmd := exec.Command(cmn.Kubectl, args...)
	cmd.Stdin = bytes.NewBuffer(b)
	if b, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to create %s (err: %v; output: %s)", entity, err, string(b))
	}
	return nil
}

func getServiceNodePort(svc *corev1.Service) (int, error) {
	args := []string{"get", "-o", "jsonpath=\"{.spec.ports[0].nodePort}\"", "svc", svc.GetName()}
	output, err := exec.Command(cmn.Kubectl, args...).CombinedOutput()
	if err != nil {
		return -1, fmt.Errorf("failed to get nodePort of service %q (err: %v; output: %s)", svc.GetName(), err, string(output))
	}
	outputStr, _ := strconv.Unquote(string(output))
	nodePort, err := strconv.Atoi(outputStr)
	if err != nil {
		return -1, fmt.Errorf("failed to parse nodePort for pod-svc %q (err: %v; output: %s)", svc.GetName(), err, string(output))
	}
	return nodePort, nil
}

func handlePodFailure(pod *corev1.Pod, msg string) {
	if deleteErr := deleteEntity(cmn.KubePod, pod.GetName()); deleteErr != nil {
		glog.Errorf("failed to delete pod %q after %s", pod.GetName(), msg)
	}
}
