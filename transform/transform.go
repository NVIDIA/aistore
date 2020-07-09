// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

var (
	targetPodName = os.Getenv("AIS_POD_NAME")
)

const (
	targetPodNameLabel = "statefulset.kubernetes.io/pod-name"
	// This topology key defines a size of a group of nodes that should satisfy pods affinity.
	// In our case "hostname" means that transformer has to be placed on the same host - node.
	// https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#built-in-node-labels
	topologyKey = "kubernetes.io/hostname"
)

// TODO: remove the `kubectl` with a proper go-sdk call

func StartTransformationPod(msg *Msg) (err error) {
	// Parse spec template.
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(msg.Spec, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to parse pod spec: %v", err)
	}

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		kind := obj.GetObjectKind().GroupVersionKind().Kind
		return fmt.Errorf("expected pod spec, got: %s", kind)
	}

	if targetPodName == "" {
		return fmt.Errorf("target's pod name is empty. Make sure that cluster was deployed on kubernetes")
	}

	// Override the name (add target ID to its name).
	// TODO: check if there's already pod with this name running (?)
	pod.SetName(pod.GetName() + "-" + targetPodName)
	if err := setTransformAffinity(pod); err != nil {
		return err
	}

	targetLabelSelector := &metav1.LabelSelector{MatchLabels: map[string]string{targetPodNameLabel: targetPodName}}
	targetRunningAffinity := corev1.PodAffinityTerm{
		LabelSelector: targetLabelSelector,
		TopologyKey:   topologyKey,
	}
	// TODO: RequiredDuringSchedulingIgnoredDuringExecution means that transformer
	//  will be placed on the same machine as target which creates it. However,
	//  if 2 targets went down and up again at the same time, they may switch nodes,
	//  leaving transformers running on the wrong machines.
	pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{targetRunningAffinity}

	// Encode the specification once again to be ready for the start.
	b, err := jsoniter.Marshal(pod)
	if err != nil {
		return err
	}

	// Start the pod.
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewBuffer(b)
	if b, err = cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to apply spec for %q pod (err: %v; output: %s)", pod.GetName(), err, string(b))
	}

	// Wait for the pod to start.
	cmd = exec.Command("kubectl", "wait", "--timeout", msg.WaitTimeout, "--for", "condition=ready", "pod", pod.GetName())
	if b, err = cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to wait for %q pod to be ready (err: %v; output: %s)", pod.GetName(), err, string(b))
	}

	// Retrieve IP of the pod.
	output, err := exec.Command("kubectl", "get", "pod", pod.GetName(), "--template={{.status.podIP}}").Output()
	if err != nil {
		return fmt.Errorf("failed to get IP of %q pod (err: %v; output: %s)", pod.GetName(), err, string(b))
	}
	ip := string(output)

	// Retrieve port of the pod.
	p := pod.Spec.Containers[0].Ports[0].ContainerPort
	port := strconv.FormatInt(int64(p), 10)

	reg.put(msg.ID, entry{
		url:      fmt.Sprintf("http://%s:%s", ip, port),
		commType: msg.CommType,
	})
	return nil
}

func DoTransform(w io.Writer, t cluster.Target, transformID string, bck *cluster.Bck, objName string) error {
	e := reg.get(transformID)

	switch e.commType {
	case putCommType:
		lom := &cluster.LOM{T: t, ObjName: objName}
		if err := lom.Init(bck.Bck); err != nil {
			return err
		}

		var (
			group  = &errgroup.Group{}
			rp, wp = io.Pipe()
		)

		group.Go(func() error {
			resp, err := t.Client().Post(e.url, "application/json", rp)
			rp.CloseWithError(err)
			if err != nil {
				return err
			}
			_, err = io.Copy(w, resp.Body)
			debug.AssertNoErr(err)
			err = resp.Body.Close()
			debug.AssertNoErr(err)
			return nil
		})

		err := t.GetObject(wp, lom, time.Now())
		wp.CloseWithError(err)
		if err != nil {
			return err
		}
		return group.Wait()
	case pushPullCommType:
		cmn.AssertMsg(false, "not yet implemented")
	default:
		cmn.AssertMsg(false, e.commType)
	}
	return nil
}

func setTransformAffinity(pod *corev1.Pod) error {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.PodAffinity == nil {
		pod.Spec.Affinity.PodAffinity = &corev1.PodAffinity{}
	}

	podAffinity := pod.Spec.Affinity.PodAffinity
	if len(podAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 ||
		len(podAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 {
		return fmt.Errorf("pod spec should not have any PodAffinities defined")
	}
	return nil
}
