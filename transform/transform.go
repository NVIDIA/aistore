// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
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

func StartTransformationPod(t cluster.Target, msg *Msg) (err error) {
	// Parse spec template.
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(msg.Spec, nil, nil)
	if err != nil {
		return err
	}

	// TODO: switch over types, v1beta1.Deployment, etc.. (is it necessary though? maybe we should enforce kind=Pod)
	//  or maybe we could use `"k8s.io/apimachinery/pkg/apis/meta/v1".Object` but
	//  it doesn't have affinity support...
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return errors.New("expected Pod spec")
	}

	if targetPodName == "" {
		return fmt.Errorf("target's pod name is empty. Make sure that cluster was deployed on kubernetes")
	}

	// Override the name (add target ID to its name).
	// TODO: check if there's already pod with this name running (?)
	newTransformName := pod.GetName() + "-" + targetPodName
	pod.SetName(newTransformName)
	if err := setTransformAffinity(pod); err != nil {
		return err
	}

	targetLabelSelector := &metav1.LabelSelector{MatchLabels: map[string]string{targetPodNameLabel: targetPodName}}
	targetRunningAffinity := corev1.PodAffinityTerm{
		LabelSelector: targetLabelSelector,
		TopologyKey:   topologyKey,
	}
	// TODO: RequiredDuringSchedulingIgnoredDuringExecution means that transformer will be placed on the same machine
	// as target which creates it. However, if 2 targets went down and up again at the same time, they may
	// switch nodes, leaving transformers running on the wrong machines.
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
		return fmt.Errorf("%v: %s", err, string(b))
	}

	if msg.WaitTimeout == "" {
		msg.WaitTimeout = "0"
	}

	// TODO: check if pod errored
	// Wait for the pod to start.
	cmd = exec.Command("kubectl", "wait", "--timeout", msg.WaitTimeout, "--for", "condition=ready", "pod", pod.GetName())
	if _, err = cmd.CombinedOutput(); err != nil {
		return err
	}

	// Retrieve IP of the pod.
	output, err := exec.Command("kubectl", "get", "pod", pod.GetName(), "--template={{.status.podIP}}").Output()
	if err != nil {
		return err
	}
	ip := string(output)

	// Retrieve port of the pod.
	p := pod.Spec.Containers[0].Ports[0].ContainerPort
	port := strconv.FormatInt(int64(p), 10)

	reg.put(msg.ID, entry{
		url:      fmt.Sprintf("http://%s:%s", ip, port),
		commType: putComm, // TODO: remove hardcoded
	})
	return nil
}

func DoTransform(w io.Writer, t cluster.Target, transformID string, bck *cluster.Bck, objName string) error {
	e := reg.get(transformID)

	switch e.commType {
	case putComm:
		lom := &cluster.LOM{T: t, ObjName: objName}
		if err := lom.Init(bck.Bck); err != nil {
			return err
		}

		// TODO: This is inefficient because we will retrieve full object to
		//  memory. We probably need to start `http.Post` in separate goroutine.
		rp, wp := io.Pipe()
		if err := t.GetObject(wp, lom, time.Now()); err != nil {
			return err
		}
		// TODO: Use `t.httpclient.Do(...)` instead of raw `http.Post`.
		resp, err := http.Post(e.url, "application/json", rp)
		if err != nil {
			return err
		}
		io.Copy(w, resp.Body)
		resp.Body.Close()
	case pushPullComm:
		cmn.AssertMsg(false, "not yet implemented")
	default:
		cmn.Assert(false)
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

	if len(pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 ||
		len(pod.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 {
		return fmt.Errorf("pod should not have any PodAffinities defined")
	}
	return nil
}
