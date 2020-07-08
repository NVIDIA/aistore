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
	"os/exec"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
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
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return errors.New("expected Pod spec")
	}

	// TODO: set affinity:
	//  1. How can we know the target's pod name?
	//  2. How can we know the target's node name?
	// terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	// terms = append(terms, corev1.NodeSelectorTerm{
	// 	MatchExpressions: []corev1.NodeSelectorRequirement{},
	// })		mapping := make(map[string]string, 1)

	// Override the name (add target ID to its name).
	pod.SetName(pod.GetName() + "-" + t.Snode().ID())

	// Encode the specification once again to be ready for the start.
	b, err := jsoniter.Marshal(pod)
	if err != nil {
		return err
	}

	// Start the pod.
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewBuffer(b)
	if _, err = cmd.CombinedOutput(); err != nil {
		return err
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
