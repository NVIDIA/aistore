// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
)

var (
	targetsNodeName = os.Getenv("AIS_NODE_NAME")
)

const (
	// built-in label https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#built-in-node-labels
	nodeNameLabel = "kubernetes.io/hostname"
)

// TODO: remove the `kubectl` with a proper go-sdk call

func StartTransformationPod(t cluster.Target, msg *Msg) (err error) {
	// Parse spec template.
	pod, err := ParsePodSpec(msg.Spec)
	if err != nil {
		return err
	}

	if targetsNodeName == "" {
		// Override the name (add target's daemon ID to its name).
		// Don't add any affinities if target's node name is unknown.
		pod.SetName(pod.GetName() + "-" + t.Snode().DaemonID)
		glog.Warningf("transformation %q starting without node affinity", pod.GetName())
	} else {
		// Override the name (add target's daemon ID and node ID to its name).
		pod.SetName(pod.GetName() + "-" + t.Snode().DaemonID + "-" + targetsNodeName)
		if err := setTransformAffinity(pod); err != nil {
			return err
		}
	}

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
	cmd = exec.Command("kubectl", "wait", "--timeout", msg.WaitTimeout.String(), "--for", "condition=ready", "pod", pod.GetName())
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

	cmn.AssertNoErr(validateCommType(msg.CommType))
	reg.put(msg.ID, entry{
		url:      fmt.Sprintf("http://%s:%s", ip, port),
		commType: msg.CommType,
	})
	return nil
}

func DoTransform(w io.Writer, r *http.Request, t cluster.Target, transformID string, bck *cluster.Bck, objName string) error {
	e, exists := reg.get(transformID)
	if !exists {
		return fmt.Errorf("transformation with %q id doesn't exist", transformID)
	}

	switch e.commType {
	case putCommType:
		lom := &cluster.LOM{T: t, ObjName: objName}
		if err := lom.Init(bck.Bck); err != nil {
			return err
		}
		if err := lom.Load(); err != nil {
			return err
		}

		var (
			group  = &errgroup.Group{}
			rp, wp = io.Pipe()
		)

		group.Go(func() error {
			req, err := http.NewRequest(http.MethodPost, e.url, rp)
			if err != nil {
				rp.CloseWithError(err)
				return err
			}

			req.ContentLength = lom.Size()
			req.Header.Set("Content-Type", "octet-stream")
			req.URL.RawQuery = r.URL.Query().Encode()
			resp, err := t.Client().Do(req)
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
		return fmt.Errorf("pod spec should not have any NodeAffinities defined")
	}

	nodeSelector := &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      nodeNameLabel,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{targetsNodeName},
			}}},
		},
	}
	// TODO: RequiredDuringSchedulingIgnoredDuringExecution means that transformer
	//  will be placed on the same machine as target which creates it. However,
	//  if 2 targets went down and up again at the same time, they may switch nodes,
	//  leaving transformers running on the wrong machines.
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
	return nil
}
