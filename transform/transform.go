// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	jsoniter "github.com/json-iterator/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

// TODO: remove the `kubectl` with a proper go-sdk call

var (
	// TODO: remove when we have `/init` handler (exact copy of hello_world/hello_world.yaml)
	HelloSpec = []byte(`
apiVersion: v1
kind: Pod
metadata:
  name: transformer-hello
spec:
  containers:
    - name: server
      image: localhost:5000/hello_world_server:v1
      ports:
        - containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
`)
)

func StartTransformationPod(t cluster.Target, msg *Msg) (err error) {
	// Parse spec template.
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(msg.Spec, nil, nil)

	if err != nil {
		return err
	}
	// TODO: switch over types, v1beta1.Deployment, etc.. (is it necessary though? maybe we should enforce kind=Pod)
	//  or maybe we could use `"k8s.io/apimachinery/pkg/apis/meta/v1".Object` but
	//  it doesn't have affinity support...

	pod := obj.(*v1.Pod)

	{
		// TODO: set affinity:
		//  1. How can we know the target's pod name?
		//  2. How can we know the target's node name?

		// terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		// terms = append(terms, corev1.NodeSelectorTerm{
		// 	MatchExpressions: []corev1.NodeSelectorRequirement{},
		// })		mapping := make(map[string]string, 1)

	}

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

	// Wait for the pod to start.
	cmd = exec.Command("kubectl", "wait", "--for", "condition=ready", "pod", pod.GetName())
	if _, err = cmd.CombinedOutput(); err != nil {
		return err
	}

	var (
		ip, port string
	)

	// GET IP

	output, err := exec.Command("kubectl", "get", "pod", pod.GetName(), "--template={{.status.podIP}}").Output()
	if err != nil {
		return err
	}
	ip = string(output)

	{
		// GET PORT

		// output, err = exec.Command("kubectl", "get", "pod", transformName, "--template={{(index (index .spec.containers 0).ports 0).containerPort}}{{\"\\n\"}}").Output()
		// glog.Error(err, ": ", string(output))

		// TODO: does it always work?? - or can we enforce it?:
		p := pod.Spec.Containers[0].Ports[0].ContainerPort
		port = strconv.FormatInt(int64(p), 10)
	}

	Registry.Put(msg.Id, fmt.Sprintf("http://%s:%s", ip, port))
	return nil
}

// TODO: replace `mapping` argument
func DoTransform(transformerId string, objectName string) error {
	url := Registry.Get(transformerId)
	glog.Errorf("mapping: %s => %s", transformerId, url)

	// Try to contact the pod to see if everything works.
	f := bytes.NewBuffer([]byte(objectName))
	resp, err := http.Post(url, "application/json", f)
	if err != nil {
		return err
	}
	b, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	glog.Error(resp.StatusCode, ": ", string(b))
	return nil
}
