// Package k8s provides utilities for communicating with Kubernetes cluster.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	tcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

type (
	// Client is simplified version of default `kubernetes.Interface` client.
	Client interface {
		Create(v interface{}) error
		Delete(entityType, entityName string) error
		CheckExists(entityType, entityName string) (bool, error)
		Pod(name string) (*corev1.Pod, error)
		Pods() (*corev1.PodList, error)
		Service(name string) (*corev1.Service, error)
		Node(name string) (*corev1.Node, error)
		Logs(podName string) ([]byte, error)
		Health(podName string) (cpuCores float64, freeMem int64, err error)
		CheckMetricsAvailability() error
		ExecCmd(podName string, command []string, stdin io.Reader, stdout, stderr io.Writer) error
	}

	// defaultClient implements Client interface.
	defaultClient struct {
		client    kubernetes.Interface
		config    *rest.Config
		namespace string
		err       error
	}

	metricsClient struct {
		client *metrics.Clientset
		err    error
	}
)

var (
	_clientOnce           sync.Once
	_metricsClientOnce    sync.Once
	_defaultK8sClient     *defaultClient
	_defaultMetricsClient *metricsClient
)

func (c *defaultClient) pods() tcorev1.PodInterface {
	return c.client.CoreV1().Pods(c.namespace)
}

func (c *defaultClient) services() tcorev1.ServiceInterface {
	return c.client.CoreV1().Services(c.namespace)
}

func (c *defaultClient) Create(v interface{}) (err error) {
	ctx := context.Background()
	switch t := v.(type) {
	case *corev1.Pod:
		_, err = c.pods().Create(ctx, t, metav1.CreateOptions{})
	case *corev1.Service:
		_, err = c.services().Create(ctx, t, metav1.CreateOptions{})
	default:
		debug.Assertf(false, "unknown entity type: %T", t)
	}
	return
}

func (c *defaultClient) ExecCmd(podName string, command []string,
	stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	req := c.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(c.namespace).
		Name(podName).
		SubResource("exec")
	req.VersionedParams(
		&corev1.PodExecOptions{
			Command: []string{"bash", "-c", strings.Join(command, " ")},
			Stdin:   stdin != nil,
			Stdout:  true,
			Stderr:  stderr != nil,
			TTY:     false,
		},
		scheme.ParameterCodec,
	)
	exec, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return err
	}
	return exec.Stream(remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	})
}

func (c *defaultClient) Delete(entityType, entityName string) (err error) {
	ctx := context.Background()
	switch entityType {
	case Pod:
		err = c.pods().Delete(ctx, entityName, *metav1.NewDeleteOptions(0))
	case Svc:
		err = c.services().Delete(ctx, entityName, *metav1.NewDeleteOptions(0))
	default:
		debug.Assertf(false, "unknown entity type: %s", entityType)
	}
	return
}

func (c *defaultClient) CheckExists(entityType, entityName string) (exists bool, err error) {
	var (
		ctx         = context.Background()
		listOptions = metav1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("metadata.name", entityName).String(),
		}
	)
	switch entityType {
	case Pod:
		var pods *corev1.PodList
		pods, err = c.pods().List(ctx, listOptions)
		if err != nil {
			return false, err
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
	case Svc:
		var services *corev1.ServiceList
		services, err = c.services().List(ctx, listOptions)
		if err != nil {
			return false, err
		}
		if len(services.Items) == 0 {
			return false, nil
		}
	default:
		debug.Assertf(false, "unknown entity type: %s", entityType)
	}
	return true, nil
}

func (c *defaultClient) Pod(name string) (*corev1.Pod, error) {
	return c.pods().Get(context.Background(), name, metav1.GetOptions{})
}

func (c *defaultClient) Pods() (*corev1.PodList, error) {
	return c.pods().List(context.Background(), metav1.ListOptions{})
}

func (c *defaultClient) Service(name string) (*corev1.Service, error) {
	return c.services().Get(context.Background(), name, metav1.GetOptions{})
}

func (c *defaultClient) Node(name string) (*corev1.Node, error) {
	return c.client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
}

func (c *defaultClient) Logs(podName string) ([]byte, error) {
	logStream, err := c.pods().GetLogs(podName, &corev1.PodLogOptions{}).Stream(context.Background())
	if err != nil {
		return nil, err
	}
	defer logStream.Close()
	return io.ReadAll(logStream)
}

func (c *defaultClient) CheckMetricsAvailability() error {
	_, err := c.client.CoreV1().RESTClient().Get().AbsPath("/apis/metrics.k8s.io/v1beta1/pods").DoRaw(context.Background())
	return err
}

func (*defaultClient) Health(podName string) (cpuCores float64, freeMem int64, err error) {
	var (
		totalCPU, totalMem int64
		fracCPU            float64
	)

	mc, err := getMetricsClient()
	if err != nil {
		return 0, 0, err
	}

	ms, err := mc.MetricsV1beta1().PodMetricses(metav1.NamespaceDefault).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		if statusErr, ok := err.(*errors.StatusError); ok && statusErr.Status().Reason == metav1.StatusReasonNotFound {
			err = cmn.NewErrNotFound("metrics for pod %q", podName)
		}
		return 0, 0, err
	}

	for _, metric := range ms.Containers {
		cpuNanoCores, ok := metric.Usage.Cpu().AsInt64()
		if !ok {
			cpuNanoCores = metric.Usage.Cpu().AsDec().UnscaledBig().Int64()
		}
		totalCPU += cpuNanoCores

		memInt, ok := metric.Usage.Memory().AsInt64()
		if !ok {
			memInt = metric.Usage.Memory().AsDec().UnscaledBig().Int64()
		}
		totalMem += memInt
	}

	// Kubernetes reports CPU in nanocores.
	// https://godoc.org/k8s.io/api/core/v1#ResourceName
	fracCPU = float64(totalCPU) / float64(1_000_000_000)
	return fracCPU, totalMem, nil
}

func GetClient() (Client, error) {
	_clientOnce.Do(_initClient)
	if _defaultK8sClient.err != nil {
		return nil, _defaultK8sClient.err
	}
	return _defaultK8sClient, nil
}

func _initClient() {
	config, err := rest.InClusterConfig()
	if err != nil {
		_defaultK8sClient = &defaultClient{err: err}
		return
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		_defaultK8sClient = &defaultClient{err: err}
		return
	}
	_defaultK8sClient = &defaultClient{
		namespace: _namespace(),
		client:    client,
		config:    config,
	}
}

func getMetricsClient() (*metrics.Clientset, error) {
	_metricsClientOnce.Do(_initMetricsClient)

	if _defaultMetricsClient.err != nil {
		return nil, _defaultMetricsClient.err
	}
	cos.Assert(_defaultMetricsClient.client != nil)
	return _defaultMetricsClient.client, nil
}

func _initMetricsClient() {
	config, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		_defaultMetricsClient = &metricsClient{
			err: fmt.Errorf("failed to retrieve metrics client config; err: %v", err),
		}
		return
	}

	mc, err := metrics.NewForConfig(config)
	if err != nil {
		_defaultMetricsClient = &metricsClient{
			err: fmt.Errorf("failed to create metrics client; err: %v", err),
		}
		return
	}

	_defaultMetricsClient = &metricsClient{
		client: mc,
	}
}

// Retrieve pod namespace
// See:
//  * https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/client-go/tools/clientcmd/client_config.go
//  * https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
//  * https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/#accessing-the-api-from-a-pod.
func _namespace() (namespace string) {
	if namespace = os.Getenv("POD_NAMESPACE"); namespace != "" {
		return
	}
	if ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if namespace = strings.TrimSpace(string(ns)); len(namespace) > 0 {
			return
		}
	}
	return "default"
}
