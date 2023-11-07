// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	tcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type (
	// Client is simplified version of default `kubernetes.Interface` client.
	Client interface {
		Create(v any) error
		Delete(entityType, entityName string) error
		CheckExists(entityType, entityName string) (bool, error)
		Pod(name string) (*corev1.Pod, error)
		Pods() (*corev1.PodList, error)
		Service(name string) (*corev1.Service, error)
		Node(name string) (*corev1.Node, error)
		Logs(podName string) ([]byte, error)
		Health(podName string) (string, error)
		CheckMetricsAvailability() error
	}

	// defaultClient implements Client interface.
	defaultClient struct {
		client    kubernetes.Interface
		config    *rest.Config
		namespace string
		err       error
	}
)

var (
	_defaultK8sClient *defaultClient
)

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

// Retrieve pod namespace
// See:
//   - https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/client-go/tools/clientcmd/client_config.go
//   - https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
//   - https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/#accessing-the-api-from-a-pod.
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

func GetClient() (Client, error) {
	if _defaultK8sClient.err != nil {
		return nil, _defaultK8sClient.err
	}
	return _defaultK8sClient, nil
}

///////////////////
// defaultClient //
///////////////////

func (c *defaultClient) pods() tcorev1.PodInterface {
	return c.client.CoreV1().Pods(c.namespace)
}

func (c *defaultClient) services() tcorev1.ServiceInterface {
	return c.client.CoreV1().Services(c.namespace)
}

func (c *defaultClient) Create(v any) (err error) {
	ctx := context.Background()
	switch t := v.(type) {
	case *corev1.Pod:
		_, err = c.pods().Create(ctx, t, metav1.CreateOptions{})
	case *corev1.Service:
		_, err = c.services().Create(ctx, t, metav1.CreateOptions{})
	default:
		debug.FailTypeCast(v)
	}
	return
}

func (c *defaultClient) Delete(entityType, entityName string) (err error) {
	ctx := context.Background()
	switch entityType {
	case Pod:
		err = c.pods().Delete(ctx, entityName, *metav1.NewDeleteOptions(0))
	case Svc:
		err = c.services().Delete(ctx, entityName, *metav1.NewDeleteOptions(0))
	default:
		debug.Assert(false, "unknown entity type", entityType)
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
		debug.Assert(false, "unknown entity type", entityType)
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

func (c *defaultClient) Health(podName string) (string, error) {
	response, err := c.pods().Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		return "Error", err
	}
	return string(response.Status.Phase), nil
}
