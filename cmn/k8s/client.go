// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	tcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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
		Logs(podName string) ([]byte, error)
		WatchPodEvents(podName string) (watch.Interface, error)
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
//   - topic: "how to get current namespace of an in-cluster go Kubernetes client"
//   - https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
func _namespace() (namespace string) {
	// production
	if namespace = os.Getenv(env.AisK8sNamespace); namespace != "" {
		debug.Func(func() {
			ns := os.Getenv(defaultNamespaceEnv)
			debug.Assertf(ns == "" || ns == namespace, "%q vs %q", ns, namespace)
		})
		return
	}
	// otherwise, try default env var
	if namespace = os.Getenv(defaultNamespaceEnv); namespace != "" {
		return
	}
	// finally, last resort kludge
	if ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if namespace = strings.TrimSpace(string(ns)); namespace != "" {
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

func (c *defaultClient) Logs(podName string) (b []byte, err error) {
	var (
		logStream io.ReadCloser
		req       = c.pods().GetLogs(podName, &corev1.PodLogOptions{})
	)
	// ref: "Stream formats and executes the request, and offers streaming of the response."
	logStream, err = req.Stream(context.Background())
	if err != nil {
		return nil, err
	}
	b, err = cos.ReadAllN(logStream, cos.ContentLengthUnknown)
	e := logStream.Close()
	if err == nil {
		err = e
	}
	return b, err
}

func (c *defaultClient) WatchPodEvents(podName string) (watch.Interface, error) {
	return c.pods().Watch(context.Background(), metav1.ListOptions{
		FieldSelector: "metadata.name=" + podName,
		Watch:         true,
	})
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

// InitTestClient initializes a K8s client for testing environments using kubeconfig.
// This is designed for use in Go tests running from shell environments with kubectl access.
func InitTestClient(namespace ...string) (Client, error) {
	config, err := _getKubeConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	ns := "default"
	if len(namespace) > 0 && namespace[0] != "" {
		ns = namespace[0]
	} else if envNs := os.Getenv("KUBERNETES_NAMESPACE"); envNs != "" {
		ns = envNs
	}

	return &defaultClient{
		namespace: ns,
		client:    client,
		config:    config,
	}, nil
}

// _getKubeConfig attempts to load kubeconfig from various standard locations
func _getKubeConfig() (*rest.Config, error) {
	// 1. Try KUBECONFIG environment variable
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	// 2. Try default location in home directory
	if home := homedir.HomeDir(); home != "" {
		kubeconfig := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(kubeconfig); err == nil {
			return clientcmd.BuildConfigFromFlags("", kubeconfig)
		}
	}

	// 3. Fall back to in-cluster config (for running in pods)
	return rest.InClusterConfig()
}
