// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"context"
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

type metricsClient struct {
	client *metrics.Clientset
	err    error
}

var (
	once sync.Once
	_mc  *metricsClient
)

func InitMetricsClient() { once.Do(_initmc) }

func _initmc() {
	config, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		_mc = &metricsClient{
			err: fmt.Errorf("failed to retrieve metrics client config: %w", err),
		}
		return
	}
	mc, err := metrics.NewForConfig(config)
	if err != nil {
		_mc = &metricsClient{
			err: fmt.Errorf("failed to create metrics client: %w", err),
		}
		return
	}
	_mc = &metricsClient{
		client: mc,
	}
}

func Metrics(podName string) (float64 /*cores*/, int64 /*mem*/, error) {
	var (
		totalCPU, totalMem int64
		fracCPU            float64
	)
	if _mc.err != nil {
		return 0, 0, _mc.err
	}
	debug.Assert(_mc.client != nil)

	var (
		mc       = _mc.client
		msgetter = mc.MetricsV1beta1().PodMetricses(metav1.NamespaceDefault)
		ms, err  = msgetter.Get(context.Background(), podName, metav1.GetOptions{})
	)
	if err != nil {
		if statusErr, ok := err.(*errors.StatusError); ok && statusErr.Status().Reason == metav1.StatusReasonNotFound {
			err = cos.NewErrNotFound(nil, "metrics for pod "+podName)
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

	// Kubernetes reports CPU in nanocores, see https://godoc.org/k8s.io/api/core/v1#ResourceName
	fracCPU = float64(totalCPU) / float64(1_000_000_000)
	return fracCPU, totalMem, nil
}
