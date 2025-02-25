// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
)

type (
	// podWatcher uses the Kubernetes API to capture ETL pod status changes,
	// providing diagnostic information about the pod's internal state.
	podWatcher struct {
		podName         string
		recentPodStatus *podStatus
		watcher         watch.Interface

		// sync
		podCtx           context.Context
		podCtxCancelFunc context.CancelFunc
		stopCh           *cos.StopCh
		wg               sync.WaitGroup
	}

	podStatus struct {
		cname   string // container name
		reason  string
		message string
	}
)

func newPodWatcher(podName string) (pw *podWatcher) {
	pw = &podWatcher{
		podName: podName,
		stopCh:  cos.NewStopCh(),
	}
	pw.stopCh.Init()
	pw.podCtx, pw.podCtxCancelFunc = context.WithCancel(context.Background())
	return pw
}

func (pw *podWatcher) processEvents() {
	defer pw.podCtxCancelFunc()

	for {
		select {
		case event := <-pw.watcher.ResultChan():
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			// Init container state changes:
			// - watch only one problematic state: `pip install` command in init container terminates with non-zero exit code
			for i := range pod.Status.InitContainerStatuses {
				ics := &pod.Status.InitContainerStatuses[i]
				if ics.State.Terminated != nil && ics.State.Terminated.ExitCode != 0 {
					pw.recentPodStatus = &podStatus{cname: ics.Name, reason: ics.State.Terminated.Reason, message: ics.State.Terminated.Message}
					return
				}
			}

			// Main container state changes:
			// - Waiting & Running: Record state changes with detailed reason in pod watcher and continue to watch
			// - Terminated with non-zero exit code: Terminates the pod watcher goroutine, cancel context to cleans up, and reports the error immediately
			for i := range pod.Status.ContainerStatuses {
				cs := &pod.Status.ContainerStatuses[i]

				switch {
				case cs.State.Waiting != nil:
					pw.recentPodStatus = &podStatus{cname: cs.Name, reason: cs.State.Waiting.Reason, message: cs.State.Waiting.Message}
				case cs.State.Running != nil:
					pw.recentPodStatus = &podStatus{cname: cs.Name, reason: "Running", message: cs.State.Running.String()}
				case cs.State.Terminated != nil:
					pw.recentPodStatus = &podStatus{cname: cs.Name, reason: cs.State.Terminated.Reason, message: cs.State.Terminated.Message}
					if cs.State.Terminated.ExitCode != 0 {
						return
					}
				}
			}

			// We don't expect any of these to happen, as ETL containers are supposed to constantly
			// listen to upcoming requests and never terminate, until manually stoped/deleted
			if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
				nlog.Errorf("Alert: ETL Pod %s is in problematic phase: %s\n", pod.Name, pod.Status.Phase)
			}
		case <-pw.stopCh.Listen():
			return
		}
	}
}

func (pw *podWatcher) start() error {
	client, err := k8s.GetClient()
	if err != nil {
		return err
	}

	pw.watcher, err = client.WatchPodEvents(pw.podName)
	if err != nil {
		return err
	}

	pw.wg.Add(1)
	go func() {
		defer pw.wg.Done()
		pw.processEvents()
	}()

	return nil
}

// stop must always be called, even if the pod watcher is not yet started or failed to start.
func (pw *podWatcher) stop() {
	pw.stopCh.Close()

	// must stop and drain the watch channel before exiting
	pw.watcher.Stop()
	for range pw.watcher.ResultChan() {
	}

	pw.wg.Wait()
}

// wrapError wraps the provided error with the most recently captured pod status information.
func (pw *podWatcher) wrapError(err error) error {
	if err == nil || pw.recentPodStatus == nil {
		return err
	}
	return fmt.Errorf("%w, recent pod status: %q", err, pw.recentPodStatus)
}

func (ps *podStatus) Error() string {
	return fmt.Sprintf("container: %q, reason: %q, message: %q", ps.cname, ps.reason, ps.message)
}
