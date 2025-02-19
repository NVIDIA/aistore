// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
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
		eventCh         <-chan watch.Event

		// sync
		stopCh *cos.StopCh
		wg     sync.WaitGroup
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
	return pw
}

func (pw *podWatcher) processEvents() {
	for {
		select {
		case event := <-pw.eventCh:
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			// For now, only watch modified status
			if event.Type != watch.Modified {
				continue
			}

			// TODO: handle states diffrerntly based severity and source
			for i := range pod.Status.ContainerStatuses {
				cs := &pod.Status.ContainerStatuses[i]
				if cs.State.Waiting != nil {
					pw.recentPodStatus = &podStatus{cname: cs.Name, reason: cs.State.Waiting.Reason, message: cs.State.Waiting.Message}
				}
				if cs.State.Terminated != nil {
					pw.recentPodStatus = &podStatus{cname: cs.Name, reason: cs.State.Terminated.Reason, message: cs.State.Terminated.Message}
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

	watcher, err := client.WatchPodEvents(pw.podName)
	if err != nil {
		return err
	}

	pw.eventCh = watcher.ResultChan()

	pw.wg.Add(1)
	go func() {
		defer pw.wg.Done()
		pw.processEvents()
	}()

	return nil
}

func (pw *podWatcher) stop() {
	pw.stopCh.Close()
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
