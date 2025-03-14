// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// Container state string constants (reference: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#containerstate-v1-core)
const (
	ctrWaiting    = "Waiting"
	ctrRunning    = "Running"
	ctrTerminated = "Terminated"
)

type (
	// podWatcher uses the Kubernetes API to capture ETL pod status changes,
	// providing diagnostic information about the pod's internal state.
	podWatcher struct {
		podName         string
		boot            *etlBootstrapper
		recentPodStatus *podStatus
		watcher         watch.Interface

		// sync
		podCtx       context.Context
		podCtxCancel context.CancelFunc
		stopCh       *cos.StopCh
		psMutex      sync.Mutex
	}

	podStatus struct {
		state   string // "Waiting" | "Running" | "Terminated"
		cname   string // container name
		reason  string
		message string
	}
)

func newPodWatcher(podName string, boot *etlBootstrapper) (pw *podWatcher) {
	pw = &podWatcher{
		podName:         podName,
		boot:            boot,
		stopCh:          cos.NewStopCh(),
		recentPodStatus: &podStatus{},
	}
	pw.podCtx, pw.podCtxCancel = context.WithCancel(context.Background())
	return pw
}

func (pw *podWatcher) processEvents() {
	defer pw.podCtxCancel()
	for {
		select {
		case event := <-pw.watcher.ResultChan():
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if exitCode := pw._process(pod); exitCode != 0 {
				if pw.boot != nil && pw.boot.xctn != nil { // pw.boot.xctn is not yet assigned in init error
					abortErr := cmn.NewErrETL(pw.boot.errCtx, ctrTerminated)
					if pw.boot.xctn.Abort(pw.wrapError(abortErr)) {
						// After Finish() call succeed, proxy will be notified and broadcast to call etl.Stop()
						// on all targets (including the current one) with the `abortErr`. No need to call Stop() again here.
						pw.boot.xctn.Finish()
					}
				}
				return
			}
		case <-pw.stopCh.Listen():
			return
		}
	}
}

// _process analyzes the pod's container states and updates the pod watcher.
// Returns the ExitCode if any container terminated unexpectedly; otherwise, returns 0.
func (pw *podWatcher) _process(pod *corev1.Pod) int32 {
	// Init container state changes:
	// - watch only one problematic state: `pip install` command in init container terminates with non-zero exit code
	for i := range pod.Status.InitContainerStatuses {
		ics := &pod.Status.InitContainerStatuses[i]
		if ics.State.Terminated != nil && ics.State.Terminated.ExitCode != 0 {
			pw.setPodStatus(ctrTerminated, ics.Name, ics.State.Terminated.Reason, ics.State.Terminated.Message)
			return ics.State.Terminated.ExitCode
		}
	}

	// Main container state changes:
	// - Waiting & Running: Record state changes with detailed reason in pod watcher and continue to watch
	// - Terminated with non-zero exit code: Terminates the pod watcher goroutine, cancel context to cleans up, and reports the error immediately
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]

		switch {
		case cs.State.Waiting != nil:
			pw.setPodStatus(ctrWaiting, cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message)
		case cs.State.Running != nil:
			pw.setPodStatus(ctrRunning, cs.Name, "Running", cs.State.Running.String())
		case cs.State.Terminated != nil:
			pw.setPodStatus(ctrTerminated, cs.Name, cs.State.Terminated.Reason, cs.State.Terminated.Message)
			if cs.State.Terminated.ExitCode != 0 {
				return cs.State.Terminated.ExitCode
			}
		}
	}

	// We don't expect any of these to happen, as ETL containers are supposed to constantly
	// listen to upcoming requests and never terminate, until manually stopped/deleted
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		nlog.Errorf("ETL Pod %s is in problematic phase: %s (expecting either %s or %s phase)\n",
			pod.Name, pod.Status.Phase, corev1.PodPending, corev1.PodRunning)
	}
	return 0
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

	go pw.processEvents()

	return nil
}

// stop must always be called, even if the pod watcher was not started or failed to start.
// If wait is true, stop processes all queued events from the K8s watcher and updates the pod watcher before returning.
// If wait is false or the pod watcher has already captured a Terminated state, stop simply drains the queued events.
func (pw *podWatcher) stop(wait bool) {
	// Notify the `pw.processEvents()` goroutine to exit through stopCh
	pw.stopCh.Close()
	pw.watcher.Stop()

	// Wait for `pw.processEvents()` to terminate, which will trigger `pw.podCtx` cancellation
	<-pw.podCtx.Done()

	if !wait || pw.getPodStatus().state == ctrTerminated {
		for range pw.watcher.ResultChan() {
		}
		return
	}

	// Process remaining events
	for event := range pw.watcher.ResultChan() {
		if pod, ok := event.Object.(*corev1.Pod); ok {
			if exitCode := pw._process(pod); exitCode != 0 {
				break
			}
		}
	}
}

// wrapError wraps the provided error with the most recently captured pod status information.
func (pw *podWatcher) wrapError(err error) error {
	if err == nil {
		return err
	}
	return fmt.Errorf("%w, recent pod status: %q", err, pw.getPodStatus())
}

// setPodStatus safely sets the pod status by mutex
func (pw *podWatcher) setPodStatus(state, cname, reason, message string) {
	pw.psMutex.Lock()
	defer pw.psMutex.Unlock()

	pw.recentPodStatus.state, pw.recentPodStatus.cname, pw.recentPodStatus.reason, pw.recentPodStatus.message = state, cname, reason, message
}

// getPodStatus safely retrieves a copy of the pod status by mutex
func (pw *podWatcher) getPodStatus() (rps podStatus) {
	pw.psMutex.Lock()
	defer pw.psMutex.Unlock()

	if pw.recentPodStatus == nil {
		return podStatus{}
	}
	return *pw.recentPodStatus
}

func (ps *podStatus) Error() string {
	return fmt.Sprintf("container: %q, state: %q, reason: %q, message: %q", ps.cname, ps.state, ps.reason, ps.message)
}
