// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// Container state string constants (reference: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#containerstate-v1-core)
const (
	ctrWaiting    = "Waiting"
	ctrRunning    = "Running"
	ctrTerminated = "Terminated"
)

// podWatcher uses the Kubernetes API to capture ETL pod status changes,
// providing diagnostic information about the pod's internal state.
type podWatcher struct {
	watcher         watch.Interface
	podCtx          context.Context
	xetl            core.Xact
	recentPodStatus *k8s.PodStatus
	podCtxCancel    context.CancelFunc
	stopCh          *cos.StopCh
	podName         string
	psMutex         sync.Mutex
}

func newPodWatcher(podName string, xetl core.Xact) (pw *podWatcher) {
	pw = &podWatcher{
		podName:         podName,
		xetl:            xetl,
		recentPodStatus: &k8s.PodStatus{},
	}
	return pw
}

func (pw *podWatcher) processEvents() {
	debug.Assert(pw.xetl != nil, "xact must be initialized before starting the pod watcher")
	for {
		select {
		case event := <-pw.watcher.ResultChan():
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if err := pw._process(pod); err != nil {
				pw.podCtxCancel()
				errCtx := &cmn.ETLErrCtx{PodName: pw.podName, PodStatus: pw.GetPodStatus()}
				pw.xetl.Abort(cmn.NewErrETL(errCtx, err.Error()))
				return
			}
		case <-pw.stopCh.Listen():
			pw.podCtxCancel()
			return
		case <-pw.xetl.ChanAbort():
			pw.podCtxCancel()
			return
		}
	}
}

// _process analyzes the pod's container states and updates the pod watcher.
// Returns the ExitCode if any container terminated unexpectedly; otherwise, returns 0.
func (pw *podWatcher) _process(pod *corev1.Pod) error {
	// Init container state changes:
	// - watch only one problematic state: `pip install` command in init container terminates with non-zero exit code
	for i := range pod.Status.InitContainerStatuses {
		ics := &pod.Status.InitContainerStatuses[i]
		if ics.State.Terminated != nil && ics.State.Terminated.ExitCode != 0 {
			pw.setPodStatus(ctrTerminated, ics.Name, ics.State.Terminated.Reason, ics.State.Terminated.Message, ics.State.Terminated.ExitCode)
			return pw.GetPodStatus()
		}
	}

	// Main container state changes:
	// - Waiting & Running: Record state changes with detailed reason in pod watcher and continue to watch
	// - Terminated: Terminates the pod watcher goroutine, cancel context to cleans up, and reports the error immediately
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]

		switch {
		case cs.State.Waiting != nil:
			pw.setPodStatus(ctrWaiting, cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message, 0)
		case cs.State.Running != nil:
			pw.setPodStatus(ctrRunning, cs.Name, "Running", cs.State.Running.String(), 0)
		case cs.State.Terminated != nil:
			pw.setPodStatus(ctrTerminated, cs.Name, cs.State.Terminated.Reason, cs.State.Terminated.Message, cs.State.Terminated.ExitCode)
			return pw.GetPodStatus()
		}
	}

	// We don't expect any of these to happen, as ETL containers are supposed to constantly
	// listen to upcoming requests and never terminate, until manually stopped/deleted
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		err := fmt.Errorf("ETL Pod %s is in problematic phase: %s (expecting either %s or %s phase)",
			pod.Name, pod.Status.Phase, corev1.PodPending, corev1.PodRunning)
		nlog.Errorln(err, pw.GetPodStatus())
		return err
	}
	return nil
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

	pw.stopCh = cos.NewStopCh()
	pw.podCtx, pw.podCtxCancel = context.WithCancel(context.Background())
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

	if !wait || pw.GetPodStatus().State == ctrTerminated {
		for range pw.watcher.ResultChan() {
		}
		return
	}

	// Process remaining events
	for event := range pw.watcher.ResultChan() {
		if pod, ok := event.Object.(*corev1.Pod); ok {
			if err := pw._process(pod); err != nil {
				break
			}
		}
	}
}

// setPodStatus safely sets the pod status by mutex
func (pw *podWatcher) setPodStatus(state, cname, reason, message string, exitCode int32) {
	pw.psMutex.Lock()
	pw.recentPodStatus.State, pw.recentPodStatus.CtrName, pw.recentPodStatus.Reason, pw.recentPodStatus.Message = state, cname, reason, message
	pw.recentPodStatus.ExitCode = exitCode
	pw.psMutex.Unlock()
}

// GetPodStatus safely retrieves a copy of the pod status by mutex
func (pw *podWatcher) GetPodStatus() (rps k8s.PodStatus) {
	pw.psMutex.Lock()
	defer pw.psMutex.Unlock()

	if pw.recentPodStatus == nil {
		return rps
	}
	return *pw.recentPodStatus
}
