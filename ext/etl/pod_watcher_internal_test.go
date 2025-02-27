// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	reasonFmt  = "%s-reason-%d"
	messageFmt = "%s-message-%d"
)

var _ = Describe("ETLPodWatcherTest", func() {
	var (
		mockWatcher *mockWatcher
		pw          *podWatcher
	)

	BeforeEach(func() {
		// simulate the pw.start() procedure with a mocked watcher to avoid real K8s API get involved
		pw = newPodWatcher("test-pod")
		mockWatcher = newMockWatcher()
		pw.watcher = mockWatcher
		go pw.processEvents()
	})

	AfterEach(func() {
		if !mockWatcher.stopped {
			pw.stop(false)
		}
	})

	Context("Sequential Events", func() {
		It("Single Waiting Event", func() {
			ps := podStatus{cname: "server", state: ctrWaiting, reason: "test-reason", message: "test-message"}
			mockWatcher.send("main", ps, 0)
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.getPodStatus()).To(Equal(ps))
		})
		It("Multiple Waiting Events", func() {
			var (
				numEvents = 10
				testName  = "multiple-waiting-events"
			)
			for i := range numEvents {
				ps := podStatus{cname: "server", state: ctrWaiting, reason: fmt.Sprintf(reasonFmt, testName, i), message: fmt.Sprintf(messageFmt, testName, i)}
				mockWatcher.send("main", ps, 0)
			}
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.getPodStatus().cname).To(Equal("server"))
			Expect(pw.getPodStatus().state).To(Equal(ctrWaiting))
			Expect(pw.getPodStatus().reason).To(Equal(fmt.Sprintf(reasonFmt, testName, numEvents-1)))
			Expect(pw.getPodStatus().message).To(Equal(fmt.Sprintf(messageFmt, testName, numEvents-1)))
		})
		It("Discard Unknown Events", func() {
			var (
				numEvents = 3
				testName  = "discard-unknown-events"
			)
			for i := range numEvents {
				ps := podStatus{cname: "server", state: ctrWaiting, reason: fmt.Sprintf(reasonFmt, testName, i), message: fmt.Sprintf(messageFmt, testName, i)}
				mockWatcher.send("main", ps, 0)
			}

			// add unknown events, should all be discarded
			for range numEvents {
				mockWatcher.resultChan <- watch.Event{Object: nil}
			}
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.getPodStatus().cname).To(Equal("server"))
			Expect(pw.getPodStatus().state).To(Equal(ctrWaiting))
			Expect(pw.getPodStatus().reason).To(Equal(fmt.Sprintf(reasonFmt, testName, numEvents-1)))
			Expect(pw.getPodStatus().message).To(Equal(fmt.Sprintf(messageFmt, testName, numEvents-1)))
		})
	})

	Context("Graceful Termination", func() {
		It("Simple Termination", func() {
			var (
				numEvents = 3
				testName  = "simple-termination"
			)
			for i := range numEvents {
				ps := podStatus{cname: "server", state: ctrWaiting, reason: fmt.Sprintf(reasonFmt, testName, i), message: fmt.Sprintf(messageFmt, testName, i)}
				mockWatcher.send("main", ps, 1)
			}

			for i := range numEvents {
				ps := podStatus{cname: "server", state: ctrTerminated, reason: fmt.Sprintf(reasonFmt, testName, i), message: fmt.Sprintf(messageFmt, testName, i)}
				mockWatcher.send("main", ps, 1)
			}
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.getPodStatus().cname).To(Equal("server"))
			Expect(pw.getPodStatus().state).To(Equal(ctrTerminated))
			// the captured pod status should stay as the first received Terminated state event (number 0),
			// since the pod watcher goroutine exits on the first termination state chagne with non-zero exit code
			Expect(pw.getPodStatus().reason).To(Equal(fmt.Sprintf(reasonFmt, testName, 0)))
			Expect(pw.getPodStatus().message).To(Equal(fmt.Sprintf(messageFmt, testName, 0)))
		})
		It("Init Container Termination", func() {
			ps := podStatus{cname: "server-deps", state: ctrTerminated, reason: "init-container-termination-reason", message: "init-container-termination-message"}
			mockWatcher.send("init", ps, 1)
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.getPodStatus()).To(Equal(ps))
		})
		It("Manually Stopped without Wait", func() {
			ps := podStatus{cname: "server", state: ctrTerminated, reason: "manual-stop-reason", message: "manual-stop-message"}
			mockWatcher.send("main", ps, 0)
			pw.stop(false) // stop without wait

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.getPodStatus()).To(SatisfyAny(
				Equal(ps),
				Equal(podStatus{}),
			))
		})
	})
})

/////////////////
// mockWatcher //
/////////////////

type mockWatcher struct {
	resultChan chan watch.Event
	stopped    bool
}

// interface guard
var _ watch.Interface = (*mockWatcher)(nil)

func (mw *mockWatcher) Stop() {
	mw.stopped = true
	close(mw.resultChan)
}

func (mw *mockWatcher) ResultChan() <-chan watch.Event {
	return mw.resultChan
}

func newMockWatcher() *mockWatcher {
	return &mockWatcher{
		resultChan: make(chan watch.Event, 10),
	}
}

// send a mock K8s API event response to the resultChan
func (mw *mockWatcher) send(source string, ps podStatus, exitCode int32) {
	var cs = corev1.ContainerStatus{Name: ps.cname}

	// Assign the appropriate container state
	switch ps.state {
	case ctrWaiting:
		cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: ps.reason, Message: ps.message}}
	case ctrRunning:
		cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	case ctrTerminated:
		cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: ps.reason, Message: ps.message, ExitCode: exitCode}}
	default:
		Fail(fmt.Sprintf("Fail to send mock event. Unexpected container state: %q", ps.state))
	}

	// Send the event to resultChan based on source
	switch source {
	case "init":
		mw.resultChan <- watch.Event{
			Object: &corev1.Pod{
				Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{cs}},
			},
		}
	case "main":
		mw.resultChan <- watch.Event{
			Object: &corev1.Pod{
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{cs}},
			},
		}
	default:
		Fail(fmt.Sprintf("Fail to send mock event. Unexpected container source: %q", source))
	}
}
