// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact/xreg"

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

	xreg.Init()
	hk.Init(false)

	BeforeEach(func() {
		// simulate the pw.start() procedure with a mocked watcher to avoid real K8s API get involved
		boot := &etlBootstrapper{xctn: mock.NewXact(apc.ActETLInline), errCtx: &cmn.ETLErrCtx{}}
		pw = newPodWatcher("test-pod", boot)
		mockWatcher = newMockWatcher()
		pw.watcher = mockWatcher
		pw.stopCh = cos.NewStopCh()
		pw.podCtx, pw.podCtxCancel = context.WithCancel(context.Background())
		go pw.processEvents()
	})

	AfterEach(func() {
		if !mockWatcher.stopped {
			pw.stop(false)
		}
	})

	Context("Sequential Events", func() {
		It("Single Waiting Event", func() {
			ps := k8s.PodStatus{CtrName: "server", State: ctrWaiting, Reason: "test-reason", Message: "test-message"}
			mockWatcher.send("main", ps, 0)
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.GetPodStatus()).To(Equal(ps))
		})
		It("Multiple Waiting Events", func() {
			var (
				numEvents = 10
				testName  = "multiple-waiting-events"
			)
			for i := range numEvents {
				ps := k8s.PodStatus{CtrName: "server", State: ctrWaiting, Reason: fmt.Sprintf(reasonFmt, testName, i), Message: fmt.Sprintf(messageFmt, testName, i)}
				mockWatcher.send("main", ps, 0)
			}
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.GetPodStatus().CtrName).To(Equal("server"))
			Expect(pw.GetPodStatus().State).To(Equal(ctrWaiting))
			Expect(pw.GetPodStatus().Reason).To(Equal(fmt.Sprintf(reasonFmt, testName, numEvents-1)))
			Expect(pw.GetPodStatus().Message).To(Equal(fmt.Sprintf(messageFmt, testName, numEvents-1)))
		})
		It("Discard Unknown Events", func() {
			var (
				numEvents = 3
				testName  = "discard-unknown-events"
			)
			for i := range numEvents {
				ps := k8s.PodStatus{CtrName: "server", State: ctrWaiting, Reason: fmt.Sprintf(reasonFmt, testName, i), Message: fmt.Sprintf(messageFmt, testName, i)}
				mockWatcher.send("main", ps, 0)
			}

			// add unknown events, should all be discarded
			for range numEvents {
				mockWatcher.resultChan <- watch.Event{Object: nil}
			}
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.GetPodStatus().CtrName).To(Equal("server"))
			Expect(pw.GetPodStatus().State).To(Equal(ctrWaiting))
			Expect(pw.GetPodStatus().Reason).To(Equal(fmt.Sprintf(reasonFmt, testName, numEvents-1)))
			Expect(pw.GetPodStatus().Message).To(Equal(fmt.Sprintf(messageFmt, testName, numEvents-1)))
		})
	})

	Context("Graceful Termination", func() {
		It("Simple Termination", func() {
			var (
				numEvents = 3
				testName  = "simple-termination"
			)
			for i := range numEvents {
				ps := k8s.PodStatus{CtrName: "server", State: ctrWaiting, Reason: fmt.Sprintf(reasonFmt, testName, i), Message: fmt.Sprintf(messageFmt, testName, i)}
				mockWatcher.send("main", ps, 0)
			}

			for i := range numEvents {
				ps := k8s.PodStatus{CtrName: "server", State: ctrTerminated, Reason: fmt.Sprintf(reasonFmt, testName, i), Message: fmt.Sprintf(messageFmt, testName, i), ExitCode: 1}
				mockWatcher.send("main", ps, 1)
			}
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.GetPodStatus().CtrName).To(Equal("server"))
			Expect(pw.GetPodStatus().State).To(Equal(ctrTerminated))
			// the captured pod status should stay as the first received Terminated state event (number 0),
			// since the pod watcher goroutine exits on the first termination state change with non-zero exit code
			Expect(pw.GetPodStatus().Reason).To(Equal(fmt.Sprintf(reasonFmt, testName, 0)))
			Expect(pw.GetPodStatus().Message).To(Equal(fmt.Sprintf(messageFmt, testName, 0)))
		})
		It("Init Container Termination", func() {
			ps := k8s.PodStatus{CtrName: "server-deps", State: ctrTerminated, Reason: "init-container-termination-reason", Message: "init-container-termination-message", ExitCode: 1}
			mockWatcher.send("init", ps, 1)
			pw.stop(true)

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.GetPodStatus()).To(Equal(ps))
		})
		It("Manually Stopped without Wait", func() {
			ps := k8s.PodStatus{CtrName: "server", State: ctrTerminated, Reason: "manual-stop-reason", Message: "manual-stop-message"}
			mockWatcher.send("main", ps, 0)
			pw.stop(false) // stop without wait

			Expect(mockWatcher.stopped).To(BeTrue(), "mock watcher is not stopped after pw.stop() called")
			Expect(pw.podCtx.Done()).To(BeClosed(), "pw.podCtx.Done() is not closed after pw.stop() called")

			Expect(pw.GetPodStatus()).To(SatisfyAny(
				Equal(ps),
				Equal(k8s.PodStatus{}),
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
func (mw *mockWatcher) send(source string, ps k8s.PodStatus, exitCode int32) {
	var cs = corev1.ContainerStatus{Name: ps.CtrName}

	// Assign the appropriate container state
	switch ps.State {
	case ctrWaiting:
		cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: ps.Reason, Message: ps.Message}}
	case ctrRunning:
		cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	case ctrTerminated:
		cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: ps.Reason, Message: ps.Message, ExitCode: exitCode}}
	default:
		Fail(fmt.Sprintf("Fail to send mock event. Unexpected container state: %q", ps.State))
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
