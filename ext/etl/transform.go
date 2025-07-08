// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact/xreg"

	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// Built-in label: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#built-in-node-labels.
	nodeNameLabel = "kubernetes.io/hostname"

	// Recommended labels: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/.
	appK8sNameLabel      = "app.kubernetes.io/name"
	appK8sComponentLabel = "app.kubernetes.io/component"

	// ETL Custom labels.
	podNameLabel = "nvidia.com/ais-etl-name"
	svcNameLabel = "nvidia.com/ais-etl-name"

	// ETL Pod's label describing which target ETL is associated with.
	podNodeLabel   = "nvidia.com/ais-etl-node"
	podTargetLabel = "nvidia.com/ais-etl-target"
)

// Definitions:
//
// ETL:
//     Refers to Extract-Transform-Load, which allows a user to do transformation
//     of the objects. Transformation is defined by an ETL spec, which is a K8s
//     yaml spec file. The operations of an ETL are executed on the ETL container.
//
// ETL container:
//     The user's K8s pod which runs the container doing the transformation of
//     the objects. It is initiated by a target and runs on the same K8s node
//     running the target.
//
// On-the-fly transformation flow:
// 1. User initiates a custom ETL workload by executing one of the documented APIs
//    and providing either the corresponding docker image or a *transforming function* -
//    a piece of code that we further run using one of the pre-built `runtimes`
//    (see https://github.com/NVIDIA/aistore/blob/main/docs/etl.md).
// 2. The API call results in deploying multiple ETL containers (K8s pods)
//    simultaneously: one container per storage target.
// 3. Each target creates a local `Communicator` instance that is based on the specified
//    `communication type`.
// 4. Client-side application (e.g., PyTorch or TensorFlow based training model)
//    starts (randomly) reading the data from a given dataset.
// 5. User-defined transformation is then performed using `Communicator.Do()`
//    on each read objects, on a per-object (or shard) basis.
// 6. Finally, the ETL container is stopped using the `Stop` API. In response,
//    each ais target in the cluster deletes its local ETL container (K8s pod).
//
// Limitations of the current implementation (soon to be removed):
//
// * No idle timeout for a ETL container. It keeps running unless explicitly
//   stopped by invoking the `Stop` API.
//
// * Delete of an ETL container is done in two stages. First we gracefully try to
//   terminate the pod with a 30s timeout. Upon failure to do so, we perform
//   a force delete.
//
// * Recreating an ETL container with the same name will delete all running
//   containers with the same name.

// (common for both `InitSpec` and `ETLSpec` flows)
func Init(msg InitMsg, xid, secret string) (core.Xact, error) {
	config := cmn.GCO.Get()
	podName, svcName, xctn, err := start(msg, xid, secret, config)
	if err != nil {
		return nil, err
	}

	if cmn.Rom.FastV(4, cos.SmoduleETL) {
		nlog.Infof("started etl[%s], msg %s, pod %s, svc %s", msg.Name(), msg, podName, svcName)
	}
	return xctn, nil
}

// cleanupEntities removes provided entities. It tries its best to remove all
// entities so it doesn't stop when encountering an error.
func cleanupEntities(errCtx *cmn.ETLErrCtx, podName, svcName string) (err error) {
	if svcName != "" {
		if deleteErr := deleteEntity(errCtx, k8s.Svc, svcName); deleteErr != nil {
			err = deleteErr
		}
	}

	if podName != "" {
		if deleteErr := deleteEntity(errCtx, k8s.Pod, podName); deleteErr != nil {
			err = deleteErr
		}
	}

	return
}

// (does the heavy-lifting)
// Returns:
// * podName - non-empty if at least one attempt of creating pod was executed
// * svcName - non-empty if at least one attempt of creating service was executed
// * err - any error occurred that should be passed on.
func start(msg InitMsg, xid, secret string, config *cmn.Config) (podName, svcName string, xctn core.Xact, err error) {
	var (
		comm    Communicator
		podAddr string
		errCtx  = &cmn.ETLErrCtx{TID: core.T.SID(), ETLName: msg.Name()}
		boot    = &etlBootstrapper{
			msg:    msg,
			errCtx: errCtx,
			config: config,
			secret: secret,
		}
	)

	client, err := k8s.GetClient()
	if err != nil {
		return podName, svcName, nil, err
	}
	boot.k8sClient = client

	debug.Assert(xid != "")
	// 1. Parse spec template and fill Pod object with necessary fields.
	if err = boot.createPodSpec(); err != nil {
		return podName, svcName, nil, err
	}
	boot.createServiceSpec()
	podName, svcName = boot.pod.GetName(), boot.svc.GetName()

	// 2. Create communicator
	if comm, err = initComm(msg, xid, secret, boot); err != nil {
		return podName, svcName, nil, err
	}

	// 3. Cleanup previously started entities, if any.
	err = cleanupEntities(errCtx, boot.pod.GetName(), boot.svc.GetName())
	debug.AssertNoErr(err)

	// initialize pod watcher
	boot.pw = newPodWatcher(boot.pod.GetName(), comm.Xact())
	if err = boot.pw.start(); err != nil {
		goto cleanup
	}

	// 4. Creating Kubernetes resources.
	if err = boot.createEntity(k8s.Svc); err != nil {
		goto cleanup
	}

	if err = boot.createEntity(k8s.Pod); err != nil {
		goto cleanup
	}

	// 5. Waiting for pod's readiness
	if err = boot.waitPodReady(boot.pw.podCtx); err != nil {
		goto cleanup
	}

	if podAddr, err = boot.getPodAddr(); err != nil {
		goto cleanup
	}
	if _, err = comm.setupConnection("http://", podAddr); err != nil {
		goto cleanup
	}

	nlog.Infof("pod %q is running, %+v, %s", podName, msg, boot.errCtx)

	return podName, svcName, comm.Xact(), nil

cleanup: // initialization failed
	Stop(msg.Name(), err)
	boot.errCtx.PodStatus = boot.pw.GetPodStatus()
	return podName, svcName, nil, cmn.NewErrETL(boot.errCtx, err.Error())
}

func StopByXid(xid string, errCause error) error {
	comm := mgr.getByXid(xid)
	if comm == nil {
		return cos.NewErrNotFound(core.T, "etl with xid "+xid+" not found")
	}
	return Stop(comm.ETLName(), errCause)
}

// three cases to call Stop()
// 1. user's DELETE requests
// 2. initialization failed
// 3. xaction abort (StopByXid)
func Stop(etlName string, errCause error) (err error) {
	comm, boot := mgr.getByName(etlName)
	if comm == nil {
		return cos.NewErrNotFound(core.T, etlName+" not found")
	}

	// Note: comm.stop() is protected by atomic bool, run only once
	if err := comm.stop(); err != nil {
		return err
	}
	boot.pw.stop(true)
	mgr.del(etlName)

	// Abort all running offline ETLs.
	xreg.AbortKind(errCause, apc.ActETLBck) // TODO: abort only related offline transforms

	errCtx := &cmn.ETLErrCtx{
		PodName:   boot.pod.GetName(),
		SvcName:   boot.svc.GetName(),
		PodStatus: boot.pw.GetPodStatus(),
		ETLName:   etlName,
		TID:       core.T.SID(),
	}
	return cleanupEntities(errCtx, boot.pod.GetName(), boot.svc.GetName())
}

func Delete(etlName string) error { return Stop(etlName, cmn.ErrXactUserAbort) }

// StopAll terminates all running ETLs.
func StopAll() {
	if !k8s.IsK8s() {
		return
	}
	for _, e := range List() {
		if err := Stop(e.Name, nil); err != nil {
			nlog.Errorln(err)
		}
	}
}

// GetCommunicator retrieves the Communicator from registry by etl name
// Returns an error if not found or not in the Running stage.
func GetCommunicator(etlName string) (Communicator, error) {
	comm, _ := mgr.getByName(etlName)
	if comm == nil {
		return nil, cos.NewErrNotFound(core.T, etlName)
	}
	return comm, nil
}

func GetInitMsg(etlName string) (InitMsg, error) {
	cc, err := GetCommunicator(etlName)
	if err != nil {
		return nil, err
	}
	return cc.getInitMsg(), nil
}

func GetOfflineTransform(etlName string, xctn core.Xact) (getROC core.GetROC, xetl *XactETL, session Session, err error) {
	cc, err := GetCommunicator(etlName)
	if err != nil {
		return nil, nil, nil, err
	}

	switch comm := cc.(type) {
	case httpCommunicator:
		return comm.OfflineTransform, comm.Xact(), nil, nil
	case statefulCommunicator:
		session, err := comm.createSession(xctn, offlineSessionMultiplier)
		if err != nil {
			return nil, nil, nil, err
		}
		return nil, comm.Xact(), session, nil
	default:
		debug.Assert(false, "unknown communicator type")
		return nil, nil, nil, cos.NewErrNotFound(core.T, etlName+" unknown communicator type")
	}
}

func List() []Info { return mgr.list() }

func PodLogs(etlName string) (logs Logs, err error) {
	_, boot := mgr.getByName(etlName)
	if boot == nil {
		return logs, cos.NewErrNotFound(core.T, etlName)
	}
	client, err := k8s.GetClient()
	if err != nil {
		return logs, err
	}
	b, err := client.Logs(boot.pod.GetName())
	if err != nil {
		return logs, err
	}
	return Logs{
		TargetID: core.T.SID(),
		Logs:     b,
	}, nil
}

func PodHealth(etlName string) (string, error) {
	_, boot := mgr.getByName(etlName)
	if boot == nil {
		return "", cos.NewErrNotFound(core.T, etlName)
	}
	client, err := k8s.GetClient()
	if err != nil {
		return "", err
	}
	return client.Health(boot.pod.GetName())
}

func PodMetrics(etlName string) (*CPUMemUsed, error) {
	_, boot := mgr.getByName(etlName)
	if boot == nil {
		return nil, cos.NewErrNotFound(core.T, etlName)
	}
	client, err := k8s.GetClient()
	if err != nil {
		return nil, err
	}
	cpuUsed, memUsed, err := k8s.Metrics(boot.pod.GetName())
	if err == nil {
		return &CPUMemUsed{TargetID: core.T.SID(), CPU: cpuUsed, Mem: memUsed}, nil
	}
	if cos.IsErrNotFound(err) {
		return nil, err
	}
	if metricsErr := client.CheckMetricsAvailability(); metricsErr != nil {
		err = fmt.Errorf("%v; failed to fetch metrics from Kubernetes: %v", metricsErr, err)
	}
	return nil, err
}

// Pod conditions include enumerated lifecycle states, such as `PodScheduled`,
// `ContainersReady`, `Initialized`, `Ready`
// (see https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle).
// First, we check that the Pod is still running (neither succeeded, nor failed),
// and secondly, whether it contains `Ready` condition.
func checkPodReady(client k8s.Client, podName string) (ready bool, err error) {
	var p *corev1.Pod
	if p, err = client.Pod(podName); err != nil {
		return false, err
	}

	// Pod has run to completion, either by failing or by succeeding. We don't
	// expect any of these to happen, as ETL containers are supposed to constantly
	// listen to upcoming requests and never terminate.
	switch p.Status.Phase {
	case corev1.PodFailed, corev1.PodSucceeded:
		return false, fmt.Errorf(
			"pod ran to completion (phase: %s), state message: %q",
			p.Status.Phase, p.Status.Message,
		)
	}

	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true, nil
		}
	}

	return false, nil
}

func deleteEntity(errCtx *cmn.ETLErrCtx, entityType, entityName string) error {
	client, err := k8s.GetClient()
	if err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}

	// Remove entity immediately (ignoring not found).
	if err = client.Delete(entityType, entityName); err != nil {
		if k8sErrors.IsNotFound(err) {
			return nil
		}
		return cmn.NewErrETL(errCtx, err.Error())
	}

	// wait
	interval := cos.ProbingFrequency(DefaultInitTimeout)
	err = wait.PollUntilContextTimeout(context.Background(), interval, DefaultInitTimeout, false, /*immediate*/
		func(context.Context) (done bool, err error) {
			var exists bool
			exists, err = client.CheckExists(entityType, entityName)
			if err == nil {
				done = !exists
			}
			return
		},
	)
	if err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}
	return nil
}
