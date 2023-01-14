// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/ext/etl/runtime"
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
//     of the objects. Transformation is defined by an ETL spec, which is a K8S
//     yaml spec file. The operations of an ETL are executed on the ETL container.
//
// ETL container:
//     The user's K8S pod which runs the container doing the transformation of
//     the objects. It is initiated by a target and runs on the same K8S node
//     running the target.
//
// On-the-fly transformation flow:
// 1. User initiates a custom ETL workload by executing one of the documented APIs
//    and providing either the corresponding docker image or a *transforming function* -
//    a piece of code that we further run using one of the pre-built `runtimes`
//    (see https://github.com/NVIDIA/aistore/blob/master/docs/etl.md).
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

type (
	// Aborter listens to smap changes and aborts the ETL on the target when
	// there is any change in targets membership. Aborter should be registered
	// on ETL init. It is unregistered by Stop function. The is no
	// synchronization between aborters on different targets. It is assumed that
	// if one target received smap with changed targets membership, eventually
	// each of the targets will receive it as well. Hence, all ETL containers
	// will be stopped.
	Aborter struct {
		t           cluster.Target
		currentSmap *cluster.Smap
		name        string
		mtx         sync.Mutex
	}

	StartOpts struct {
		Env map[string]string
	}
)

// interface guard
var _ cluster.Slistener = (*Aborter)(nil)

func newAborter(t cluster.Target, name string) *Aborter {
	return &Aborter{
		name:        name,
		t:           t,
		currentSmap: t.Sowner().Get(),
	}
}

func (e *Aborter) String() string {
	return fmt.Sprintf("etl-aborter-%s", e.name)
}

func (e *Aborter) ListenSmapChanged() {
	// New goroutine as kubectl calls can take a lot of time,
	// making other listeners wait.
	go func() {
		e.mtx.Lock()
		defer e.mtx.Unlock()
		newSmap := e.t.Sowner().Get()

		if newSmap.Version <= e.currentSmap.Version {
			return
		}

		if !newSmap.CompareTargets(e.currentSmap) {
			err := cmn.NewErrETL(&cmn.ETLErrCtx{
				TID:     e.t.SID(),
				ETLName: e.name,
			}, "targets have changed, aborting...")
			glog.Warning(err)
			// Stop will unregister `e` from smap listeners.
			if err := Stop(e.t, e.name, err); err != nil {
				glog.Error(err)
			}
		}

		e.currentSmap = newSmap
	}()
}

// (common for both `InitCode` and `InitSpec` flows)
func InitSpec(t cluster.Target, msg *InitSpecMsg, xactID string, opts StartOpts) error {
	errCtx, podName, svcName, err := start(t, msg, xactID, opts)
	if err != nil {
		glog.Warning(cmn.NewErrETL(errCtx, "%s: cleanup after unsuccessful Start", t))
		if errV := cleanupEntities(errCtx, podName, svcName); errV != nil {
			glog.Error(errV)
		}
	}
	return err
}

// Given user message `InitCodeMsg`:
// - make the corresponding assorted substitutions in the etl/runtime/podspec.yaml spec, and
// - execute `InitSpec` with the modified podspec
// See also: etl/runtime/podspec.yaml
func InitCode(t cluster.Target, msg *InitCodeMsg, xactID string) error {
	var (
		ftp      = fromToPairs(msg)
		replacer = strings.NewReplacer(ftp...)
	)
	r, exists := runtime.Get(msg.Runtime)
	debug.Assert(exists, msg.Runtime) // must've been checked by proxy

	podSpec := replacer.Replace(r.PodSpec())

	// Start ETL
	// (the point where InitCode flow converges w/ InitSpec)
	return InitSpec(t,
		&InitSpecMsg{msg.InitMsgBase, []byte(podSpec)},
		xactID,
		StartOpts{Env: map[string]string{
			r.CodeEnvName(): string(msg.Code),
			r.DepsEnvName(): string(msg.Deps),
		}})
}

// generate (from => to) replacements
//
//nolint:gocritic // appendCombine vs readability
func fromToPairs(msg *InitCodeMsg) (ftp []string) {
	var (
		chunk string
		flags string
		name  = msg.IDX
	)
	ftp = make([]string, 0, 16)
	ftp = append(ftp, "<NAME>", name)
	ftp = append(ftp, "<COMM_TYPE>", msg.CommTypeX)

	// chunk == 0 means no chunks (and no streaming) - in other words,
	// reading the entire payload in memory, and then transforming in one shot
	if msg.ChunkSize > 0 {
		chunk = "\"" + strconv.FormatInt(msg.ChunkSize, 10) + "\""
	}
	ftp = append(ftp, "<CHUNK_SIZE>", chunk)

	if msg.Flags > 0 {
		flags = "\"" + strconv.FormatInt(msg.Flags, 10) + "\""
	}
	ftp = append(ftp, "<FLAGS>", flags)

	// functions
	ftp = append(ftp, "<FUNC_TRANSFORM>", msg.Funcs.Transform)

	switch msg.CommTypeX {
	case Hpush, Hpull, Hrev:
		ftp = append(ftp, "<COMMAND>", "['sh', '-c', 'python /server.py']")
	case HpushStdin:
		ftp = append(ftp, "<COMMAND>", "['python /code/code.py']")
	default:
		cos.AssertMsg(false, msg.CommTypeX)
	}
	return
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
// * errCtx - ETL error context
// * podName - non-empty if at least one attempt of creating pod was executed
// * svcName - non-empty if at least one attempt of creating service was executed
// * err - any error occurred that should be passed on.
func start(t cluster.Target, msg *InitSpecMsg, xactID string, opts StartOpts) (errCtx *cmn.ETLErrCtx, podName, svcName string, err error) {
	debug.Assert(k8s.NodeName != "") // checked above

	errCtx = &cmn.ETLErrCtx{TID: t.SID(), ETLName: msg.IDX}
	boot := &etlBootstrapper{errCtx: errCtx, t: t, env: opts.Env}
	boot.msg = *msg

	// Parse spec template and fill Pod object with necessary fields.
	if err = boot.createPodSpec(); err != nil {
		return
	}

	boot.createServiceSpec()

	// 1. Cleanup previously started entities, if any.
	errCleanup := cleanupEntities(errCtx, boot.pod.Name, boot.svc.Name)
	debug.AssertNoErr(errCleanup)

	// 2. Creating service.
	svcName = boot.svc.GetName()
	if err = boot.createEntity(k8s.Svc); err != nil {
		return
	}
	// 3. Creating pod.
	podName = boot.pod.GetName()
	if err = boot.createEntity(k8s.Pod); err != nil {
		return
	}
	if err = boot.waitPodReady(); err != nil {
		return
	}
	if err = boot.setupConnection(); err != nil {
		return
	}

	boot.setupXaction(xactID)

	// finally, add Communicator to the runtime registry
	c := makeCommunicator(commArgs{
		listener:     newAborter(t, msg.IDX),
		bootstrapper: boot,
	})
	if err = reg.add(msg.IDX, c); err != nil {
		return
	}
	t.Sowner().Listeners().Reg(c)
	return
}

// Stop deletes all occupied by the ETL resources, including Pods and Services.
// It unregisters ETL smap listener.
func Stop(t cluster.Target, id string, errCause error) error {
	errCtx := &cmn.ETLErrCtx{
		TID:     t.SID(),
		ETLName: id,
	}

	// Abort all running offline ETLs.
	xreg.AbortKind(errCause, apc.ActETLBck)

	c, err := GetCommunicator(id, t.Snode())
	if err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}
	errCtx.PodName = c.PodName()
	errCtx.SvcName = c.SvcName()

	if err := cleanupEntities(errCtx, c.PodName(), c.SvcName()); err != nil {
		return err
	}

	if c := reg.del(id); c != nil {
		t.Sowner().Listeners().Unreg(c)
	}

	c.Stop()

	return nil
}

// StopAll terminates all running ETLs.
func StopAll(t cluster.Target) {
	if k8s.Detect() != nil {
		return
	}
	for _, e := range List() {
		if err := Stop(t, e.Name, nil); err != nil {
			glog.Error(err)
		}
	}
}

func GetCommunicator(etlName string, lsnode *cluster.Snode) (Communicator, error) {
	c, exists := reg.get(etlName)
	if !exists {
		return nil, cmn.NewErrNotFound("%s: etl[%s]", lsnode, etlName)
	}
	return c, nil
}

func List() []Info { return reg.list() }

func PodLogs(t cluster.Target, transformID string) (logs PodLogsMsg, err error) {
	c, err := GetCommunicator(transformID, t.Snode())
	if err != nil {
		return logs, err
	}
	client, err := k8s.GetClient()
	if err != nil {
		return logs, err
	}
	b, err := client.Logs(c.PodName())
	if err != nil {
		return logs, err
	}
	return PodLogsMsg{
		TargetID: t.SID(),
		Logs:     b,
	}, nil
}

func PodHealth(t cluster.Target, etlName string) (stats *PodHealthMsg, err error) {
	var (
		c      Communicator
		client k8s.Client
	)
	if c, err = GetCommunicator(etlName, t.Snode()); err != nil {
		return
	}
	if client, err = k8s.GetClient(); err != nil {
		return
	}

	cpuUsed, memUsed, err := client.Health(c.PodName())
	if err != nil {
		if metricsErr := client.CheckMetricsAvailability(); metricsErr != nil {
			err = fmt.Errorf("%v; failed to fetch metrics from Kubernetes: %v", metricsErr, err)
		}
		return nil, err
	}
	return &PodHealthMsg{
		TargetID: t.SID(),
		CPU:      cpuUsed,
		Mem:      memUsed,
	}, nil
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

	err = wait.PollImmediate(time.Second, time.Minute, func() (done bool, err error) {
		exists, err := client.CheckExists(entityType, entityName)
		if err != nil {
			return false, err
		}
		return !exists, nil
	})
	if err != nil {
		return cmn.NewErrETL(errCtx, err.Error())
	}
	return nil
}

func podConditionsToString(conditions []corev1.PodCondition) string {
	parts := make([]string, 0, len(conditions))
	for _, cond := range conditions {
		parts = append(parts, podConditionToString(cond))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func podConditionToString(cond corev1.PodCondition) string {
	parts := []string{
		fmt.Sprintf("type: %q", cond.Type),
		fmt.Sprintf("status: %q", cond.Status),
	}
	if cond.Reason != "" {
		parts = append(parts, fmt.Sprintf("reason: %q", cond.Reason))
	}
	if cond.Message != "" {
		parts = append(parts, fmt.Sprintf("msg: %q", cond.Message))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
