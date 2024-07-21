// Package tetl provides helpers for ETL.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tetl

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
	corev1 "k8s.io/api/core/v1"
)

const (
	commTypeAnnotation    = "communication_type"
	waitTimeoutAnnotation = "wait_timeout"

	Tar2TF        = "tar2tf"
	Echo          = "transformer-echo"
	EchoGolang    = "echo-go"
	MD5           = "transformer-md5"
	Tar2tfFilters = "tar2tf-filters"
	tar2tfFilter  = `
{
  "conversions": [
    { "type": "Decode", "ext_name": "png"},
    { "type": "Rotate", "ext_name": "png"}
  ],
  "selections": [
    { "ext_name": "png" },
    { "ext_name": "cls" }
  ]
}
`
)

var (
	links = map[string]string{
		MD5:           "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml",
		Tar2TF:        "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/tar2tf/pod.yaml",
		Tar2tfFilters: "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/tar2tf/pod.yaml",
		Echo:          "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/echo/pod.yaml",
		EchoGolang:    "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/go_echo/pod.yaml",
	}

	client = &http.Client{}
)

func validateETLName(name string) error {
	if _, ok := links[name]; !ok {
		return fmt.Errorf("%s is invalid etlName, expected predefined (%s, %s, %s)", name, Echo, Tar2TF, MD5)
	}
	return nil
}

func GetTransformYaml(etlName string) ([]byte, error) {
	if err := validateETLName(etlName); err != nil {
		return nil, err
	}

	var resp *http.Response
	// with retry in case github in unavailable for a moment
	err := cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call: func() (code int, err error) {
			resp, err = client.Get(links[etlName]) //nolint:bodyclose // see defer close below
			return
		},
		Action:   "get transform yaml for ETL[" + etlName + "]",
		SoftErr:  3,
		HardErr:  1,
		IsClient: true,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := cos.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(b))
	}

	specStr := os.Expand(string(b), func(v string) string {
		// Hack: Neither os.Expand, nor os.ExpandEnv supports bash env variable default-value
		// syntax. The whole ${VAR:-default} is matched as v.
		if strings.Contains(v, "COMMUNICATION_TYPE") {
			return etl.Hpull
		}
		if strings.Contains(v, "DOCKER_REGISTRY_URL") {
			return "aistore"
		}
		if etlName == Tar2tfFilters {
			if strings.Contains(v, "OPTION_KEY") {
				return "--spec"
			}
			if strings.Contains(v, "OPTION_VALUE") {
				return tar2tfFilter
			}
		}
		return ""
	})

	return []byte(specStr), nil
}

func StopAndDeleteETL(t *testing.T, bp api.BaseParams, etlName string) {
	if t.Failed() {
		tlog.Logln("Fetching logs from ETL containers")
		if logsByTarget, err := api.ETLLogs(bp, etlName); err == nil {
			for _, etlLogs := range logsByTarget {
				tlog.Logln(headETLLogs(etlLogs, 10*cos.KiB))
			}
		} else {
			tlog.Logf("Error retrieving ETL[%s] logs: %v\n", etlName, err)
		}
	}
	tlog.Logf("Stopping ETL[%s]\n", etlName)

	if err := api.ETLStop(bp, etlName); err != nil {
		tlog.Logf("Stopping ETL[%s] failed; err %v\n", etlName, err)
	} else {
		tlog.Logf("ETL[%s] stopped\n", etlName)
	}
	err := api.ETLDelete(bp, etlName)
	tassert.CheckFatal(t, err)
}

func headETLLogs(etlLogs etl.Logs, maxLen int) string {
	logs, l := etlLogs.Logs, len(etlLogs.Logs)
	if maxLen < l {
		logs = logs[:maxLen]
	}
	str := fmt.Sprintf("%s logs:\n%s", meta.Tname(etlLogs.TargetID), string(logs))
	if maxLen < l {
		str += fmt.Sprintf("\nand %d bytes more...", l-maxLen)
	}
	return str
}

func WaitForContainersStopped(t *testing.T, bp api.BaseParams) {
	tlog.Logln("Waiting for ETL containers to stop...")
	var (
		etls         etl.InfoList
		stopDeadline = time.Now().Add(20 * time.Second)
		interval     = 2 * time.Second
		err          error
	)

	for {
		etls, err = api.ETLList(bp)
		tassert.CheckFatal(t, err)
		if len(etls) == 0 {
			tlog.Logln("ETL containers stopped successfully")
			return
		}
		if time.Now().After(stopDeadline) {
			break
		}
		tlog.Logf("ETLs %+v still running, waiting %s... \n", etls, interval)
		time.Sleep(interval)
	}

	err = fmt.Errorf("expected all ETLs to stop, got %+v still running", etls)
	tassert.CheckFatal(t, err)
}

func WaitForAborted(bp api.BaseParams, xid, kind string, timeout time.Duration) error {
	tlog.Logf("Waiting for ETL x-%s[%s] to abort...\n", kind, xid)
	args := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: timeout /* total timeout */}
	status, err := api.WaitForXactionIC(bp, &args)
	if err == nil {
		if !status.Aborted() {
			err = fmt.Errorf("expected ETL x-%s[%s] status to indicate 'abort', got: %+v", kind, xid, status)
		}
		return err
	}
	tlog.Logf("Aborting ETL x-%s[%s]\n", kind, xid)
	if abortErr := api.AbortXaction(bp, &args); abortErr != nil {
		tlog.Logf("Nested error: failed to abort upon api.wait failure: %v\n", abortErr)
	}
	return err
}

// NOTE: relies on x-kind to choose the waiting method
// TODO -- FIXME: remove and simplify - here and everywhere
func WaitForFinished(bp api.BaseParams, xid, kind string, timeout time.Duration) (err error) {
	tlog.Logf("Waiting for ETL x-%s[%s] to finish...\n", kind, xid)
	args := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: timeout /* total timeout */}
	if xact.IdlesBeforeFinishing(kind) {
		err = api.WaitForXactionIdle(bp, &args)
	} else {
		_, err = api.WaitForXactionIC(bp, &args)
	}
	if err == nil {
		return
	}
	tlog.Logf("Aborting ETL x-%s[%s]\n", kind, xid)
	if abortErr := api.AbortXaction(bp, &args); abortErr != nil {
		tlog.Logf("Nested error: failed to abort upon api.wait failure: %v\n", abortErr)
	}
	return err
}

func ReportXactionStatus(bp api.BaseParams, xid string, stopCh *cos.StopCh, interval time.Duration, totalObj int) {
	go func() {
		var (
			xactStart = time.Now()
			etlTicker = time.NewTicker(interval)
		)
		defer etlTicker.Stop()
		for {
			select {
			case <-etlTicker.C:
				// Check number of objects transformed.
				xs, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid})
				if err != nil {
					tlog.Logf("Failed to get x-etl[%s] stats: %v\n", xid, err)
					continue
				}
				locObjs, outObjs, inObjs := xs.ObjCounts(xid)
				tlog.Logf("ETL[%s] progress: (objs=%d, outObjs=%d, inObjs=%d) out of %d objects\n",
					xid, locObjs, outObjs, inObjs, totalObj)
				locBytes, outBytes, inBytes := xs.ByteCounts(xid)
				bps := float64(locBytes+outBytes) / time.Since(xactStart).Seconds()
				bpsStr := cos.ToSizeIEC(int64(bps), 2) + "/s"
				tlog.Logf("ETL[%s] progress: (bytes=%d, outBytes=%d, inBytes=%d), %sBps\n",
					xid, locBytes, outBytes, inBytes, bpsStr)
			case <-stopCh.Listen():
				return
			}
		}
	}()
}

func InitSpec(t *testing.T, bp api.BaseParams, etlName, comm string) (xid string) {
	tlog.Logf("InitSpec ETL[%s], communicator %s\n", etlName, comm)

	msg := &etl.InitSpecMsg{}
	msg.IDX = etlName
	msg.CommTypeX = comm
	spec, err := GetTransformYaml(etlName)
	tassert.CheckFatal(t, err)
	msg.Spec = spec
	tassert.Fatalf(t, msg.Name() == etlName, "%q vs %q", msg.Name(), etlName) // assert

	xid, err = api.ETLInit(bp, msg)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, cos.IsValidUUID(xid), "expected valid xaction ID, got %q", xid)

	tlog.Logf("ETL %q: running x-etl-spec[%s]\n", etlName, xid)

	// reread `InitMsg` and compare with the specified
	etlMsg, err := api.ETLGetInitMsg(bp, etlName)
	tassert.CheckFatal(t, err)

	initSpec := etlMsg.(*etl.InitSpecMsg)
	tassert.Errorf(t, initSpec.Name() == etlName, "expected etlName %s != %s", etlName, initSpec.Name())
	tassert.Errorf(t, initSpec.CommType() == comm, "expected communicator type %s != %s", comm, initSpec.CommType())
	tassert.Errorf(t, bytes.Equal(spec, initSpec.Spec), "pod specs differ")

	return
}

func InitCode(t *testing.T, bp api.BaseParams, msg *etl.InitCodeMsg) (xid string) {
	id, err := api.ETLInit(bp, msg)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, cos.IsValidUUID(id), "expected valid xaction ID, got %q", xid)
	xid = id

	// reread `InitMsg` and compare with the specified
	etlMsg, err := api.ETLGetInitMsg(bp, msg.Name())
	tassert.CheckFatal(t, err)

	initCode := etlMsg.(*etl.InitCodeMsg)
	tassert.Errorf(t, initCode.Name() == msg.Name(), "expected etlName %q != %q", msg.Name(), initCode.Name())
	tassert.Errorf(t, msg.CommType() == "" || initCode.CommType() == msg.CommType(),
		"expected communicator type %s != %s", msg.CommType(), initCode.CommType())
	tassert.Errorf(t, msg.Runtime == initCode.Runtime, "expected runtime %s != %s", msg.Runtime, initCode.Runtime)
	tassert.Errorf(t, bytes.Equal(msg.Code, initCode.Code), "ETL codes differ")
	tassert.Errorf(t, bytes.Equal(msg.Deps, initCode.Deps), "ETL dependencies differ")

	return
}

func ETLBucketWithCleanup(t *testing.T, bp api.BaseParams, bckFrom, bckTo cmn.Bck, msg *apc.TCBMsg) string {
	xid, err := api.ETLBucket(bp, bckFrom, bckTo, msg)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		tools.DestroyBucket(t, bp.URL, bckTo)
	})

	tlog.Logf("ETL[%s]: running %s => %s xaction %q\n",
		msg.Transform.Name, bckFrom.Cname(""), bckTo.Cname(""), xid)
	return xid
}

func ETLShouldBeRunning(t *testing.T, params api.BaseParams, etlName string) {
	etls, err := api.ETLList(params)
	tassert.CheckFatal(t, err)
	for _, etl := range etls {
		if etlName == etl.Name {
			return
		}
	}
	t.Fatalf("etl[%s] is not running (%v)", etlName, etls)
}

func ETLShouldNotBeRunning(t *testing.T, params api.BaseParams, etlName string) {
	etls, err := api.ETLList(params)
	tassert.CheckFatal(t, err)
	for _, etl := range etls {
		if etlName == etl.Name {
			t.Fatalf("expected etl[%s] to be stopped (%v)", etlName, etls)
		}
	}
}

func CheckNoRunningETLContainers(t *testing.T, params api.BaseParams) {
	etls, err := api.ETLList(params)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(etls) == 0, "Expected no ETL running, got %+v", etls)
}

func SpecToInitMsg(spec []byte /*yaml*/) (msg *etl.InitSpecMsg, err error) {
	errCtx := &cmn.ETLErrCtx{}
	msg = &etl.InitSpecMsg{Spec: spec}
	pod, err := etl.ParsePodSpec(errCtx, msg.Spec)
	if err != nil {
		return msg, err
	}
	errCtx.ETLName = pod.GetName()
	msg.IDX = pod.GetName()

	if err := k8s.ValidateEtlName(msg.IDX); err != nil {
		return msg, err
	}
	// Check annotations.
	msg.CommTypeX = podTransformCommType(pod)
	if msg.Timeout, err = podTransformTimeout(errCtx, pod); err != nil {
		return msg, err
	}

	return msg, msg.Validate()
}

func podTransformCommType(pod *corev1.Pod) string {
	if pod.Annotations == nil || pod.Annotations[commTypeAnnotation] == "" {
		// By default assume `Hpush`.
		return etl.Hpush
	}
	return pod.Annotations[commTypeAnnotation]
}

func podTransformTimeout(errCtx *cmn.ETLErrCtx, pod *corev1.Pod) (cos.Duration, error) {
	if pod.Annotations == nil || pod.Annotations[waitTimeoutAnnotation] == "" {
		return 0, nil
	}

	v, err := time.ParseDuration(pod.Annotations[waitTimeoutAnnotation])
	if err != nil {
		return cos.Duration(v), cmn.NewErrETL(errCtx, err.Error()).WithPodName(pod.Name)
	}
	return cos.Duration(v), nil
}
