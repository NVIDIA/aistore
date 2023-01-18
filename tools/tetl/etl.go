// Package tetl provides helpers for ETL.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tetl

import (
	"bytes"
	"fmt"
	"io"
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
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
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
		EchoGolang:    "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/go-echo/pod.yaml",
	}

	client = &http.Client{}
)

func validateETLName(name string) error {
	if _, ok := links[name]; !ok {
		return fmt.Errorf("%s invalid name; expected one of %s, %s, %s", name, Echo, Tar2TF, MD5)
	}
	return nil
}

func GetTransformYaml(name string) ([]byte, error) {
	if err := validateETLName(name); err != nil {
		return nil, err
	}

	var resp *http.Response
	// with retry in case github in unavailable for a moment
	err := cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call: func() (code int, err error) {
			resp, err = client.Get(links[name]) //nolint:bodyclose // see defer close below
			return
		},
		Action:   "get transform yaml for ETL " + name,
		SoftErr:  3,
		HardErr:  1,
		IsClient: true,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
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
		if name == Tar2tfFilters {
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

func StopAndDeleteETL(t *testing.T, baseParams api.BaseParams, etlName string) {
	if t.Failed() {
		tlog.Logln("Fetching logs from ETL containers")
		if logMsgs, err := api.ETLLogs(baseParams, etlName); err == nil {
			for _, msg := range logMsgs {
				tlog.Logf("%s\n", msg.String(10*cos.KiB))
			}
		} else {
			tlog.Logf("Error retrieving logs; err %v\n", err)
		}
	}
	tlog.Logf("Stopping ETL[%s]\n", etlName)

	if err := api.ETLStop(baseParams, etlName); err != nil {
		tlog.Logf("Stopping ETL[%s] failed; err %v\n", etlName, err)
	} else {
		tlog.Logf("ETL[%s] stopped\n", etlName)
	}
	err := api.ETLDelete(baseParams, etlName)
	tassert.CheckFatal(t, err)
}

func WaitForContainersStopped(t *testing.T, baseParams api.BaseParams) {
	tlog.Logln("Waiting for ETL containers to be stopped...")
	var (
		etls         etl.InfoList
		stopDeadline = time.Now().Add(20 * time.Second)
		interval     = 2 * time.Second
		err          error
	)

	for time.Now().Before(stopDeadline) {
		etls, err = api.ETLList(baseParams)
		tassert.CheckFatal(t, err)
		if len(etls) == 0 {
			tlog.Logln("ETL containers stopped successfully")
			return
		}
		tlog.Logf("ETLs %+v still running, waiting %s... \n", etls, interval)
		time.Sleep(interval)
	}

	tassert.Fatalf(t, len(etls) != 0, "expected all ETLs to be stopped, got %+v", etls)
}

func WaitForAborted(baseParams api.BaseParams, xactID string, timeout time.Duration) error {
	return waitForXactDone(baseParams, xactID, timeout, true)
}

func WaitForFinished(baseParams api.BaseParams, xactID string, timeout time.Duration) error {
	return waitForXactDone(baseParams, xactID, timeout, false)
}

func waitForXactDone(baseParams api.BaseParams, xactID string, timeout time.Duration, waitForAbort bool) error {
	action := "finished"
	if waitForAbort {
		action = "aborted"
	}

	tlog.Logf("Waiting for ETL xaction to be %s...\n", action)
	args := api.XactReqArgs{ID: xactID, Kind: apc.ActETLBck, Timeout: timeout /* total timeout */}
	status, err := api.WaitForXactionIC(baseParams, args)
	if err == nil {
		if waitForAbort && !status.Aborted() {
			return fmt.Errorf("expected ETL xaction to be aborted")
		}
		tlog.Logf("ETL xaction %s successfully\n", action)
		return nil
	}
	if abortErr := api.AbortXaction(baseParams, args); abortErr != nil {
		tlog.Logf("Nested error: failed to abort ETL xaction upon wait failue; err %v\n", abortErr)
	}
	return err
}

func ReportXactionStatus(baseParams api.BaseParams, xactID string, stopCh *cos.StopCh, interval time.Duration, totalObj int) {
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
				xs, err := api.QueryXactionSnaps(baseParams, api.XactReqArgs{ID: xactID})
				if err != nil {
					tlog.Logf("Failed to get x-etl[%s] stats: %v\n", xactID, err)
					continue
				}
				locObjs, outObjs, inObjs := xs.ObjCounts(xactID)
				tlog.Logf("ETL[%s] progress: (objs=%d, outObjs=%d, inObjs=%d) out of %d objects\n",
					xactID, locObjs, outObjs, inObjs, totalObj)
				locBytes, outBytes, inBytes := xs.ByteCounts(xactID)
				bps := float64(locBytes+outBytes) / time.Since(xactStart).Seconds()
				bpsStr := fmt.Sprintf("%s/s", cos.B2S(int64(bps), 2))
				tlog.Logf("ETL[%s] progress: (bytes=%d, outBytes=%d, inBytes=%d), %sBps\n",
					xactID, locBytes, outBytes, inBytes, bpsStr)
			case <-stopCh.Listen():
				return
			}
		}
	}()
}

func InitSpec(t *testing.T, baseParams api.BaseParams, name, comm string) string {
	tlog.Logf("InitSpec: %s template, %s communicator\n", name, comm)

	msg := &etl.InitSpecMsg{}
	msg.IDX = name
	msg.CommTypeX = comm
	spec, err := GetTransformYaml(name)
	tassert.CheckFatal(t, err)
	msg.Spec = spec

	retName, err := api.ETLInit(baseParams, msg)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, retName == name, "expected name %s != %s", name, retName)

	etlMsg, err := api.ETLGetInitMsg(baseParams, retName)
	tassert.CheckFatal(t, err)

	initSpec := etlMsg.(*etl.InitSpecMsg)
	tassert.Errorf(t, initSpec.Name() == name, "expected name %s != %s", name, initSpec.Name())
	tassert.Errorf(t, initSpec.CommType() == comm, "expected communicator type %s != %s", comm, initSpec.CommType())
	tassert.Errorf(t, bytes.Equal(spec, initSpec.Spec), "pod specs differ")

	return name
}

func InitCode(t *testing.T, baseParams api.BaseParams, msg etl.InitCodeMsg) string {
	retName, err := api.ETLInit(baseParams, &msg)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, retName == msg.Name(), "expected name %s != %s", msg.Name(), retName)

	etlMsg, err := api.ETLGetInitMsg(baseParams, msg.Name())
	tassert.CheckFatal(t, err)

	initCode := etlMsg.(*etl.InitCodeMsg)
	tassert.Errorf(t, initCode.Name() == retName, "expected name %s != %s", retName, initCode.Name())
	tassert.Errorf(t, msg.CommType() == "" || initCode.CommType() == msg.CommType(),
		"expected communicator type %s != %s", msg.CommType(), initCode.CommType())
	tassert.Errorf(t, msg.Runtime == initCode.Runtime, "expected runtime %s != %s", msg.Runtime, initCode.Runtime)
	tassert.Errorf(t, bytes.Equal(msg.Code, initCode.Code), "ETL codes differ")
	tassert.Errorf(t, bytes.Equal(msg.Deps, initCode.Deps), "ETL dependencies differ")

	return retName
}

func ETLBucket(t *testing.T, baseParams api.BaseParams, fromBck, toBck cmn.Bck, bckMsg *apc.TCBMsg) string {
	xactID, err := api.ETLBucket(baseParams, fromBck, toBck, bckMsg)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, baseParams.URL, toBck)
	})
	return xactID
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
