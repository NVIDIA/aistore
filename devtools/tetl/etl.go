// Package tetl provides helpers for ETL.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tetl

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/etl"
	jsoniter "github.com/json-iterator/go"
)

const (
	Tar2TF        = "tar2tf"
	Echo          = "echo"
	EchoGolang    = "echo-go"
	Md5           = "md5"
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
		Md5:           "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml",
		Tar2TF:        "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/tar2tf/pod.yaml",
		Tar2tfFilters: "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/tar2tf/pod.yaml",
		Echo:          "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/echo/pod.yaml",
		EchoGolang:    "https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/go-echo/pod.yaml",
	}

	client = &http.Client{}
)

func validateETLName(name string) error {
	if _, ok := links[name]; !ok {
		return fmt.Errorf("%s invalid name; expected one of %s, %s, %s", name, Echo, Tar2TF, Md5)
	}
	return nil
}

func GetTransformYaml(name string) ([]byte, error) {
	if err := validateETLName(name); err != nil {
		return nil, err
	}

	var resp *http.Response
	// Retry in case github in unavailable to fulfill the request for a moment.
	err := cmn.NetworkCallWithRetry(&cmn.CallWithRetryArgs{
		Call: func() (code int, err error) {
			resp, err = client.Get(links[name]) // nolint:bodyclose // closed after returning from NetworkCallWithRetry().
			return
		},
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
			return etl.RedirectCommType
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

func StopETL(t *testing.T, baseParams api.BaseParams, etlID string) {
	if t.Failed() {
		tlog.Logln("Fetching logs from ETL containers")
		if logMsgs, err := api.ETLLogs(baseParams, etlID); err == nil {
			for _, msg := range logMsgs {
				tlog.Logf("%s\n", msg.String(10*cos.KiB))
			}
		} else {
			tlog.Logf("Error retrieving logs; err %v\n", err)
		}
	}
	tlog.Logf("Stopping ETL %q\n", etlID)

	if err := api.ETLStop(baseParams, etlID); err != nil {
		tlog.Logf("Stopping ETL %q failed; err %v\n", etlID, err)
	} else {
		tlog.Logf("ETL %q stopped\n", etlID)
	}
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

	refreshInterval := time.Duration(timeout.Nanoseconds() / 20)

	tlog.Logf("Waiting for ETL xaction to be %s...\n", action)
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActETLBck, Timeout: timeout /* total timeout */}
	status, err := api.WaitForXaction(baseParams, args, refreshInterval)
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
				stats, err := api.GetXactionStatsByID(baseParams, xactID)
				if err != nil {
					tlog.Logf("Failed to get xaction stats; err %v\n", err)
					continue
				}
				bps := float64(stats.BytesCount()) / time.Since(xactStart).Seconds()
				bpsStr := fmt.Sprintf("%s/s", cos.B2S(int64(bps), 2))
				tlog.Logf("ETL %q already transformed %d/%d objects (%s) (%s)\n", xactID, stats.ObjCount(), totalObj, cos.B2S(stats.BytesCount(), 2), bpsStr)
			case <-stopCh.Listen():
				return
			}
		}
	}()
}

func Init(baseParams api.BaseParams, name, comm string) (string, error) {
	tlog.Logln("Reading template")

	spec, err := GetTransformYaml(name)
	if err != nil {
		return "", err
	}

	pod, err := etl.ParsePodSpec(nil, spec)
	if err != nil {
		return "", err
	}
	if comm != "" {
		pod.Annotations["communication_type"] = comm
	}

	spec, _ = jsoniter.Marshal(pod)
	tlog.Logln("Init ETL")
	return api.ETLInitSpec(baseParams, spec)
}

func ETLBucket(t *testing.T, baseParams api.BaseParams, fromBck, toBck cmn.Bck, bckMsg *cmn.TransCpyBckMsg) string {
	xactID, err := api.ETLBucket(baseParams, fromBck, toBck, bckMsg)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tutils.DestroyBucket(t, baseParams.URL, toBck)
	})
	return xactID
}

func CheckNoRunningETLContainers(t *testing.T, params api.BaseParams) {
	etls, err := api.ETLList(params)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(etls) == 0, "Expected no ETL running, got %+v", etls)
}
