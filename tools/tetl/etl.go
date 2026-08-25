// Package tetl provides helpers for ETL.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tetl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
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
	"k8s.io/apimachinery/pkg/util/yaml"
)

const (
	PodWithResourcesConstraint = "resources-constraint"

	Tar2TF        = "tar2tf"
	Echo          = "transformer-echo"
	EchoGolang    = "echo-go"
	MD5           = "transformer-md5"
	HashWithArgs  = "hash-with-args"
	Tar2tfFilters = "tar2tf-filters"
	ParquetParser = "parquet-parser"
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

const (
	podWithResourcesConstraintSpec = `
name: md5-transformer-etl
runtime:
  image: aistorage/transformer_md5:latest
resources:
  requests:
    memory: "%s"
    cpu: "%s"
  limits:
    memory: "%s"
    cpu: "%s"
`
	tar2TFSpec = `
name: tar2tf
runtime:
  image: aistorage/transformer_tar2tf:latest
  command: ["./tar2tf", "-l", "0.0.0.0", "-p", "8000"]
`
	tar2TFFiltersSpec = `
name: tar2tf-filters
runtime:
  image: aistorage/transformer_tar2tf:latest
  command: ["./tar2tf", "-l", "0.0.0.0", "-p", "8000", "--spec", '%s']
`
)

var (
	links = map[string]string{
		MD5:           "https://raw.githubusercontent.com/NVIDIA/ais-etl/main/transformers/md5/etl_spec.yaml",
		HashWithArgs:  "https://raw.githubusercontent.com/NVIDIA/ais-etl/main/transformers/hash_with_args/etl_spec.yaml",
		Echo:          "https://raw.githubusercontent.com/NVIDIA/ais-etl/main/transformers/echo/etl_spec.yaml",
		EchoGolang:    "https://raw.githubusercontent.com/NVIDIA/ais-etl/main/transformers/go_echo/etl_spec.yaml",
		ParquetParser: "https://raw.githubusercontent.com/NVIDIA/ais-etl/main/transformers/parquet-parser/etl_spec.yaml",
	}

	testSpecs = map[string]string{
		PodWithResourcesConstraint: podWithResourcesConstraintSpec,
		Tar2TF:                     tar2TFSpec,
		Tar2tfFilters:              fmt.Sprintf(tar2TFFiltersSpec, strings.Join(strings.Fields(tar2tfFilter), " ")),
	}

	client = &http.Client{}
)

var (
	EchoTransform  = func(r io.Reader) io.Reader { return r }
	NumpyTransform = func(_ io.Reader) io.Reader { return bytes.NewReader([]byte("\x00\x00\x01\x00\x02\x00\x03\x00")) }
	MD5Transform   = func(r io.Reader) io.Reader {
		data, _ := io.ReadAll(r)
		return bytes.NewReader([]byte(cos.ChecksumB2S(data, cos.ChecksumMD5)))
	}
)

func validateETLName(name string) error {
	if _, ok := links[name]; !ok {
		return fmt.Errorf("%s is invalid etlName, expected predefined (%s, %s, %s, %s)", name, Echo, Tar2TF, MD5, ParquetParser)
	}
	return nil
}

func GetTransformYaml(etlName string, replaceArgs ...string) ([]byte, error) {
	if spec, ok := testSpecs[etlName]; ok {
		if len(replaceArgs) > 0 {
			args := make([]any, len(replaceArgs))
			for i, v := range replaceArgs {
				args[i] = v
			}
			spec = fmt.Sprintf(spec, args...)
		}
		return []byte(spec), nil
	}
	if err := validateETLName(etlName); err != nil {
		return nil, err
	}

	var (
		resp   *http.Response
		action = "get transform yaml for ETL[" + etlName + "]"
		args   = &cmn.RetryArgs{
			Call: func() (_ int, err error) {
				req, e := http.NewRequestWithContext(context.Background(), http.MethodGet, links[etlName], http.NoBody)
				if e != nil {
					return 0, err
				}
				resp, err = client.Do(req) //nolint:bodyclose // see defer close below
				if resp != nil {
					return resp.StatusCode, err
				}
				return 0, err
			},
			Action:   action,
			SoftErr:  3,
			HardErr:  1,
			IsClient: true,
		}
	)
	// with retry in case github in unavailable for a moment
	_, err := args.Do()
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

	return b, nil
}

func StopAndDeleteETL(t *testing.T, bp api.BaseParams, etlName string) {
	if t.Failed() {
		tlog.Logln("Fetching logs from ETL containers")
		if logsByTarget, err := api.ETLLogs(bp, etlName); err == nil {
			for _, etlLogs := range logsByTarget {
				tlog.Logln(headETLLogs(etlLogs, 10*cos.KiB))
			}
		} else {
			tlog.Logfln("Error retrieving ETL[%s] logs: %v", etlName, err)
		}
	}
	tlog.Logfln("Stopping ETL[%s]", etlName)

	if err := api.ETLStop(bp, etlName); err != nil {
		tlog.Logfln("Stopping ETL[%s] failed; err %v", etlName, err)
	} else {
		tlog.Logfln("ETL[%s] stopped", etlName)
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

func WaitForETLAborted(t *testing.T, bp api.BaseParams, etlNames ...string) {
	tlog.Logln("Waiting for all ETLs to abort...")
	var (
		etls         etl.InfoList
		stopDeadline = time.Now().Add(20 * time.Second)
		watchlist    = cos.NewStrSet(etlNames...)
		interval     = 2 * time.Second
		err          error
	)

	for {
		etls, err = api.ETLList(bp)
		tassert.CheckFatal(t, err)

		allAborted := true
		for _, info := range etls {
			if watchlist.Contains(info.Name) && info.Stage != etl.Aborted.String() {
				allAborted = false
				break
			}
		}

		if allAborted {
			tlog.Logln("All ETL containers aborted successfully")
			return
		}

		if time.Now().After(stopDeadline) {
			break
		}

		tlog.Logfln("ETLs %+v not fully aborted, waiting %s...", etls, interval)
		time.Sleep(interval)
	}

	err = fmt.Errorf("expected all ETLs to stop, got %+v still running", etls)
	tassert.CheckFatal(t, err)
}

func WaitForAborted(bp api.BaseParams, xid, kind string, timeout time.Duration) error {
	tlog.Logfln("Waiting for ETL x-%s[%s] to abort...", kind, xid)
	args := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: timeout /* total timeout */}
	status, err := api.WaitForXactionIC(bp, &args)
	if err == nil {
		if !status.IsAborted() {
			err = fmt.Errorf("expected ETL x-%s[%s] status to indicate 'abort', got: %+v", kind, xid, status)
		}
		return err
	}
	tlog.Logfln("Aborting ETL x-%s[%s]", kind, xid)
	if abortErr := api.AbortXaction(bp, &args); abortErr != nil {
		tlog.Logfln("Nested error: failed to abort upon api.wait failure: %v", abortErr)
	}
	return err
}

// NOTE: relies on x-kind to choose the waiting method
// TODO -- FIXME: remove and simplify - here and everywhere
func WaitForFinished(bp api.BaseParams, xid, kind string, timeout time.Duration) (err error) {
	tlog.Logfln("Waiting for ETL x-%s[%s] to finish...", kind, xid)
	args := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: timeout /* total timeout */}
	if xact.IdlesBeforeFinishing(kind) {
		err = api.WaitForSnapsIdle(bp, &args)
	} else {
		_, err = api.WaitForXactionIC(bp, &args)
	}
	if err == nil {
		return
	}
	tlog.Logfln("error waiting for xaction to finish: %v", err)
	tlog.Logfln("Aborting ETL x-%s[%s]", kind, xid)
	if abortErr := api.AbortXaction(bp, &args); abortErr != nil {
		tlog.Logfln("Nested error: failed to abort upon api.wait failure: %v", abortErr)
	}
	return nil
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
					tlog.Logfln("Failed to get x-etl[%s] stats: %v", xid, err)
					continue
				}
				locObjs, outObjs, inObjs := xs.ObjCounts(xid)
				tlog.Logfln("ETL[%s] progress: (objs=%d, outObjs=%d, inObjs=%d) out of %d objects",
					xid, locObjs, outObjs, inObjs, totalObj)
				locBytes, outBytes, inBytes := xs.ByteCounts(xid)
				bps := float64(locBytes+outBytes) / time.Since(xactStart).Seconds()
				bpsStr := cos.IEC(int64(bps), 2) + "/s"
				tlog.Logfln("ETL[%s] progress: (bytes=%d, outBytes=%d, inBytes=%d), %sBps",
					xid, locBytes, outBytes, inBytes, bpsStr)
			case <-stopCh.Listen():
				return
			}
		}
	}()
}

func InitSpec(t *testing.T, bp api.BaseParams, etlName, commType string, replaceArgs ...string) (msg etl.InitMsg) {
	tlog.Logf("InitSpec ETL[%s], communicator %s\n", etlName, commType)
	spec, err := GetTransformYaml(etlName, replaceArgs...)
	tassert.CheckFatal(t, err)

	etlSpec := &etl.ETLSpecMsg{}
	etlName += strings.ReplaceAll(strings.ToLower(cos.GenUUID()), "_", "-") // add random suffix to avoid conflicts
	tassert.CheckFatal(t, yaml.Unmarshal(spec, etlSpec))
	etlSpec.EtlName = etlName
	etlSpec.CommTypeX = commType
	etlSpec.InitTimeout = cos.Duration(time.Minute * 2) // manually increase timeout in testing environment
	tassert.CheckFatal(t, etlSpec.Validate())
	msg = etlSpec

	tassert.Fatalf(t, msg.Name() == etlName, "%q vs %q", msg.Name(), etlName) // assert

	xid, err := api.ETLInit(bp, msg)
	if herr, ok := err.(*cmn.ErrHTTP); ok && herr.TypeCode == "ErrUnsupp" && msg.CommType() == etl.WebSocket {
		t.Skip("skipping, WebSocket only work with direct put supported transformers")
	}
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, cos.IsValidUUID(xid), "expected valid xaction ID, got %q", xid)
	// reread `InitMsg` and compare with the specified
	details, err := api.ETLGetDetail(bp, etlName, "")
	tassert.CheckFatal(t, err)

	tlog.Logfln("ETL %q: running x-etl-spec[%s]", etlName, xid)

	tassert.Errorf(t, details.InitMsg.Name() == etlName, "expected etlName %s, got %s", etlName, details.InitMsg.Name())
	tassert.Errorf(t, details.InitMsg.CommType() == commType, "expected communicator type %s, got %s", commType, details.InitMsg.CommType())

	return msg
}

func InspectPod(t *testing.T, podName string) corev1.Pod {
	client, err := k8s.InitTestClient(tools.DefaultNamespace)
	tassert.CheckFatal(t, err)
	pod, err := client.Pod(podName)
	tassert.CheckFatal(t, err)
	return *pod
}

func ETLBucketWithCleanup(t *testing.T, bp api.BaseParams, bckFrom, bckTo cmn.Bck, msg *apc.TCBMsg) string {
	xid, err := api.ETLBucket(bp, bckFrom, bckTo, msg)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		tools.DestroyBucket(t, bp.URL, bckTo)
	})

	tlog.Logfln("ETL[%s]: running %s => %s xaction %q",
		msg.Transform.Name, bckFrom.Cname(""), bckTo.Cname(""), xid)
	return xid
}

func ETLBucketWithCmp(t *testing.T, bp api.BaseParams, bckFrom, bckTo cmn.Bck, msg *apc.TCBMsg, cmp func(r1, r2 io.Reader) bool) {
	xid := ETLBucketWithCleanup(t, bp, bckFrom, bckTo, msg)
	err := WaitForFinished(bp, xid, apc.ActETLBck, 3*time.Minute)
	tassert.CheckFatal(t, err)

	tlog.Logfln("ETL[%s]: comparing buckets, %s vs %s", msg.Transform.Name, bckFrom.Cname(""), bckTo.Cname(""))

	objeList, err := api.ListObjects(bp, bckFrom, &apc.LsoMsg{}, api.ListArgs{})
	tassert.CheckFatal(t, err)
	for _, en := range objeList.Entries {
		r1, _, err := api.GetObjectReader(bp, bckFrom, en.Name, &api.GetArgs{})
		tassert.CheckFatal(t, err)
		r2, _, err := api.GetObjectReader(bp, bckTo, en.Name, &api.GetArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, cmp(r1, r2), "object content mismatch: %s vs %s", bckFrom.Cname(en.Name), bckTo.Cname(en.Name))
		tassert.CheckFatal(t, r1.Close())
		tassert.CheckFatal(t, r2.Close())
	}
}

func ETLCheckStage(t *testing.T, params api.BaseParams, etlName string, stage etl.Stage) {
	etls, err := api.ETLList(params)
	tassert.CheckFatal(t, err)
	for _, inst := range etls {
		if etlName == inst.Name && inst.Stage == stage.String() {
			return
		}
	}
	t.Fatalf("etl[%s] doesn't exist or isn't in status %s (%v)", etlName, stage.String(), etls)
}

func CheckNoRunningETLContainers(t *testing.T, params api.BaseParams) {
	etls, err := api.ETLList(params)
	tassert.CheckFatal(t, err)
	for _, info := range etls {
		tassert.Fatalf(t, info.Stage == etl.Aborted.String(), "expected no running ETL containers, got %s in stage %s", info.Name, info.Stage)
	}
}

// SpecToInitMsg converts an ETL runtime specification into an init message.
func SpecToInitMsg(spec []byte /*yaml*/) (*etl.ETLSpecMsg, error) {
	msg := &etl.ETLSpecMsg{}
	if err := yaml.Unmarshal(spec, msg); err != nil {
		return msg, err
	}
	return msg, msg.Validate()
}

func ListObjectsWithRetry(bp api.BaseParams, bckTo cmn.Bck, prefix string, expectedCount int, opts tools.WaitRetryOpts) (err error) {
	var (
		retries       = opts.MaxRetries
		retryInterval = opts.Interval
		i             int
	)
retry:
	list, err := api.ListObjects(bp, bckTo, &apc.LsoMsg{Prefix: prefix}, api.ListArgs{})
	if err == nil && len(list.Entries) == expectedCount {
		return nil
	}
	if !cmn.IsStatusServiceUnavailable(err) && !cos.IsErrRetriableConn(err) {
		return
	}
	time.Sleep(retryInterval)
	i++
	if i > retries {
		return fmt.Errorf("api.ListObjects max retries (%d) exceeded, expected %d objects, got %d", retries, expectedCount, len(list.Entries))
	}
	goto retry
}
