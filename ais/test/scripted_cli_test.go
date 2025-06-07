// Package integration_test.
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestGetWarmValidateS3UsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		CloudBck: true,
		Bck:      cliBck,
	})
	// note additional limitation
	normp, _ := cmn.NormalizeProvider(cliBck.Provider)
	if normp != apc.AWS {
		t.Skipf("skipping %s - the test uses s3cmd (command line tool) and requires s3 bucket (see \"prerequisites\")", t.Name())
	}

	var (
		bucketName = cliBck.Cname("")
		cmd        = exec.Command("./scripts/s3-get-validate.sh", "--bucket", bucketName)
	)
	tlog.Logf("Running '%s %s'\n", cmd.Path, strings.Join(cmd.Args, " "))
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestGetWarmValidateRemaisUsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})

	bck := cliBck
	if bck.IsRemoteAIS() {
		tlog.Logf("using existing %s ...\n", bck.Cname(""))
	} else {
		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
			Ns:       cmn.Ns{UUID: tools.RemoteCluster.Alias},
		}
		tlog.Logf("using temp bucket %s ...\n", bck.Cname(""))
	}

	var (
		bucketName = bck.Cname("")
		cmd        = exec.Command("./scripts/remais-get-validate.sh", "--bucket", bucketName)
	)
	tlog.Logf("Running '%s'\n", cmd.String())
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestPrefetchLatestS3UsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		CloudBck: true,
		Bck:      cliBck,
	})
	// note additional limitation
	normp, _ := cmn.NormalizeProvider(cliBck.Provider)
	if normp != apc.AWS {
		t.Skipf("skipping %s - the test uses s3cmd (command line tool) and requires s3 bucket (see \"prerequisites\")", t.Name())
	}

	var (
		bucketName = cliBck.Cname("")
		cmd        = exec.Command("./scripts/s3-prefetch-latest-prefix.sh", "--bucket", bucketName)
	)
	tlog.Logf("Running '%s'\n", cmd.String())
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestPrefetchLatestRemaisUsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})

	bck := cliBck
	if bck.IsRemoteAIS() {
		tlog.Logf("using existing %s ...\n", bck.Cname(""))
	} else {
		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
			Ns:       cmn.Ns{UUID: tools.RemoteCluster.Alias},
		}
		tlog.Logf("using temp bucket %s ...\n", bck.Cname(""))
	}

	var (
		bucketName = bck.Cname("")
		cmd        = exec.Command("./scripts/remais-prefetch-latest.sh", "--bucket", bucketName)
	)
	tlog.Logf("Running '%s'\n", cmd.String())
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

func TestCopySyncRemaisUsingScript(t *testing.T) {
	bck := cliBck
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Bck:                   bck,
		Long:                  true,
		RequiresRemoteCluster: true,
		CloudBck:              true,
	})

	var (
		bucketName = bck.Cname("")
		cmd        = exec.Command("./scripts/cp-sync-remais-out-of-band.sh", "--bucket", bucketName)
	)
	tlog.Logf("Running '%s'\n", cmd.String())
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

// NOTE: not running with an actual remote s3 bucket (could take hours)
// instead, using aisore S3 API with a temp `ais://` bucket, and with two additional workarounds:
// 1. MD5
// 2. "apc.S3Scheme+apc.BckProviderSeparator+bck.Name" (below)
func TestMultipartUploadUsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long: true,
	})

	tempdir := t.TempDir()
	bck := cmn.Bck{Name: trand.String(10), Provider: apc.AIS}

	// 1. set MD5 to satisfy `s3cmd` (for details, see docs/s3compat.md)
	bprops := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumMD5)},
	}
	tools.CreateBucket(t, proxyURL, bck, bprops, true /*cleanup*/)

	// 2. subst "ais://" with "s3://" to circumvent s3cmd failing with "not a recognized URI"
	cmd := exec.Command("./scripts/s3-mpt-large-files.sh", tempdir, apc.S3Scheme+apc.BckProviderSeparator+bck.Name,
		"1",    // number of iterations
		"true", // generate large files
		"1",    // number of large files
	)

	tlog.Logf("Running '%s' (this may take a while...)\n", cmd.String())
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

// remais-blob-download.sh
func TestRemaisBlobDownloadUsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		RequiresRemoteCluster: true,
		Long:                  true,
	})
	bck := cmn.Bck{
		Name:     trand.String(10),
		Ns:       cmn.Ns{UUID: tools.RemoteCluster.Alias},
		Provider: apc.AIS,
	}
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	name := bck.Cname("")

	// use remais-blob-download.sh defaults for everything except bucket name (e.g.):
	// "--minsize", "1MB",
	// "--maxsize", "10MB",
	// "--totalsize", "100MB",
	// "--chunksize", "500K",
	// "--numworkers", "5"
	cmd := exec.Command("./scripts/remais-blob-download.sh", "--bucket", name)

	tlog.Logf("Running '%s' (this may take a while...)\n", cmd.String())
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

// get-archregx-wdskey.sh
func TestGetArchregxWdskeyUsingScript(t *testing.T) {
	cmd := exec.Command("./scripts/get-archregx-wdskey.sh")

	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

// runs 2 scripts to generate test subtree and then delete random bunch
// see related unit: fs/lpi_test
func TestRemaisDeleteUsingScript(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		RequiresRemoteCluster: true,
	})

	const (
		verbose      = false // NOTE: enable to trace
		lpiGenScript = "./scripts/gen-nested-dirs.sh"
		lpiGenPrefix = "Total files created: "

		remaisScript = "./scripts/remais-del-list-deleted.sh"
	)
	// 1. create temp root
	root := t.TempDir()

	//
	// 2. script #1: generate
	//
	cmd := exec.Command(lpiGenScript, "--root_dir", root)
	out, err := cmd.CombinedOutput()
	tassert.CheckFatal(t, err)

	var (
		lines = strings.Split(string(out), "\n")
		num   int64
	)
	for _, ln := range lines {
		i := strings.Index(ln, lpiGenPrefix)
		if i >= 0 {
			s := ln[i+len(lpiGenPrefix):]
			num, err = strconv.ParseInt(s, 10, 64)
			tassert.CheckFatal(t, err)
		}
	}
	tassert.Fatalf(t, num > 0, "failed to parse script output (num %d)", num)

	//
	// 3. script #2: put generated subtree => remais and perform random out-of-band delete
	//
	bck := cmn.Bck{
		Name:     trand.String(10),
		Ns:       cmn.Ns{UUID: tools.RemoteCluster.Alias},
		Provider: apc.AIS,
	}
	tools.CreateBucket(t, proxyURL, bck, nil, false /*cleanup*/)
	name := bck.Cname("")
	tlog.Logln("bucket " + name)

	defer func() {
		if !t.Failed() || !verbose {
			api.DestroyBucket(baseParams, bck)
		}
	}()

	cmd = exec.Command(remaisScript, "--root_dir", root, "--bucket", name)
	out, err = cmd.CombinedOutput()
	tassert.CheckFatal(t, err)

	lines = strings.Split(string(out), "\n")
	var (
		scnt   int
		sorted = make(sort.StringSlice, 0, 100)
		prefix = "deleted: " + apc.AIS + apc.BckProviderSeparator + bck.Name + "/" // TODO: nit
	)
	for _, ln := range lines {
		if strings.HasPrefix(ln, prefix) {
			scnt++
			sorted = append(sorted, ln[len(prefix):])
		}
	}
	tlog.Logf("## objects deleted out-of-band:\t%d\n", scnt)

	defer func() {
		if t.Failed() && verbose {
			fmt.Println("\t ============")
			sort.Sort(sorted)
			for _, ln := range sorted {
				fmt.Println("\t", ln)
			}
			fmt.Println("\t ============")
		}
	}()

	// 4. ls --check-versions
	lsmsg := &apc.LsoMsg{}
	lsmsg.AddProps([]string{apc.GetPropsName, apc.GetPropsSize, apc.GetPropsVersion, apc.GetPropsCopies,
		apc.GetPropsLocation, apc.GetPropsCustom}...)
	lsmsg.SetFlag(apc.LsDiff)
	lst, err := api.ListObjects(baseParams, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	var lcnt int
	for _, en := range lst.Entries {
		if en.IsAnyFlagSet(apc.EntryVerRemoved) {
			// fmt.Println("\tdeleted:", en.Name)
			lcnt++
		}
	}
	if scnt != lcnt {
		err := fmt.Errorf("deleted out-of-band (%d) != (%d) list-objects version-removed", scnt, lcnt)
		tassert.CheckFatal(t, err)
	}
	tlog.Logf("## list-objects version-removed:\t%d\n", lcnt)
}

// rate-limit-frontend-test.sh
func TestRateLimitFrontendUsingScript(t *testing.T) {
	cmd := exec.Command("./scripts/rate-limit-frontend-test.sh")

	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}

// rate-limit-backend-test.sh
func TestRateLimitBackendUsingScript(t *testing.T) {
	cmd := exec.Command("./scripts/rate-limit-backend-test.sh")

	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		tlog.Logln(string(out))
	}
	tassert.CheckFatal(t, err)
}
