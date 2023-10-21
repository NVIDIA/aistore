// Package integration_test.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

const (
	// Public object name to download from Google Cloud Storage.
	gcsBck      = "gcp-public-data-landsat"
	gcsFilename = "LT08_L1GT_040021_20130506_20170310_01_T2_B10.TIF"
	gcsObjXML   = "LT08/01/040/021/LT08_L1GT_040021_20130506_20170310_01_T2/" + gcsFilename
)

// generate URL to request object from GCS
func genObjURL(isSecure, isXML bool) (s string) {
	if isSecure || !isXML { // Using JSON requires HTTPS: "SSL is required to perform this operation."
		s = "https://"
	} else {
		s = "http://"
	}
	if isXML {
		s += fmt.Sprintf("storage.googleapis.com/%s/%s", gcsBck, gcsObjXML)
	} else {
		// Reformat object name from XML to JSON API requirements.
		gcsObjJSON := strings.ReplaceAll(gcsObjXML, "/", "%2F")
		s += fmt.Sprintf("www.googleapis.com/storage/v1/b/%s/o/%s?alt=media", gcsBck, gcsObjJSON)
	}
	return s
}

// build command line for CURL
func genCURLCmdLine(t *testing.T, resURL, proxyURL string, targets meta.NodeMap) []string {
	var noProxy []string
	for _, t := range targets {
		if !cos.StringInSlice(t.PubNet.Hostname, noProxy) {
			noProxy = append(noProxy, t.PubNet.Hostname)
		}
	}

	// TODO:  "--proxy-insecure" requires `curl` 7.58.0+ and is needed when we USE_HTTPS (see #885)
	return []string{
		"-L", "-X", "GET",
		resURL,
		"-o", filepath.Join(t.TempDir(), "curl.file"),
		"-x", proxyURL,
		"--max-redirs", "3",
		"--noproxy", strings.Join(noProxy, ","),
		"--insecure",
	}
}

// Extract download speed from CURL output.
func extractSpeed(out []byte) int64 {
	lines := strings.Split(string(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if lines[i] == "" {
			continue
		}
		words := strings.Split(lines[i], " ")
		if spd, err := cos.ParseSize(words[len(words)-1], cos.UnitsIEC); err == nil {
			return spd
		}
	}
	return 0
}

func TestRProxyGCS(t *testing.T) {
	var (
		resURL     = genObjURL(false, true)
		proxyURL   = tools.GetPrimaryURL()
		smap       = tools.GetClusterMap(t, proxyURL)
		baseParams = tools.BaseAPIParams(proxyURL)

		maxRetries = 2
	)

	if cos.IsHTTPS(proxyURL) {
		t.Skip("test doesn't work for HTTPS")
	}

	initMountpaths(t, proxyURL)
	bck := cmn.Bck{Provider: apc.HTTP}
	queryBck := cmn.QueryBcks(bck)
	bckList, err := api.ListBuckets(baseParams, queryBck, apc.FltExists)
	tassert.CheckFatal(t, err)

retry:
	cmdline := genCURLCmdLine(t, resURL, proxyURL, smap.Tmap)
	tlog.Logf("First time download via XML API: %s\n", cmdline)
	out, err := exec.Command("curl", cmdline...).CombinedOutput()
	tlog.Logln("\n" + string(out))
	tassert.CheckFatal(t, err)

	speedCold := extractSpeed(out)
	tlog.Logf("Cold download speed:   %s\n", cos.ToSizeIEC(speedCold, 1))
	tassert.Fatalf(t, speedCold != 0, "Failed to detect cold download speed")

	// at less than 100KBps we likely failed to download
	if speedCold < 100*cos.KiB {
		if testing.Short() {
			fmt := "cold download speed %s is way too low indicating potential timeout"
			tools.ShortSkipf(t, fmt, cos.ToSizeIEC(speedCold, 1))
		}
		if maxRetries > 0 {
			tlog.Logf("Warning: will retry (%d)\n", maxRetries)
			time.Sleep(15 * time.Second)
			tlog.Logln("Warning: retrying...")
			maxRetries--
			goto retry
		}
	}

	bckListNew, err := api.ListBuckets(baseParams, queryBck, apc.FltExists)
	tassert.CheckFatal(t, err)
	bck, err = detectNewBucket(bckList, bckListNew)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, bck)
	})

	pathCached := findObjOnDisk(bck, gcsFilename)
	tassert.Fatalf(t, pathCached != "", "object was not downloaded")
	tlog.Logf("Downloaded as %q\n", pathCached)

	tlog.Logf("HTTP download\n")
	cmdline = genCURLCmdLine(t, resURL, proxyURL, smap.Tmap)
	out, err = exec.Command("curl", cmdline...).CombinedOutput()
	tlog.Logln(string(out))
	tassert.CheckFatal(t, err)
	speedHTTP := extractSpeed(out)
	tassert.Fatalf(t, speedHTTP != 0, "Failed to detect speed for HTTP download")

	/*
		TODO: uncomment when target supports HTTPS client

		tlog.Logf("HTTPS download\n")
		cmdline = genCURLCmdLine(true, true, proxyURL, smap.Tmap)
		out, err = exec.Command("curl", cmdline...).CombinedOutput()
		tlog.Logln(string(out))
		tassert.CheckFatal(t, err)
		speedHTTPS := extractSpeed(out)
		tassert.Fatalf(t, speedHTTPS != 0, "Failed to detect speed for HTTPS download")

		bckListNew, err = api.ListBuckets(baseParams, queryBck)
		tassert.CheckFatal(t, err)
		bckHTTPS, err := detectNewBucket(bckList, bckListNew)
		tassert.CheckFatal(t, err)
		defer tools.DestroyBucket(t, proxyURL, bckHTTPS)

		tlog.Logf("Check via JSON API\n")
		cmdline = genCURLCmdLine(false, false, proxyURL, smap.Tmap)
		tlog.Logf("JSON: %s\n", cmdline)
		out, err = exec.Command("curl", cmdline...).CombinedOutput()
		t.Log(string(out))
		tassert.CheckFatal(t, err)
		speedJSON := extractSpeed(out)
		tassert.Fatalf(t, speedJSON != 0, "Failed to detect speed for JSON download")
	*/

	tlog.Logf("Cold download speed:   %s\n", cos.ToSizeIEC(speedCold, 1))
	tlog.Logf("HTTP download speed:   %s\n", cos.ToSizeIEC(speedHTTP, 1))
	/*
		TODO: uncomment when target supports HTTPS client

		tlog.Logf("HTTPS download speed:  %s\n", cos.ToSizeIEC(speedHTTPS, 1))
		tlog.Logf("JSON download speed:   %s\n", cos.ToSizeIEC(speedJSON, 1))
	*/
	ratio := float64(speedHTTP) / float64(speedCold)
	if ratio < 0.8 {
		tlog.Logf("Cached download is %.1f slower than Cold\n", ratio)
	} else if ratio > 1.2 {
		tlog.Logf("HTTP is %.1f faster than Cold\n", ratio)
	}
}

func TestRProxyInvalidURL(t *testing.T) {
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		client     = tools.NewClientWithProxy(proxyURL)
	)
	tests := []struct {
		url        string
		statusCode int
		doAndCheck bool
	}{
		// case 1
		{url: "http://storage.googleapis.com/kubernetes-release/release", statusCode: http.StatusNotFound, doAndCheck: true},
		{url: "http://invalid.invaliddomain.com/test/webpage.txt", statusCode: http.StatusBadRequest, doAndCheck: true}, // Invalid domain
		// case 2
		{url: "http://archive.ics.uci.edu/ml/datasets/Abalone", doAndCheck: false},
	}
	for _, test := range tests {
		hbo, err := cmn.NewHTTPObjPath(test.url)
		tassert.CheckError(t, err)
		api.DestroyBucket(baseParams, hbo.Bck)

		req, err := http.NewRequest(http.MethodGet, test.url, http.NoBody)
		tassert.CheckFatal(t, err)
		api.SetAuxHeaders(req, &baseParams)

		if test.doAndCheck {
			// case 1: bad response on GET followed by a failure to HEAD
			tassert.DoAndCheckResp(t, client, req, test.statusCode, http.StatusForbidden)
			_, err = api.HeadBucket(baseParams, hbo.Bck, false /* don't add */)
			tassert.Errorf(t, err != nil, "shouldn't create bucket (%s) for invalid resource URL %q", hbo.Bck, test.url)
		} else {
			// case 2: cannot GET but can still do a HEAD (even though ETag is not provided)
			resp, err := client.Do(req)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			tassert.Errorf(t, err != nil, "expecting error executing GET %q", test.url)
			_, err = api.HeadBucket(baseParams, hbo.Bck, false /* don't add */)
			tassert.CheckError(t, err)
		}

		api.DestroyBucket(baseParams, hbo.Bck)
	}
}
