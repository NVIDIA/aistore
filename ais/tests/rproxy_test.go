// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

const (
	// Public object name to download from Google Cloud Storage(GCS)
	gcsFilename = "LT80400212013126LGN01_B10.TIF"
	gcsObjXML   = "LT08/PRE/040/021/LT80400212013126LGN01/" + gcsFilename
	// Public GCS bucket
	gcsBck = "gcp-public-data-landsat"
	// Without this query GCS returns only object's information
	gcsQuery    = "?alt=media"
	gcsHostJSON = "www.googleapis.com"
	gcsHostXML  = "storage.googleapis.com"
	gcsTmpFile  = "/tmp/rproxy_test_download.tiff"
)

// reformat object name from XML to JSON API requirements
var gcsObjJSON = strings.ReplaceAll(gcsObjXML, "/", "%2F")

// search for the full path of cached object
// TODO: replace with initMountpaths and fs.WalkBck
func pathForCached() string {
	var fpath string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if fpath != "" {
			return filepath.SkipDir
		}
		if err != nil || info == nil {
			return nil
		}

		// TODO -- FIXME - avoid hardcoded on-disk layout
		if info.IsDir() || !strings.Contains(path, cmn.ProviderHTTP+"/") {
			return nil
		}

		if strings.HasSuffix(path, gcsFilename) {
			fpath = path
			return filepath.SkipDir
		}
		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)
	return fpath
}

// generate URL to request object from GCS
func genObjURL(isSecure, isXML bool) (s string) {
	if isSecure || !isXML { // Using JSON requires HTTPS: "SSL is required to perform this operation."
		s = "https://"
	} else {
		s = "http://"
	}
	if isXML {
		s += fmt.Sprintf("%s/%s/%s", gcsHostXML, gcsBck, gcsObjXML)
	} else {
		s += fmt.Sprintf("%s/storage/v1/b/%s/o/%s%s", gcsHostJSON, gcsBck, gcsObjJSON, gcsQuery)
	}
	return s
}

// build command line for CURL
func genCURLCmdLine(resURL, proxyURL string, targets cluster.NodeMap) []string {
	var noProxy []string
	for _, t := range targets {
		if !cmn.StringInSlice(t.PublicNet.NodeHostname, noProxy) {
			noProxy = append(noProxy, t.PublicNet.NodeHostname)
		}
	}
	// TODO:  "--proxy-insecure" requires `curl` 7.58.0+ and is needed when we USE_HTTPS (see #885)
	return []string{
		"-L", "-X", "GET",
		resURL,
		"-o", gcsTmpFile,
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
		if spd, err := cmn.S2B(words[len(words)-1]); err == nil {
			return spd
		}
	}
	return 0
}

func TestRProxyGCS(t *testing.T) {
	var (
		resURL   = genObjURL(false, true)
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
	)

	if cmn.IsHTTPS(proxyURL) {
		t.Skip("test doesn't work for HTTPS")
	}

	// look for leftovers and cleanup if found
	pathCached := pathForCached()
	if pathCached != "" {
		tutils.Logf("Found in cache: %s. Removing...\n", pathCached)
		os.Remove(pathCached)
	}
	os.Remove(gcsTmpFile)
	cmdline := genCURLCmdLine(resURL, proxyURL, smap.Tmap)
	tutils.Logf("First time download via XML API: %s\n", cmdline)
	out, err := exec.Command("curl", cmdline...).CombinedOutput()
	tutils.Logln(string(out))
	tassert.CheckFatal(t, err)

	pathCached = pathForCached()
	tassert.Fatalf(t, pathCached != "", "object was not cached")

	defer func() {
		os.Remove(gcsTmpFile)
		os.Remove(pathCached)
	}()

	tutils.Logf("Cached at: %q\n", pathCached)
	speedCold := extractSpeed(out)
	tassert.Fatalf(t, speedCold != 0, "Failed to detect speed for cold download")

	tutils.Logf("HTTP download\n")
	cmdline = genCURLCmdLine(resURL, proxyURL, smap.Tmap)
	out, err = exec.Command("curl", cmdline...).CombinedOutput()
	tutils.Logln(string(out))
	tassert.CheckFatal(t, err)
	speedHTTP := extractSpeed(out)
	tassert.Fatalf(t, speedHTTP != 0, "Failed to detect speed for HTTP download")

	/*
		TODO: uncomment when target supports HTTPS client

		tutils.Logf("HTTPS download\n")
		cmdline = genCURLCmdLine(true, true, proxyURL, smap.Tmap)
		out, err = exec.Command("curl", cmdline...).CombinedOutput()
		tutils.Logln(string(out))
		tassert.CheckFatal(t, err)
		speedHTTPS := extractSpeed(out)
		tassert.Fatalf(t, speedHTTPS != 0, "Failed to detect speed for HTTPS download")


		tutils.Logf("Check via JSON API\n")
		cmdline = genCURLCmdLine(false, false, proxyURL, smap.Tmap)
		tutils.Logf("JSON: %s\n", cmdline)
		out, err = exec.Command("curl", cmdline...).CombinedOutput()
		t.Log(string(out))
		tassert.CheckFatal(t, err)
		speedJSON := extractSpeed(out)
		tassert.Fatalf(t, speedJSON != 0, "Failed to detect speed for JSON download")
	*/

	tutils.Logf("Cold download speed:   %s\n", cmn.B2S(speedCold, 1))
	tutils.Logf("HTTP download speed:   %s\n", cmn.B2S(speedHTTP, 1))
	/*
		TODO: uncomment when target supports HTTPS client

		tutils.Logf("HTTPS download speed:  %s\n", cmn.B2S(speedHTTPS, 1))
		tutils.Logf("JSON download speed:   %s\n", cmn.B2S(speedJSON, 1))
	*/
	tutils.Logf("HTTP (cached) is %.1f times faster than Cold\n", float64(speedHTTP)/float64(speedCold))
}

func TestRProxyInvalidURL(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	client := tutils.NewClientWithProxy(proxyURL)

	tests := []struct {
		url        string
		statusCode int
	}{
		{url: "http://archive.ics.uci.edu/ml/datasets/Abalone", statusCode: http.StatusBadRequest},
		{url: "http://storage.googleapis.com/kubernetes-release/release", statusCode: http.StatusNotFound},
		{url: "http://invalid.invaliddomain.com/test/webpage.txt", statusCode: http.StatusBadRequest}, // Invalid domain
	}

	for _, test := range tests {
		hbo, err := cmn.NewHTTPObjPath(test.url)
		tassert.CheckError(t, err)
		api.DestroyBucket(baseParams, hbo.Bck)

		res, err := client.Get(test.url)
		tassert.CheckFatal(t, err)
		res.Body.Close()
		tassert.Errorf(t, res.StatusCode == test.statusCode, "%q: expected status %d - got %d", test.url, test.statusCode, res.StatusCode)

		_, err = api.HeadBucket(baseParams, hbo.Bck)
		tassert.Errorf(t, err != nil, "shouldn't create bucket (%s) for invalid resource URL", hbo.Bck)
		api.DestroyBucket(baseParams, hbo.Bck)
	}
}
