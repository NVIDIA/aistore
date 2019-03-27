// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	// Public object name to download from Google Cloud Storage(GCS)
	GcsFilename = "LT80400212013126LGN01_B10.TIF"
	GcsObjXML   = "LT08/PRE/040/021/LT80400212013126LGN01/" + GcsFilename
	// Public GCS bucket
	GcsBck = "gcp-public-data-landsat"
	// wihtout this query GCS returns only object's information
	GcsQry      = "?alt=media"
	GcsHostJSON = "www.googleapis.com"
	GcsHostXML  = "storage.googleapis.com"
	GcsTmpFile  = "/tmp/rproxy_test_download.tiff"
)

var (
	// reformat object name from XML to JSON API requirements
	GcsObjJSON = strings.Replace(GcsObjXML, "/", "%2F", -1)
)

// search for the full path of cached object
func pathForCached(filename string) string {
	var fpath string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if fpath != "" {
			return filepath.SkipDir
		}
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() || !strings.Contains(path, "/cloud/") {
			return nil
		}

		if strings.HasSuffix(path, GcsFilename) {
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
	if isSecure {
		s = "https://"
	} else {
		s = "http://"
	}
	if isXML {
		s += fmt.Sprintf("%s/%s/%s", GcsHostXML, GcsBck, GcsObjXML)
	} else {
		s += fmt.Sprintf("%s/storage/v1/b/%s/o/%s", GcsHostJSON, GcsBck, GcsObjJSON)
	}
	return s
}

// build command line for CURL
func genCURLCmdLine(isSecure, isXML bool, proxyURL string) []string {
	return []string{
		"-L", "-X", "GET",
		fmt.Sprintf("%s%s", genObjURL(isSecure, isXML), GcsQry),
		"-o", GcsTmpFile,
		"-x", proxyURL,
	}
}

// extract download speed from CURL output (from the last non-empty line)
func extractSpeed(lines []string) int64 {
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
	const coeff = 3 // cached download speed must be at least coeff times faster

	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	config := getDaemonConfig(t, proxyURL)

	// the test requires very specific configuration that cannot be enabled
	// on the fly. That is why it is not Fatal error
	if config.Net.HTTP.RevProxy != cmn.RevProxyCloud {
		t.Skipf("%s requires the cluster deployed in reverse proxy mode", t.Name())
	}

	// look for leftovers and cleanup if found
	pathCached := pathForCached(GcsFilename)
	if pathCached != "" {
		tutils.Logf("Found in cache: %s. Removing...\n", pathCached)
		os.Remove(pathCached)
	}
	if _, err := os.Stat(GcsTmpFile); err == nil {
		os.Remove(GcsTmpFile)
	}

	tutils.Logf("First time download via XML API\n")
	cmdline := genCURLCmdLine(false, true, proxyURL)
	out, err := exec.Command("curl", cmdline...).CombinedOutput()
	t.Log(string(out))
	tutils.CheckFatal(err, t)

	pathCached = pathForCached(GcsFilename)
	if pathCached == "" {
		t.Fatalf("Object was not cached")
	}
	defer func() {
		os.Remove(GcsTmpFile)
		os.Remove(pathCached)
	}()

	tutils.Logf("Cached at: %q\n", pathCached)
	lines := strings.Split(string(out), "\n")
	speedCold := extractSpeed(lines)
	if speedCold == 0 {
		t.Fatal("Failed to detect speed for cold download")
	}

	tutils.Logf("HTTPS download\n")
	cmdline = genCURLCmdLine(true, true, proxyURL)
	out, err = exec.Command("curl", cmdline...).CombinedOutput()
	t.Log(string(out))
	tutils.CheckFatal(err, t)
	lines = strings.Split(string(out), "\n")
	speedHTTPS := extractSpeed(lines)
	if speedHTTPS == 0 {
		t.Fatal("Failed to detect speed for HTTPS download")
	}

	tutils.Logf("Cache check via JSON API\n")
	cmdline = genCURLCmdLine(false, false, proxyURL)
	out, err = exec.Command("curl", cmdline...).CombinedOutput()
	t.Log(string(out))
	tutils.CheckFatal(err, t)
	lines = strings.Split(string(out), "\n")
	speedCache := extractSpeed(lines)
	if speedCache == 0 {
		t.Fatal("Failed to detect speed for cached download")
	}

	tutils.Logf("Cold download speed:   %s\n", cmn.B2S(speedCold, 1))
	tutils.Logf("HTTPS download speed:  %s\n", cmn.B2S(speedHTTPS, 1))
	tutils.Logf("Cached download speed: %s\n", cmn.B2S(speedCache, 1))
	tutils.Logf("Cached is %v times faster than Cold\n", speedCache/speedCold)
	if speedCache < speedCold*coeff || speedCache < speedHTTPS*coeff {
		t.Error("Downloading did not use the cache")
	}
}
