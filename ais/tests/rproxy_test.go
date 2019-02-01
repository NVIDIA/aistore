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
	GCS_FILENAME = "LT80400212013126LGN01_B10.TIF"
	GCS_OBJ_XML  = "LT08/PRE/040/021/LT80400212013126LGN01/" + GCS_FILENAME
	// Public GCS bucket
	GCS_BCK = "gcp-public-data-landsat"
	// wihtout this query GCS returns only object's information
	GCS_QRY       = "?alt=media"
	GCS_HOST_JSON = "www.googleapis.com"
	GCS_HOST_XML  = "storage.googleapis.com"
	GCS_TMP_FILE  = "/tmp/rproxy_test_download.tiff"
)

var (
	// reformat object name from XML to JSON API requirements
	GCS_OBJ_JSON = strings.Replace(GCS_OBJ_XML, "/", "%2F", -1)
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

		if strings.HasSuffix(path, GCS_FILENAME) {
			fpath = path
			return filepath.SkipDir
		}
		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)
	return fpath
}

// generate URL to request object from GCS
func genObjURL(is_secure, is_xml bool) (s string) {
	if is_secure {
		s = "https://"
	} else {
		s = "http://"
	}
	if is_xml {
		s += fmt.Sprintf("%s/%s/%s", GCS_HOST_XML, GCS_BCK, GCS_OBJ_XML)
	} else {
		s += fmt.Sprintf("%s/storage/v1/b/%s/o/%s", GCS_HOST_JSON, GCS_BCK, GCS_OBJ_JSON)
	}
	return s
}

// build command line for CURL
func genCURLCmdLine(is_secure, is_xml bool, proxyURL string) []string {
	return []string{
		"-L", "-X", "GET",
		fmt.Sprintf("%s%s", genObjURL(is_secure, is_xml), GCS_QRY),
		"-o", GCS_TMP_FILE,
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
		t.Skip("Test requires the cluster deployed in reverse proxy mode")
	}

	// look for leftovers and cleanup if found
	pathCached := pathForCached(GCS_FILENAME)
	if pathCached != "" {
		tutils.Logf("Found in cache: %s. Removing...\n", pathCached)
		os.Remove(pathCached)
	}
	if _, err := os.Stat(GCS_TMP_FILE); err == nil {
		os.Remove(GCS_TMP_FILE)
	}

	tutils.Logf("First time download via XML API\n")
	cmdline := genCURLCmdLine(false, true, proxyURL)
	out, err := exec.Command("curl", cmdline...).CombinedOutput()
	t.Log(string(out))
	tutils.CheckFatal(err, t)

	pathCached = pathForCached(GCS_FILENAME)
	if pathCached == "" {
		t.Fatalf("Object was not cached")
	}
	defer func() {
		os.Remove(GCS_TMP_FILE)
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
