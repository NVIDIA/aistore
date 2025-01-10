// Package s3_integration provides tests of compatibility with AWS S3
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3_test

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var testTools = []string{"aws", "s3cmd"}

func TestE2ES3(t *testing.T) {
	for _, testTool := range testTools {
		if _, err := exec.LookPath(testTool); err != nil {
			t.Fatalf("exec.LookupPath(\"%s\") failed: %v", testTool, err)
		}
	}

	tools.InitLocalCluster()

	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}

// testE2ES3ParseVersion computes a uint64 representation of versionAsString
// with the assumption that it is in the form of either "X", "X.Y", or "X.Y.Z"
// where each of "X", "Y", and "Z" are a decimal representation of a uint16 value.
func testE2ES3ParseVersion(versionAsString string) (versionAsUint64 uint64, ok bool) {
	var (
		major uint16
		minor uint16
		patch uint16
		extra string
		n     int
		err   error
	)

	n, err = fmt.Sscanf(versionAsString, "%d.%d.%d%s", &major, &minor, &patch, &extra)
	if (err == io.EOF) && (n == 3) {
		return (uint64(major) << 32) + (uint64(minor) << 16) + uint64(patch), true
	}
	n, err = fmt.Sscanf(versionAsString, "%d.%d%s", &major, &minor, &extra)
	if (err == io.EOF) && (n == 2) {
		return (uint64(major) << 32) + (uint64(minor) << 16), true
	}
	n, err = fmt.Sscanf(versionAsString, "%d%s", &major, &extra)
	if (err == io.EOF) && (n == 1) {
		return (uint64(major) << 32), true
	}

	return 0, false
}

// testE2ES3ToIncludeFile parses inFileName looking for tool version bounds (supporting
// <= and >= only) and, if present, compares against the supplied tool version to determine
// whether inFileName's version is compatible.
func testE2ES3ToIncludeFile(toolVersion uint64, inFileName string) bool {
	var andBeyond bool

	droppedDotIn := strings.TrimSuffix(inFileName, ".in")

	splitOnUnderscore := strings.Split(droppedDotIn, "_")
	if len(splitOnUnderscore) < 2 {
		return true
	}

	fileVersionWithPlusOrMinus := splitOnUnderscore[len(splitOnUnderscore)-1]
	if len(fileVersionWithPlusOrMinus) < 2 {
		return true
	}

	switch fileVersionWithPlusOrMinus[len(fileVersionWithPlusOrMinus)-1] {
	case '-':
		// andBeyond == false
	case '+':
		andBeyond = true
	default:
		return true
	}

	fileVersionAsString := fileVersionWithPlusOrMinus[:len(fileVersionWithPlusOrMinus)-1]

	fileVersionAsUint64, ok := testE2ES3ParseVersion(fileVersionAsString)
	if !ok {
		return true
	}

	if andBeyond {
		return fileVersionAsUint64 <= toolVersion
	}
	return fileVersionAsUint64 >= toolVersion
}

var _ = Describe("E2E AWS Compatibility Tests", func() {
	var (
		err                           error
		host                          string
		params                        string
		s3cmdVersion                  uint64
		s3cmdVersionKnown             bool
		s3cmdVersionOutput            []byte
		s3cmdVersionOutputStringSplit []string
	)

	if value := os.Getenv(env.AisUseHTTPS); cos.IsParseBool(value) {
		host = "https://localhost:8080/s3"
		params = "--no-check-certificate"
	} else {
		host = "http://localhost:8080/s3"
		params = "--no-ssl --no-check-certificate"
	}

	s3cmdVersionOutput, err = exec.Command("s3cmd", "--version").Output()
	if err == nil {
		s3cmdVersionOutputStringSplit =
			strings.Split(strings.TrimRight(string(s3cmdVersionOutput), " \n"), " ")
		s3cmdVersion, s3cmdVersionKnown = testE2ES3ParseVersion(
			s3cmdVersionOutputStringSplit[len(s3cmdVersionOutputStringSplit)-1])
	}

	var (
		f = &tools.E2EFramework{
			Vars: map[string]string{"HOST": host, "PARAMS": params},
		}
		files, _ = filepath.Glob("./*.in")
		args     = make([]any, 0, len(files)+1)
	)
	args = append(args, f.RunE2ETest)
	for _, fileName := range files {
		if !s3cmdVersionKnown || testE2ES3ToIncludeFile(s3cmdVersion, fileName) {
			fileName = fileName[:len(fileName)-len(filepath.Ext(fileName))]
			args = append(args, Entry(fileName, fileName))
		}
	}
	DescribeTable("e2e-s3", args...)
})
