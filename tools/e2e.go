// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"bufio"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

type E2EFramework struct {
	Dir  string
	Vars map[string]string // Custom variables passed to input and output files.
}

func (f *E2EFramework) RunE2ETest(fileName string) {
	var (
		outs []string

		lastResult = ""
		bucket     = strings.ToLower(trand.String(10))
		space      = regexp.MustCompile(`\s+`) // Used to replace all whitespace with single spaces.
		target     = randomTarget()
		mountpath  = randomMountpath(target)
		backends   = getConfiguredBackends()
		etlName    = "etlname-" + strings.ToLower(trand.String(4))

		inputFileName   = fileName + ".in"
		outputFileName  = fileName + ".stdout"
		cleanupFileName = fileName + ".cleanup"
	)

	// Create random file.
	tmpFile, err := os.CreateTemp("", "e2e-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	object := tmpFile.Name()
	tmpFile.Close()
	defer os.RemoveAll(object)

	substituteVariables := func(s string) string {
		s = strings.ReplaceAll(s, "$BUCKET", bucket)
		s = strings.ReplaceAll(s, "$OBJECT", object)
		s = strings.ReplaceAll(s, "$RANDOM_TARGET", target.ID())
		s = strings.ReplaceAll(s, "$RANDOM_MOUNTPATH", mountpath)
		s = strings.ReplaceAll(s, "$DIR", f.Dir)
		s = strings.ReplaceAll(s, "$RESULT", lastResult)
		s = strings.ReplaceAll(s, "$BACKENDS", strings.Join(backends, ","))
		s = strings.ReplaceAll(s, "$ETL_NAME", etlName)
		for k, v := range f.Vars {
			s = strings.ReplaceAll(s, "$"+k, v)
		}
		return s
	}

	defer func() {
		if err := destroyMatchingBuckets(bucket); err != nil {
			tlog.Logf("failed to remove buckets: %v", err)
		}

		fh, err := os.Open(cleanupFileName)
		if err != nil {
			return
		}
		defer fh.Close()
		for _, line := range readContent(fh, true /*ignoreEmpty*/) {
			scmd := substituteVariables(line)
			_ = exec.Command("bash", "-c", scmd).Run()
		}
	}()

	inFile, err := os.Open(inputFileName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer inFile.Close()

	for _, scmd := range readContent(inFile, true /*ignoreEmpty*/) {
		var (
			saveResult    = false
			ignoreOutput  = false
			expectFail    = false
			expectFailMsg = ""
		)

		// Parse comment if present.
		switch {
		case strings.Contains(scmd, " //"):
			var comment string
			tmp := strings.Split(scmd, " //")
			scmd, comment = tmp[0], tmp[1]
			if strings.Contains(comment, "SAVE_RESULT") {
				saveResult = true
			}
			if strings.Contains(comment, "IGNORE") {
				ignoreOutput = true
			}
			if strings.Contains(comment, "FAIL") {
				expectFail = true
				if strings.Count(comment, `"`) >= 2 {
					firstIdx := strings.Index(comment, `"`)
					lastIdx := strings.LastIndex(comment, `"`)
					expectFailMsg = comment[firstIdx+1 : lastIdx]
					expectFailMsg = substituteVariables(expectFailMsg)
					if !isLineRegex(expectFailMsg) {
						expectFailMsg = strings.ToLower(expectFailMsg)
					}
				}
			}
		case strings.HasPrefix(scmd, "// RUN"):
			comment := strings.TrimSpace(strings.TrimPrefix(scmd, "// RUN"))

			switch comment {
			case "local-deployment":
				// Skip running test if requires local deployment and the cluster
				// is not in testing env.
				config, err := getClusterConfig()
				cos.AssertNoErr(err)
				if !config.TestingEnv() {
					tlog.Logfln("SKIPPING %q: requires local deployment", fileName)
					ginkgo.Skip("requires local deployment")
					return
				}

				continue
			case "authn":
				// Skip running AuthN e2e tests if the former is not enabled
				// (compare w/ `SkipTestArgs.RequiresAuth`)
				if config, err := getClusterConfig(); err == nil && config.Auth.Enabled {
					continue
				}
				tlog.Logfln("SKIPPING %q: AuthN not enabled", fileName)
				ginkgo.Skip("AuthN not enabled - skipping")
				return
			case "k8s":
				// Skip if we can't verify or aren't on a Kubernetes cluster
				status, err := isClusterK8s()
				if err != nil {
					tlog.Logfln("SKIPPING %q: cannot determine cluster type: %v", fileName, err)
					ginkgo.Skip(fmt.Sprintf("cannot determine cluster type: %v", err))
				}
				if !status {
					tlog.Logfln("SKIPPING %q: requires Kubernetes deployment", fileName)
					ginkgo.Skip("requires Kubernetes deployment")
				}
				// Otherwise, we’re on K8s – run the test step
				continue
			case "remais":
				// Skip if no remote AIS clusters are attached
				if RemoteCluster.UUID == "" {
					tlog.Logfln("SKIPPING %q: no remote AIS clusters attached", fileName)
					ginkgo.Skip("no remote AIS clusters attached")
					return
				}
				continue
			case "clean-cluster":
				// Skip if there are pre-existing buckets in the cluster
				if hasExistingBuckets() {
					tlog.Logfln("SKIPPING %q: cluster has pre-existing buckets", fileName)
					ginkgo.Skip("cluster has pre-existing buckets")
					return
				}
				continue
			default:
				cos.AssertMsg(false, "invalid run mode: "+comment)
			}
		case strings.HasPrefix(scmd, "// SKIP"):
			message := strings.TrimSpace(strings.TrimPrefix(scmd, "// SKIP"))
			message = strings.Trim(message, `"`)
			tlog.Logfln("SKIPPING %q: %s", fileName, message)
			ginkgo.Skip(message)
			return
		}

		scmd = substituteVariables(scmd)
		if strings.Contains(scmd, "$PRINT_SIZE") {
			// Expecting: $PRINT_SIZE FILE_NAME
			fileName := strings.ReplaceAll(scmd, "$PRINT_SIZE ", "")
			scmd = fmt.Sprintf("wc -c %s | awk '{print $1}'", fileName)
		}
		cmd := exec.Command("bash", "-c", scmd)
		b, err := cmd.Output()
		if expectFail {
			var desc string
			if ee, ok := err.(*exec.ExitError); ok {
				desc = strings.ToLower(string(ee.Stderr))
			}
			gomega.Expect(err).To(gomega.HaveOccurred(), "expected FAIL but command succeeded")
			gomega.Expect(desc).To(gomega.ContainSubstring(expectFailMsg))
			continue
		}
		var desc string
		if ee, ok := err.(*exec.ExitError); ok {
			desc = string(ee.Stderr)
		}
		desc = fmt.Sprintf("cmd: %q, err: %s", cmd.String(), desc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), desc)

		if saveResult {
			lastResult = strings.TrimSpace(string(b))
		} else if !ignoreOutput {
			out := strings.Split(string(b), "\n")
			if out[len(out)-1] == "" {
				out = out[:len(out)-1]
			}
			outs = append(outs, out...)
		}
	}

	outFile, err := os.Open(outputFileName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer outFile.Close()

	outLines := readContent(outFile, false /*ignoreEmpty*/)
	for idx, line := range outLines {
		gomega.Expect(idx).To(
			gomega.BeNumerically("<", len(outs)),
			"output file has more lines that were produced",
		)
		expectedOut := space.ReplaceAllString(line, "")
		expectedOut = substituteVariables(expectedOut)

		out := strings.TrimSpace(outs[idx])
		out = space.ReplaceAllString(out, "")
		// Sometimes quotation marks are returned which are not visible on
		// console so we just remove them.
		out = strings.ReplaceAll(out, "&#34;", "")
		if isLineRegex(expectedOut) {
			gomega.Expect(out).To(gomega.MatchRegexp(expectedOut))
		} else {
			gomega.Expect(out).To(gomega.Equal(expectedOut), "%s: %d", outputFileName, idx+1)
		}
	}

	gomega.Expect(len(outLines)).To(
		gomega.Equal(len(outs)),
		"more lines were produced than were in output file",
	)
}

//
// helper methods
//

func destroyMatchingBuckets(subName string) (err error) {
	proxyURL := GetPrimaryURL()
	bp := BaseAPIParams(proxyURL)

	bcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS}, apc.FltExists)
	if err != nil {
		return err
	}

	if RemoteCluster.UUID != "" {
		remoteBcks, errR := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}, apc.FltExists)
		if errR == nil {
			bcks = append(bcks, remoteBcks...)
		}
	}

	for _, bck := range bcks {
		if !strings.Contains(bck.Name, subName) {
			continue
		}
		if errD := api.DestroyBucket(bp, bck); errD != nil && err == nil {
			err = errD
		}
	}

	return err
}

func randomTarget() *meta.Snode {
	smap, err := api.GetClusterMap(BaseAPIParams(proxyURLReadOnly))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	si, err := smap.GetRandTarget()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return si
}

func randomMountpath(target *meta.Snode) string {
	mpaths, err := api.GetMountpaths(BaseAPIParams(proxyURLReadOnly), target)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(len(mpaths.Available)).NotTo(gomega.Equal(0))
	return mpaths.Available[rand.IntN(len(mpaths.Available))]
}

func getConfiguredBackends() []string {
	backends, err := api.GetConfiguredBackends(BaseAPIParams(proxyURLReadOnly))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return backends
}

func readContent(r io.Reader, ignoreEmpty bool) []string {
	var (
		scanner = bufio.NewScanner(r)
		lines   = make([]string, 0, 4)
	)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" && ignoreEmpty {
			continue
		}
		lines = append(lines, line)
	}
	gomega.Expect(scanner.Err()).NotTo(gomega.HaveOccurred())
	return lines
}

// hasExistingBuckets checks if there are any existing AIS or remote AIS buckets in the cluster
func hasExistingBuckets() bool {
	proxyURL := GetPrimaryURL()
	bp := BaseAPIParams(proxyURL)

	// Check for AIS buckets
	aisBcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS}, apc.FltExists)
	if err == nil && len(aisBcks) > 0 {
		return true
	}

	// Check for remote AIS buckets
	remaisBcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}, apc.FltExists)
	if err == nil && len(remaisBcks) > 0 {
		return true
	}

	return false
}

func isLineRegex(msg string) bool {
	return len(msg) > 2 && msg[0] == '^' && msg[len(msg)-1] == '$'
}
