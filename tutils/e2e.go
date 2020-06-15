// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/onsi/gomega"
)

type (
	E2EFramework struct {
		Dir string
	}
)

func destroyMatchingBuckets(subName string) (err error) {
	proxyURL := GetPrimaryURL()
	baseParams := BaseAPIParams(proxyURL)

	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks{
		Provider: cmn.ProviderAIS,
	})
	if err != nil {
		return err
	}

	for _, bck := range bcks {
		if !strings.Contains(bck.Name, subName) {
			continue
		}
		if errD := api.DestroyBucket(baseParams, bck); errD != nil && err == nil {
			err = errD
		}
	}

	return err
}

func randomTarget() string {
	smap, err := api.GetClusterMap(BaseAPIParams(proxyURLReadOnly))
	cmn.AssertNoErr(err)
	si, err := smap.GetRandTarget()
	cmn.AssertNoErr(err)
	return si.DaemonID
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

func isLineRegex(msg string) bool {
	return len(msg) > 2 && msg[0] == '^' && msg[len(msg)-1] == '$'
}

func (f *E2EFramework) RunE2ETest(fileName string) {
	var (
		outs []string

		lastResult = ""
		bucket     = GenRandomString(10)
		space      = regexp.MustCompile(`\s+`) // used to replace all whitespace with single spaces
		targetID   = randomTarget()

		inputFileName   = fileName + ".in"
		outputFileName  = fileName + ".stdout"
		cleanupFileName = fileName + ".cleanup"
	)

	// Create random file
	tmpFile, err := ioutil.TempFile("", "e2e-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	object := tmpFile.Name()
	tmpFile.Close()
	defer os.RemoveAll(object)

	substituteVariables := func(s string) string {
		s = strings.ReplaceAll(s, "$BUCKET", bucket)
		s = strings.ReplaceAll(s, "$OBJECT", object)
		s = strings.ReplaceAll(s, "$RANDOM_TARGET", targetID)
		s = strings.ReplaceAll(s, "$DIR", f.Dir)
		s = strings.ReplaceAll(s, "$RESULT", lastResult)
		return s
	}

	defer func() {
		if err := destroyMatchingBuckets(bucket); err != nil {
			Logf("failed to remove buckets: %v", err)
		}

		f, err := os.Open(cleanupFileName)
		if err != nil {
			return
		}
		defer f.Close()
		for _, line := range readContent(f, true /*ignoreEmpty*/) {
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
		if strings.Contains(scmd, " //") {
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
				if strings.HasPrefix(comment, `"`) && strings.HasSuffix(comment, `"`) {
					firstIdx := strings.Index(comment, `"`)
					lastIdx := strings.LastIndex(comment, `"`)
					expectFailMsg = comment[firstIdx+1 : lastIdx]
					expectFailMsg = substituteVariables(expectFailMsg)
					if !isLineRegex(expectFailMsg) {
						expectFailMsg = strings.ToLower(expectFailMsg)
					}
				}
			}
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
		} else {
			var desc string
			if ee, ok := err.(*exec.ExitError); ok {
				desc = string(ee.Stderr)
			}
			desc = fmt.Sprintf("cmd: %q, err: %s", cmd.String(), desc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), desc)
		}

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
