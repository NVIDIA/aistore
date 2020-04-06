// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"bufio"
	"fmt"
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

	bcks, err := api.ListBuckets(baseParams, cmn.Bck{
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

func (f *E2EFramework) RunE2ETest(inputFileName, outputFileName string) {
	var (
		outs []string

		bucket = GenRandomString(10)
		space  = regexp.MustCompile(`\s+`) // used to replace all whitespace with single spaces
	)

	defer func() {
		if err := destroyMatchingBuckets(bucket); err != nil {
			Logf("failed to remove buckets: %v", err)
		}
	}()

	tmpFile, err := ioutil.TempFile("", "e2e-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	object := tmpFile.Name()
	tmpFile.Close()
	defer os.RemoveAll(object)

	inFile, err := os.Open(inputFileName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer inFile.Close()

	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		scmd := line

		var (
			ignoreOutput  = false
			expectFail    = false
			expectFailMsg = ""
		)

		// Parse comment if present.
		if strings.Contains(scmd, " //") {
			var comment string
			tmp := strings.Split(scmd, " //")
			scmd, comment = tmp[0], tmp[1]
			if strings.Contains(comment, "IGNORE") {
				ignoreOutput = true
			}
			if strings.Contains(comment, "FAIL") {
				expectFail = true
				if strings.Count(comment, `"`) == 2 {
					firstIdx := strings.Index(comment, `"`)
					lastIdx := strings.LastIndex(comment, `"`)
					expectFailMsg = comment[firstIdx+1 : lastIdx]
					expectFailMsg = strings.ReplaceAll(expectFailMsg, "$BUCKET", bucket)
					expectFailMsg = strings.ReplaceAll(expectFailMsg, "$OBJECT", object)
					expectFailMsg = strings.ToLower(expectFailMsg)
				}
			}
		}

		scmd = strings.ReplaceAll(scmd, "$BUCKET", bucket)
		scmd = strings.ReplaceAll(scmd, "$OBJECT", object)
		scmd = strings.ReplaceAll(scmd, "$DIR", f.Dir)
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

		if !ignoreOutput {
			out := strings.Split(string(b), "\n")
			if out[len(out)-1] == "" {
				out = out[:len(out)-1]
			}
			outs = append(outs, out...)
		}
	}
	gomega.Expect(scanner.Err()).NotTo(gomega.HaveOccurred())

	outFile, err := os.Open(outputFileName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer outFile.Close()

	scanner = bufio.NewScanner(outFile)
	var idx = 0
	for ; scanner.Scan(); idx++ {
		gomega.Expect(idx).To(
			gomega.BeNumerically("<", len(outs)),
			"output file has more lines that were produced",
		)
		expectedOut := scanner.Text()
		expectedOut = space.ReplaceAllString(expectedOut, "")
		expectedOut = strings.ReplaceAll(expectedOut, "$BUCKET", bucket)
		expectedOut = strings.ReplaceAll(expectedOut, "$OBJECT", object)
		expectedOut = strings.ReplaceAll(expectedOut, "$DIR", f.Dir)

		out := strings.TrimSpace(outs[idx])
		out = space.ReplaceAllString(out, "")
		// Sometimes quotation marks are returned which are not visible on
		// console so we just remove them.
		out = strings.ReplaceAll(out, "&#34;", "")
		if len(expectedOut) > 2 && expectedOut[0] == '^' && expectedOut[len(expectedOut)-1] == '$' {
			gomega.Expect(out).To(gomega.MatchRegexp(expectedOut))
		} else {
			gomega.Expect(out).To(gomega.Equal(expectedOut), "%s: %d", outputFileName, idx+1)
		}
	}

	gomega.Expect(idx).To(
		gomega.Equal(len(outs)),
		"more lines were produced than were in output file",
	)

	gomega.Expect(scanner.Err()).NotTo(gomega.HaveOccurred())
}
