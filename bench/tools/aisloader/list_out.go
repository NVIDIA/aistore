// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// validate options for '-list-out', which lists the bucket and exits
// (benchmark run options, e.g. '-split-reads' and '-cleanup', do not apply)
func (p *params) validateListOut() error {
	if isDirectS3() {
		return errors.New("'-list-out' with direct S3 access is not supported")
	}
	if p.fileList != "" || p.fileListObj != "" {
		return errors.New("'-list-out' cannot be combined with '-filelist' or '-filelist-obj'")
	}
	if p.getLoaderID {
		return errors.New("'-list-out' cannot be combined with '-getloaderid'")
	}
	var err error
	if p.listOutBck, p.listOutObjName, err = parseObjectURI(p.listOut); err != nil {
		return fmt.Errorf("invalid '-list-out=%s': %w", p.listOut, err)
	}

	bck, err := parseBucket(p.bck)
	if err != nil {
		return fmt.Errorf("invalid '-bucket=%s': %w", p.bck.Name, err)
	}
	if bck.Name == "" {
		return errors.New("'-list-out' requires '-bucket'")
	}
	if err := p.validateListFlags(bck); err != nil {
		return err
	}
	if err := p.cleanSubdir(); err != nil {
		return err
	}
	if p.listOutBck.Equal(&bck) {
		fmt.Fprintln(os.Stderr, "Warning: object list saved to the benchmark bucket may be included as a workload object in later runs:", p.listOut)
	}
	p.bck = bck
	return nil
}

func (p *params) checkListOut() error {
	if _, err := api.HeadBucket(p.bp, p.listOutBck, true); err != nil {
		return fmt.Errorf("failed to check '-list-out=%s' bucket: %w", p.listOut, err)
	}
	warnOverwrite(p.bp, p.listOutBck, p.listOutObjName, p.listOut, "object list upload")
	return nil
}

func (p *params) writeObjList(names []string) error {
	if p.dryRun {
		fmt.Printf("Found %s object name%s for '-list-out'\n", cos.FormatBigInt(len(names)), cos.Plural(len(names)))
		return nil
	}
	if len(names) == 0 {
		fmt.Fprintln(os.Stderr, "Warning: no objects listed; writing an empty list to", p.listOut)
	}

	var b bytes.Buffer
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte('\n')
	}
	args := api.PutArgs{
		BaseParams: p.bp,
		Bck:        p.listOutBck,
		ObjName:    p.listOutObjName,
		Reader:     cos.NewByteReader(b.Bytes()),
		Size:       uint64(b.Len()),
	}
	if _, err := api.PutObject(&args); err != nil {
		return fmt.Errorf("failed to put '-list-out=%s': %w", p.listOut, err)
	}
	fmt.Printf("Wrote %s object name%s to %s\n", cos.FormatBigInt(len(names)), cos.Plural(len(names)), p.listOut)
	return nil
}
