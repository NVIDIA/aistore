// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type statsOutput struct {
	file     *os.File
	filePath string // local filename (or stdout) for upload, cleanup, and recovery
	writer   *bufio.Writer

	// object upload fields
	isTmp   bool // file is a temporary file
	retain  bool // keep file after failure
	bck     cmn.Bck
	objName string
}

func (p *params) checkStatsOutputObj() error {
	benchmarkBck, err := parseBucket(p.bck)
	if err != nil {
		return err
	}
	if !p.statsBck.Equal(&benchmarkBck) {
		if _, err := api.HeadBucket(p.bp, p.statsBck, false); err != nil {
			return fmt.Errorf("failed to check '-stats-output-obj=%s' bucket: %w", p.statsOutputObj, err)
		}
	}
	warnOverwrite(p.bp, p.statsBck, p.statsOutputObjName, p.statsOutputObj, "statistics upload")
	return nil
}

func (p *params) openStatsOutput() (*statsOutput, error) {
	o := &statsOutput{}
	if p.dryRun {
		return o, nil
	}

	var err error
	if p.statsOutputObj != "" {
		o.bck, o.objName = p.statsBck, p.statsOutputObjName
	}

	switch {
	case p.statsOutput != "":
		o.file, err = cos.CreateFile(p.statsOutput)
	case p.statsOutputObj != "":
		o.isTmp = true
		if err := cos.CreateDir(p.tmpDir); err != nil {
			return nil, fmt.Errorf("failed to create statistics temporary directory %q: %w", p.tmpDir, err)
		}
		o.file, err = os.CreateTemp(p.tmpDir, "aisloader-stats-*")
	default:
		o.file = os.Stdout
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create statistics output: %w", err)
	}

	o.filePath = o.file.Name()
	if o.isTmp {
		fmt.Fprintln(os.Stderr, "Statistics recovery path:", o.filePath)
	}
	var writer io.Writer = o.file
	if p.statsOutput == "" && p.statsOutputObj != "" {
		writer = cos.NewWriterMulti(o.file, os.Stdout)
	}
	o.writer = bufio.NewWriter(writer) // remembers first write error for easier error reporting
	return o, nil
}

func (o *statsOutput) close() error {
	if o.file == nil || o.file == os.Stdout {
		return nil
	}
	f := o.file
	// clear handle so deferred cleanup does not close it again
	o.file = nil
	return f.Close()
}

func (o *statsOutput) closeAndRetain(errWrite error) error {
	o.retain = true // keep incomplete statistics
	err := errors.Join(fmt.Errorf("failed to write statistics output: %w", errWrite), o.close())
	if o.filePath != os.Stdout.Name() {
		err = fmt.Errorf("%w; incomplete statistics retained at %s", err, o.filePath)
	}
	return err
}

func (o *statsOutput) closeAndUpload(bp api.BaseParams) error {
	if err := o.close(); err != nil {
		o.retain = true
		return fmt.Errorf("failed to close statistics output: %w; statistics retained at %s", err, o.filePath)
	}
	if !o.isTmp && o.filePath != os.Stdout.Name() {
		fmt.Println("Stats written to", o.filePath)
	}
	if o.bck.IsEmpty() {
		return nil
	}

	if err := o.upload(bp); err != nil {
		o.retain = true // upload may fail, keep file just in case
		return fmt.Errorf("%w; statistics retained at %s", err, o.filePath)
	}
	fmt.Println("Stats written to", o.bck.Cname(o.objName))
	return nil
}

func (o *statsOutput) closeAndRemoveTemp() error {
	err := o.close()
	if o.isTmp && !o.retain {
		err = errors.Join(err, cos.RemoveFile(o.filePath))
	}
	return err
}

func (o *statsOutput) upload(bp api.BaseParams) error {
	r, err := cos.NewFileHandle(o.filePath) // reopen file for upload
	if err != nil {
		return err
	}
	defer r.Close()
	info, err := r.Stat()
	if err != nil {
		return err
	}
	args := api.PutArgs{BaseParams: bp, Bck: o.bck, ObjName: o.objName, Reader: r, Size: uint64(info.Size())}
	if _, err := api.PutObject(&args); err != nil {
		return fmt.Errorf("failed to put '-stats-output-obj=%s': %w", o.bck.Cname(o.objName), err)
	}
	return nil
}
