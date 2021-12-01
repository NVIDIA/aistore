// Package soakprim provides the framework for running soak tests
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package soakprim

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

type (
	aisloaderResponse struct {
		Get stats.AISLoaderStat `json:"get"`
		Put stats.AISLoaderStat `json:"put"`
		Cfg stats.AISLoaderStat `json:"cfg"`
	}

	AISLoaderExecParams struct {
		pctput       int
		duration     time.Duration
		totalputsize int64
		minsize      int64
		maxsize      int64
		readoff      int64
		readlen      int64
		stopCh       chan struct{}
		stopable     bool

		verifyhash bool
	}
)

const (
	soaktestDirname = "/tmp/ais-soak"
)

var aisloaderPath string

func init() {
	os.MkdirAll(soaktestDirname, cos.PermRWXRX)

	// We expect the `aisloader` binary to be present in `$GOPATH/bin` directory.
	aisloaderPath = filepath.Join(os.Getenv("GOPATH"), "bin", "aisloader")
	_, err := os.Stat(aisloaderPath)
	if err != nil {
		cos.ExitLogf("'aisloader' binary is not present in %q (err: %v)", aisloaderPath, err)
	}
}

func AISExec(ch chan *stats.PrimitiveStat, opType string, bck cmn.Bck, numWorkers int, params *AISLoaderExecParams) {
	filebasename := cos.RandString(13)
	filename := path.Join(soaktestDirname, filebasename+".json")
	defer os.Remove(filename)

	getConfig := false
	if opType == soakcmn.OpTypeCfg {
		getConfig = true
	}

	// Using the default readertype sgl for now. If later we decided to use file, need to also set aisloader tmpdir to be a unique folder per call.
	spf := fmt.Sprintf
	cmd := exec.Command(aisloaderPath,
		spf("-ip=%s", primaryIP), spf("-port=%s", primaryPort),
		spf("-bucket=%s", bck.Name),
		spf("-provider=%s", bck.Provider),
		spf("-numworkers=%v", numWorkers),
		spf("-pctput=%v", params.pctput),
		spf("-duration=%s", params.duration),
		spf("-getconfig=%t", getConfig),
		spf("-totalputsize=%v", params.totalputsize),
		spf("-verifyhash=%t", params.verifyhash),
		spf("-minsize=%v", params.minsize),
		spf("-maxsize=%v", params.maxsize),
		spf("-readoff=%v", params.readoff),
		spf("-readlen=%v", params.readlen),
		spf("-stats-output=%s", filename),
		"-statsinterval=0", "-cleanup=false", "-json=true")
	cmd.Dir = soaktestDirname

	var out []byte
	var err error

	if params.stopable {
		if err = cmd.Start(); err == nil {
			<-params.stopCh
			err = cmd.Process.Signal(syscall.SIGTERM)
			if err != nil {
				cmd.Process.Kill()
			} else {
				err = cmd.Wait()
			}
			out = []byte("<Regression Run>")
		}
	} else {
		out, err = cmd.Output()
	}

	if err != nil {
		report.Writef(report.SummaryLevel, "ais error: %v\n", err.Error())
		report.Writef(report.DetailLevel, "-----aisloader output-----\n%v\n-----end aisloader output-----\n\n", string(out))
		ch <- &stats.PrimitiveStat{Fatal: true, AISLoaderStat: stats.AISLoaderStat{StartTime: time.Now()}}
		if strings.Contains(err.Error(), "signal: interrupt") {
			Terminate()
		}
		return
	}

	result, err := os.ReadFile(filename)
	if err != nil {
		report.Writef(report.SummaryLevel, "error reading ais results from %v\n", filename)
		ch <- &stats.PrimitiveStat{Fatal: true, AISLoaderStat: stats.AISLoaderStat{StartTime: time.Now()}}
		return
	}

	aisloaderStats, err := parseAisloaderResponse(opType, result)
	if err != nil {
		report.Writef(report.SummaryLevel, "error parsing aisloader response")
		ch <- &stats.PrimitiveStat{Fatal: true}
		return
	}

	ch <- aisloaderStats
}

func parseAisloaderResponse(opType string, response []byte) (*stats.PrimitiveStat, error) {
	aisloaderresp := make([]aisloaderResponse, 0)
	err := jsoniter.Unmarshal(response, &aisloaderresp)
	if err != nil {
		return nil, err
	}

	if len(aisloaderresp) == 0 {
		return nil, errors.New("aisloader returned empty response, expected at least summary")
	}

	if opType == soakcmn.OpTypeGet {
		primitiveStat := stats.PrimitiveStat{
			AISLoaderStat: aisloaderresp[len(aisloaderresp)-1].Get, // The last element of aisloader response is the summary
			OpType:        soakcmn.OpTypeGet,
		}
		return &primitiveStat, nil
	}

	if opType == soakcmn.OpTypePut {
		primitiveStat := stats.PrimitiveStat{
			AISLoaderStat: aisloaderresp[len(aisloaderresp)-1].Put, // The last element of aisloader response is the summary
			OpType:        soakcmn.OpTypePut,
		}
		return &primitiveStat, nil
	}

	if opType == soakcmn.OpTypeCfg {
		primitiveStat := stats.PrimitiveStat{
			AISLoaderStat: aisloaderresp[len(aisloaderresp)-1].Cfg, // The last element of aisloader response is the summary
			OpType:        soakcmn.OpTypeCfg,
		}
		return &primitiveStat, nil
	}

	return nil, fmt.Errorf("not a valid operation type %v", opType)
}
