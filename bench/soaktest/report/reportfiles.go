// Package report provides the framework for collecting results of the soaktest
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package report

import (
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/stats"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Static file, used for logging errors and stuff to report logs

const (
	DefaultDir = "/tmp/ais-soak/reports"

	DetailLevel  = 3
	ConsoleLevel = 2
	SummaryLevel = 1
)

var (
	fsummaryPath string
	fdetailPath  string

	fsummary *os.File
	fdetail  *os.File

	regressionWriter stats.StatWriter
	summaryWriter    stats.StatWriter
	detailWriter     stats.StatWriter
	sysinfoWriter    stats.StatWriter
)

func InitReportFiles() {
	dir := soakcmn.Params.ReportDir

	if dir == "" {
		err := os.MkdirAll(DefaultDir, cos.PermRWXRX)
		cos.AssertNoErr(err)
		dir = DefaultDir
	} else {
		info, err := os.Stat(dir)
		cos.AssertNoErr(err)
		if !info.IsDir() {
			cos.AssertNoErr(errors.New("%v is not a folder"))
		}
	}

	t := time.Now()
	suffix := fmt.Sprintf("%04d%02d%02d-%02d%02d%02d.%d",
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		os.Getpid())

	reportPath := fmt.Sprintf("soak-report-%s", suffix)

	fsummaryPath = path.Join(dir, reportPath, fmt.Sprintf("summary-%s.log", suffix))
	fdetailPath = path.Join(dir, reportPath, fmt.Sprintf("detail-%s.log", suffix))

	summaryWriter.Path = path.Join(dir, reportPath, fmt.Sprintf("summary-%s.csv", suffix))
	detailWriter.Path = path.Join(dir, reportPath, fmt.Sprintf("detail-%s.csv", suffix))
	regressionWriter.Path = path.Join(dir, reportPath, fmt.Sprintf("regression-%s.csv", suffix))
	sysinfoWriter.Path = path.Join(dir, reportPath, fmt.Sprintf("sysinfo-%s.csv", suffix))

	var err error
	fsummary, err = cos.CreateFile(fsummaryPath)
	cos.AssertNoErr(err)

	fdetail, err = cos.CreateFile(fdetailPath)
	cos.AssertNoErr(err)

	fmt.Printf("----- Writing Reports to Disk -----\n")
	fmt.Printf("Summary Logs: %v\n", fsummaryPath)
	fmt.Printf("Detailed Logs: %v\n", fdetailPath)
	fmt.Println()
	fmt.Printf("Summary Data: %v\n", summaryWriter.Path)
	fmt.Printf("Detailed Data: %v\n", detailWriter.Path)
	fmt.Printf("Regression Data: %v\n", regressionWriter.Path)
	fmt.Printf("System Info Data: %v\n", sysinfoWriter.Path)
	fmt.Printf("----- Writing Reports to Disk -----\n")
}

func Writef(level int, format string, args ...interface{}) {
	format = fmt.Sprintf("[%v] ", time.Now().Format(time.StampMilli)) + format

	if level <= SummaryLevel {
		fsummary.WriteString(fmt.Sprintf(format, args...))
	}
	if level <= DetailLevel {
		fdetail.WriteString(fmt.Sprintf(format, args...))
	}
	if level <= ConsoleLevel {
		fmt.Printf(format, args...)
	}
}

func Flush() {
	fsummary.Sync()
	fdetail.Sync()

	regressionWriter.Flush()
	summaryWriter.Flush()
	detailWriter.Flush()
	sysinfoWriter.Flush()
}

func Close() {
	fsummary.Close()
	fdetail.Close()

	regressionWriter.Close()
	summaryWriter.Close()
	detailWriter.Close()
	sysinfoWriter.Close()

	fmt.Printf("----- Reports Written to Disk -----\n")
	fmt.Printf("Summary Logs: %v\n", fsummaryPath)
	fmt.Printf("Detailed Logs: %v\n", fdetailPath)
	fmt.Println()
	fmt.Printf("Summary Data: %v\n", summaryWriter.Path)
	fmt.Printf("Detailed Data: %v\n", detailWriter.Path)
	fmt.Printf("Regression Data: %v\n", regressionWriter.Path)
	fmt.Printf("System Info Data: %v\n", sysinfoWriter.Path)
	fmt.Printf("----- Reports Written to Disk -----\n")
}
