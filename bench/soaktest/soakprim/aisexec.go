package soakprim

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/constants"
	"github.com/NVIDIA/aistore/bench/soaktest/report"

	"github.com/NVIDIA/aistore/bench/soaktest/stats"
	"github.com/NVIDIA/aistore/tutils"
	jsoniter "github.com/json-iterator/go"

	"github.com/NVIDIA/aistore/cmn"
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
		verifyhash   bool
		minsize      int
		maxsize      int
		readoff      int
		readlen      int
		stopable     bool
		stopCh       chan struct{}
	}
)

const (
	soaktestDirname = "/tmp/ais-soak/"
	aisloaderFolder = soaktestDirname + "/aisloaderexec"
)

var (
	aisloaderSrc    = path.Join(os.Getenv("GOPATH"), "src/github.com/NVIDIA/aistore/bench/aisloader")
	aisloaderTarget = path.Join(aisloaderFolder, "aisloader")
)

func init() {
	os.RemoveAll(aisloaderFolder)
	os.MkdirAll(soaktestDirname, 0755)
	os.MkdirAll(aisloaderFolder, 0755)
	cmd := exec.Command("go", "build", "-o", aisloaderTarget)
	cmd.Dir = aisloaderSrc
	err := cmd.Run()
	cmn.AssertNoErr(err)
}

func AISExec(ch chan *stats.PrimitiveStat, bucket string, numWorkers int, params *AISLoaderExecParams) {
	randomSrc := rand.New(rand.NewSource(time.Now().UnixNano()))
	filebasename := tutils.FastRandomFilename(randomSrc, 13)
	filename := path.Join(soaktestDirname, filebasename+".json")
	defer os.Remove(filename)

	spf := fmt.Sprintf

	// Using the default readertype sgl for now. If later we decided to use file, need to also set aisloader tmpdir to be a unique folder per call.

	cmd := exec.Command(aisloaderTarget, spf("-ip=%s", primaryIP), spf("-port=%s", primaryPort),
		spf("-bucket=%s", bucket),
		spf("-numworkers=%v", numWorkers),
		spf("-pctput=%v", params.pctput),
		spf("-duration=%s", params.duration),
		spf("-totalputsize=%v", params.totalputsize/cmn.KiB),
		spf("-verifyhash=%t", params.verifyhash),
		spf("-minsize=%v", params.minsize/cmn.KiB),
		spf("-maxsize=%v", params.maxsize/cmn.KiB),
		spf("-readoff=%v", params.readoff),
		spf("-readlen=%v", params.readlen),
		spf("-stats-output=%s", filename),
		spf("-stopable=%t", params.stopable),
		"-statsinterval=0", "-cleanup=false", "-json=true")
	cmd.Dir = aisloaderSrc

	var out []byte
	var err error

	if params.stopable {
		cmd.Start()
		<-params.stopCh
		err = cmd.Process.Signal(syscall.SIGHUP)
		if err != nil {
			cmd.Process.Kill()
		} else {
			err = cmd.Wait()
		}
		out = []byte("<Regression Run>")
	} else {
		out, err = cmd.Output()
	}

	if err != nil {
		report.Writef(report.SummaryLevel, "ais error: %v\n", err.Error())
		report.Writef(report.DetailLevel, "-----aisloader output-----\n%v\n-----end aisloader output-----\n\n", string(out))
		ch <- &stats.PrimitiveStat{Fatal: true, AISLoaderStat: stats.AISLoaderStat{StartTime: time.Now()}}
		if strings.Contains(err.Error(), "signal: interrupt") {
			Terminated = true
		}
		return
	}

	result, err := ioutil.ReadFile(filename)
	if err != nil {
		report.Writef(report.SummaryLevel, "error reading ais results from %v\n", filename)
		ch <- &stats.PrimitiveStat{Fatal: true, AISLoaderStat: stats.AISLoaderStat{StartTime: time.Now()}}
		return
	}

	aisloaderStats, err := parseAisloaderResponse(result, params.pctput)

	if err != nil {
		report.Writef(report.SummaryLevel, "error parsing aisloader response")
		ch <- &stats.PrimitiveStat{Fatal: true}
		return
	}

	ch <- aisloaderStats
}

func parseAisloaderResponse(response []byte, totalputsize int) (*stats.PrimitiveStat, error) {
	aisloaderresp := make([]aisloaderResponse, 0)
	err := jsoniter.Unmarshal(response, &aisloaderresp)

	if err != nil {
		return nil, err
	}

	if len(aisloaderresp) <= 0 {
		return nil, errors.New("aisloader returned empty response, expected at least summary")
	}

	// TODO: figure out better way of distingushing primitive type
	// Maybe just pass from primitive
	if totalputsize < 50 {
		primitiveStat := stats.PrimitiveStat{
			AISLoaderStat: aisloaderresp[len(aisloaderresp)-1].Get,
			OpType:        constants.OpTypeGet,
		}
		return &primitiveStat, nil
	}

	// TODO: the last element of aisloader response is always a summary
	// we might make use of the rest as well at some point
	primitiveStat := stats.PrimitiveStat{
		AISLoaderStat: aisloaderresp[len(aisloaderresp)-1].Put,
		OpType:        constants.OpTypePut,
	}

	return &primitiveStat, nil
}
