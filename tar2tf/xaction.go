// Package ta2tf provides core functionality for integrating with TensorFlow tools
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */

package tar2tf

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

type (
	Xact struct {
		cmn.XactBase
		Job *SamplesStreamJob
		T   cluster.Target
		sync.RWMutex
	}
)

func (t *Xact) IsMountpathXact() bool { return false }
func (t *Xact) Run() {
	defer func() {
		t.EndTime(time.Now())
		t.Job.Wg.Done()
	}()

	var w = t.Job.Writer
	flusher, ok := w.(http.Flusher)
	if !ok || flusher == nil {
		http.NotFound(w, t.Job.Request)
		return
	}

	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	encoder := jsoniter.NewEncoder(w)
	flusher.Flush()

	err := cluster.HrwIterMatchingObjects(t.T, cluster.NewBckEmbed(t.Bck()), t.Job.Template, func(lom *cluster.LOM) error {
		var (
			err           error
			recordManager *extract.RecordManager
		)
		recordManager, err = extractRecords(t.T, lom)
		if err != nil {
			return err
		}

		allRecords := recordManager.Records.All()
		if t.Job.ShuffleTar {
			for i := len(allRecords) - 1; i > 0; i-- {
				j := rand.Intn(i + 1)
				allRecords[i], allRecords[j] = allRecords[j], allRecords[i]
			}
		}

		for _, r := range recordManager.Records.All() {
			tarRecord := NewTarRecord(len(r.Objects))
			for _, obj := range r.Objects {
				value, err := prepareRecordEntryValue(recordManager, obj)
				if err != nil {
					return fmt.Errorf("[%s] %s", lom.ObjName, err.Error())
				}

				tarRecord[strings.TrimPrefix(obj.Extension, ".")] = &TarRecordEntry{Value: value}
			}

			for _, c := range t.Job.Conversions {
				tarRecord = c.Do(tarRecord)
			}

			select {
			case <-t.ChanAbort():
				return fmt.Errorf("task %s aborted", t.ID())
			default:
				break
			}

			if t.Job.Request.Context().Err() != nil {
				return fmt.Errorf("connection with client canceled")
			}
			err := encoder.Encode([]string{t.Job.Selections[0].Select(tarRecord), t.Job.Selections[1].Select(tarRecord)})
			if err != nil {
				glog.Errorf("error encoding:" + err.Error())
				return err
			}
			flusher.Flush()
		}

		return nil
	})

	if err != nil {
		glog.Errorf("error %s", err.Error())
	}
}

func extractRecords(target cluster.Target, lom *cluster.LOM) (*extract.RecordManager, error) {
	var (
		extractCreator extract.ExtractCreator
		ext            = filepath.Ext(lom.ObjName)
		onDuplicates   = func(s string) error { glog.Errorf("[%s] duplicated record %s", lom.String(), s); return nil }
	)
	switch ext {
	case cmn.ExtTar:
		extractCreator = extract.NewTarExtractCreator(target)
	case cmn.ExtTarTgz, cmn.ExtTgz:
		extractCreator = extract.NewTargzExtractCreator(target)
	default:
		return nil, fmt.Errorf("%s extension not supported. Please provide one of: %s/%s/%s", ext, cmn.ExtTar, cmn.ExtTarTgz, cmn.ExtTgz)
	}

	keyExtractor, err := extract.NewNameKeyExtractor()
	cmn.AssertNoErr(err) // err always nil
	recordManager := extract.NewRecordManager(target, target.Snode().DaemonID, lom.Bck().Name, lom.Bck().Provider, ext, extractCreator, keyExtractor, onDuplicates)

	f, err := os.Open(lom.FQN)
	if err != nil {
		return nil, err
	}
	reader := io.NewSectionReader(f, 0, lom.Size())
	_, _, err = extractCreator.ExtractShard(lom, reader, recordManager, false)

	return recordManager, err
}

func prepareRecordEntryValue(recordManager *extract.RecordManager, obj *extract.RecordObj) ([]byte, error) {
	contentPath := recordManager.FullContentPath(obj)
	v, ok := recordManager.RecordContents().Load(contentPath) // *SGL
	if !ok {
		return nil, fmt.Errorf("unexpected content path %q not found (extension %q)", obj.Extension, obj.ContentPath)
	}

	sgl := v.(*memsys.SGL)
	reader := memsys.NewReader(sgl)
	_, err := reader.Seek(obj.MetadataSize, io.SeekStart)
	if err != nil {
		return nil, err
	}
	value := make([]byte, sgl.Size()-obj.MetadataSize)
	readBytes, err := reader.Read(value)
	if err != nil {
		return nil, err
	}
	if int64(readBytes) != sgl.Size()-obj.MetadataSize || readBytes == 0 {
		return nil, fmt.Errorf("expected to read %d bytes, but read %d instead", sgl.Size(), readBytes)
	}

	return value, nil
}
