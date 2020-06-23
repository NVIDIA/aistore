// Package ta2tf provides core functionality for integrating with TensorFlow tools
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	jsoniter "github.com/json-iterator/go"
)

type (
	tarSamplesReader struct {
		records       []*extract.Record
		recordManager *extract.RecordManager
		lom           *cluster.LOM
		read          *atomic.Int64
	}

	samplesStreamer struct {
		w       http.ResponseWriter
		flusher http.Flusher
		encoder *jsoniter.Encoder
		xact    *Xact
	}
)

func newTarSamplesReader(lom *cluster.LOM) (*tarSamplesReader, error) {
	reader := &tarSamplesReader{}
	reader.read = atomic.NewInt64(-1)
	reader.lom = lom
	var (
		err error
	)
	reader.recordManager, err = extractRecords(lom.T, lom)
	if err != nil {
		return nil, err
	}

	reader.records = reader.recordManager.Records.All()
	return reader, nil
}

func (r *tarSamplesReader) Read() (*core.Sample, error) {
	currentIdx := r.read.Inc()
	if currentIdx >= int64(len(r.records)) {
		return nil, io.EOF
	}

	sample := core.NewSample()
	for _, obj := range r.records[currentIdx].Objects {
		value, err := prepareRecordEntryValue(r.recordManager, obj)
		if err != nil {
			return nil, fmt.Errorf("[%s] %s", r.lom.ObjName, err.Error())
		}

		sample.Entries[strings.TrimPrefix(obj.Extension, ".")] = value
	}

	return sample, nil
}

func (r *tarSamplesReader) Shuffle() {
	for i := len(r.records) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		r.records[i], r.records[j] = r.records[j], r.records[i]
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
	defer func() {
		debug.AssertNoErr(f.Close())
	}()
	reader := io.NewSectionReader(f, 0, lom.Size())
	_, _, err = extractCreator.ExtractShard(lom, reader, recordManager, false)

	return recordManager, err
}

func prepareRecordEntryValue(recordManager *extract.RecordManager, obj *extract.RecordObj) ([]byte, error) {
	contentPath := recordManager.FullContentPath(obj)
	v, ok := recordManager.RecordContents().Load(contentPath) // *SGL
	if !ok {
		return nil, fmt.Errorf("unexpected content path %q not found (extension %q)", obj.ContentPath, obj.Extension)
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
		return nil, fmt.Errorf("expected to read %d bytes, but read %d instead", sgl.Size()-obj.MetadataSize, readBytes)
	}

	return value, nil
}

func newSamplesStreamer(xact *Xact) *samplesStreamer {
	var ok bool
	streamer := &samplesStreamer{}
	streamer.xact = xact
	streamer.w = xact.Job.Writer
	streamer.flusher, ok = streamer.w.(http.Flusher)
	if !ok || streamer.flusher == nil {
		http.NotFound(streamer.w, xact.Job.Request)
		return nil
	}

	streamer.w.Header().Set("Transfer-Encoding", "chunked")
	streamer.w.WriteHeader(http.StatusOK)

	streamer.encoder = jsoniter.NewEncoder(streamer.w)
	streamer.flusher.Flush()
	return streamer
}

func (s *samplesStreamer) Stream(reader core.SampleReader) error {
	var (
		err    error
		sample *core.Sample
	)
	for sample, err = reader.Read(); err == nil; sample, err = reader.Read() {
		select {
		case <-s.xact.ChanAbort():
			return fmt.Errorf("task %s aborted", s.xact.ID())
		default:
			break
		}

		if s.xact.Job.Request.Context().Err() != nil {
			return fmt.Errorf("connection with client canceled")
		}
		err := s.encoder.Encode([]string{s.xact.Job.Selections[0].SelectValue(sample), s.xact.Job.Selections[1].SelectValue(sample)})
		if err != nil {
			glog.Errorf("error encoding selections: %v", err)
			return err
		}
		s.flusher.Flush()
	}
	if err != io.EOF {
		return err
	}
	return nil
}
