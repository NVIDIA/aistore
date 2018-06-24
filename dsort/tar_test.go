/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"archive/tar"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/iosgl"
)

func TestCreateThenExtractShard(t *testing.T) {
	var (
		localFileName       = "localInputShard"
		localPathToContents = "/some/dir/localInputShard"
		localBytes          = []byte("these contents belong to a file located on this current target")
		localSize           = int64(len(localBytes))
		localContents       = iosgl.NewSGL(uint64(localSize))

		remoteFileName       = "remoteInputShard"
		remotePathToContents = "some/remote/dir/remoteInputShard"
		remoteBytes          = []byte("these contents belong to a file located on a different target")
		remoteSize           = int64(len(remoteBytes))

		shardName = "new-shard.tar"
		shardFQN  = "/tmp/" + shardName

		allContents  = [][]byte{localBytes, remoteBytes}
		curTargetURL = "foo"

		extractionPath = "/tmp"
	)

	_, err := io.Copy(localContents, bytes.NewBuffer(localBytes))
	if err != nil {
		t.Fatalf("error writing to localContents iosgl, err: %v", err)
	}
	otherTargetMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.URLPath(api.Version, api.RecordContents) && r.Method == http.MethodGet &&
			r.URL.Query().Get(api.URLParamPathToContents) == remotePathToContents {
			w.Write(remoteBytes)
		} else {
			http.Error(w, "bad request to otherTargetMock", http.StatusBadRequest)
		}
	}))
	localRecord := Record{
		Size:           localSize,
		Name:           localFileName,
		TargetURL:      curTargetURL,
		PathToContents: localPathToContents,
		Header: Header{
			Typeflag: tar.TypeReg,
			Mode:     0755,
		},
	}
	remoteRecord := Record{
		Size:           remoteSize,
		Name:           remoteFileName,
		TargetURL:      otherTargetMock.URL,
		PathToContents: remotePathToContents,
		Header: Header{
			Typeflag: tar.TypeReg,
			Mode:     0755,
		},
	}
	shard := Shard{
		Name:    shardName,
		Size:    localRecord.Size + remoteRecord.Size,
		Records: []Record{localRecord, remoteRecord},
	}

	tec := &tarExtractCreater{
		recordContents: &sync.Map{},
		client:         http.DefaultClient,
		key:            keyIdentity,
		targetURL:      curTargetURL,
	}
	tec.recordContents.Store(localRecord.PathToContents, localContents)

	// First, create.
	written, err := tec.CreateShard(shard, shardFQN)
	if err != nil {
		t.Fatalf("CreateShard failed, err: %v", err)
	}
	if written != localSize+remoteSize {
		t.Errorf("expected written: %d, actual: %d", localSize+remoteSize, written)
	}
	shardFile, err := os.Open(shardFQN)
	defer shardFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(shardFQN)
	if shardFile.Name() != shardFQN {
		t.Errorf("expected output shard name: %s, actual: %s", shardFQN, shardFile.Name())
	}
	if _, err = shardFile.Stat(); err != nil {
		t.Fatal(err)
	}

	// Then, extract (to memory).
	tec.recordContents = &sync.Map{}
	records, extracted, err := tec.ExtractShard(shardFile, false, extractionPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(shard.Records) {
		t.Errorf("expected records length: %d, actual: %d", len(shard.Records), len(records))
	}

	for i, rec := range records {
		if rec.TargetURL != tec.targetURL {
			t.Errorf("expected Records target URL: %s, actual: %s", tec.targetURL, rec.TargetURL)
		}
		if rec.Size != shard.Records[i].Size {
			t.Errorf("expected Records size: %d, actual: %d", shard.Records[i].Size, rec.Size)
		}
		if rec.Name != shard.Records[i].Name {
			t.Fatalf("expected Records name: %s, actual %s", shard.Records[i].Name, rec.Name)
		}
		v, ok := tec.recordContents.Load(rec.PathToContents)
		if !ok {
			t.Fatalf("could not Load Records name: %s from recordContents", rec.Name)
		}
		b, err := ioutil.ReadAll(v.(io.Reader))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != string(allContents[i]) {
			t.Errorf("expected content: %s\nactual content: %s", string(allContents[i]), string(b))
		}
	}

	if extracted != localSize+remoteSize {
		t.Errorf("expected extracted: %d, actual: %d", localSize+remoteSize, extracted)
	}

	// Then, extract again (but this time, to disk).
	tec.recordContents = &sync.Map{}
	if _, err = shardFile.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	records, extracted, err = tec.ExtractShard(shardFile, true, extractionPath)
	defer os.RemoveAll(extractionPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(shard.Records) {
		t.Errorf("expected records length: %d, actual: %d", len(shard.Records), len(records))
	}

	for i, rec := range records {
		if rec.TargetURL != tec.targetURL {
			t.Errorf("expected Records target URL: %s, actual: %s", tec.targetURL, rec.TargetURL)
		}
		if rec.Size != shard.Records[i].Size {
			t.Errorf("expected Records size: %d, actual: %d", shard.Records[i].Size, rec.Size)
		}
		if rec.Name != shard.Records[i].Name {
			t.Fatalf("expected Records name: %s, actual %s", shard.Records[i].Name, rec.Name)
		}
		if _, ok := tec.recordContents.Load(rec.PathToContents); ok {
			t.Fatalf("should not have been able to load Records name: %s"+
				" from recContents since extraction is to disk", rec.Name)
		}
		f, err := os.Open(rec.PathToContents)
		if err != nil {
			t.Fatal(err)
		}
		if f.Name() != rec.PathToContents {
			t.Errorf("expected file name: %s, actual: %s", rec.PathToContents, f.Name())
		}
		b, err := ioutil.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != string(allContents[i]) {
			t.Errorf("expected content: %s\nactual content: %s", string(allContents[i]), string(b))
		}
		f.Close()
	}

	if extracted != localSize+remoteSize {
		t.Errorf("expected extracted: %d, actual: %d", localSize+remoteSize, extracted)
	}
}
