/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/iosgl"
)

type tarExtractCreater struct {
	targetURL      string
	gzipped        bool
	h              hash.Hash
	client         *http.Client
	recordContents *sync.Map
	key            func(base string, h hash.Hash) string
	extractSema    chan struct{} // Counting semaphore to limit concurrent calls to ExtractShard
	createSema     chan struct{} // Counting semaphore to limit concurrent calls to CreateShard
}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *tarExtractCreater) ExtractShard(f *os.File, toDisk bool, extractionPath string) (records []Record, extracted int64, err error) {
	var (
		pathToContents string
		tr             *tar.Reader
		header         *tar.Header
	)
	extractionPath += f.Name()
	t.acquireExtractSema()
	defer t.releaseExtractSema()
	records = make([]Record, 0)
	if t.gzipped {
		gzr, err := gzip.NewReader(f)
		if err != nil {
			return records, 0, err
		}
		defer gzr.Close()
		tr = tar.NewReader(gzr)
	} else {
		tr = tar.NewReader(f)
	}

	for {
		header, err = tr.Next()
		if err == io.EOF {
			return records, extracted, nil
		} else if err != nil {
			return records, extracted, err
		}

		if header.Typeflag == tar.TypeDir {
			if !toDisk {
				continue
			}
			if err = os.MkdirAll(filepath.Join(extractionPath, header.Name), 0755); err != nil {
				return records, extracted, err
			}
		} else if header.Typeflag == tar.TypeReg {
			pathToContents = filepath.Join(extractionPath, header.Name)
			if toDisk {
				if err = os.MkdirAll(filepath.Dir(pathToContents), 0755); err != nil {
					return records, extracted, err
				}
				newF, err := os.OpenFile(pathToContents, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
				if err != nil {
					return records, extracted, err
				}
				if _, err = io.Copy(newF, tr); err != nil {
					newF.Close()
					return records, extracted, err
				}
				newF.Close()
			} else {
				sgl := iosgl.NewSGL(uint64(header.Size))
				if _, err = io.Copy(sgl, tr); err != nil {
					return records, extracted, err
				}
				t.recordContents.Store(pathToContents, sgl)
			}
		}

		records = append(records, Record{
			Key:            t.key(header.Name, t.h),
			Name:           header.Name,
			Size:           header.Size,
			TargetURL:      t.targetURL,
			PathToContents: pathToContents,
			Header: Header{
				Typeflag: header.Typeflag,
				Linkname: header.Linkname,
				Mode:     header.Mode,
				Uid:      header.Uid,
				Gid:      header.Gid,
				Uname:    header.Uname,
				Gname:    header.Gname,
			},
		})
		extracted += header.Size
	}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *tarExtractCreater) CreateShard(s Shard, shardFQN string) (written int64, err error) {
	var (
		tarball *os.File
		gzw     *gzip.Writer
		tw      *tar.Writer
	)
	t.acquireCreateSema()
	defer t.releaseCreateSema()
	if err = os.MkdirAll(filepath.Dir(shardFQN), 0755); err != nil {
		return 0, err
	}
	if tarball, err = os.OpenFile(shardFQN, os.O_CREATE|os.O_RDWR, 0664); err != nil {
		return 0, fmt.Errorf("failed to create new tarball, err: %v", err)
	}
	defer tarball.Close()
	if t.gzipped {
		gzw = gzip.NewWriter(tarball)
		tw = tar.NewWriter(gzw)
		defer gzw.Close()
	} else {
		tw = tar.NewWriter(tarball)
	}
	defer tw.Close()

	for _, rec := range s.Records {
		if err = tw.WriteHeader(&tar.Header{
			Size:     rec.Size,
			Name:     rec.Name,
			Typeflag: rec.Typeflag,
			Linkname: rec.Linkname,
			Mode:     rec.Mode,
			Uid:      rec.Uid,
			Gid:      rec.Gid,
			Uname:    rec.Uname,
			Gname:    rec.Gname,
		}); err != nil {
			return written, err
		}

		if rec.TargetURL != t.targetURL { // File source contents are located on a different target.
			p := url.QueryEscape(rec.PathToContents)
			u := rec.TargetURL + api.URLPath(api.Version, api.RecordContents)
			resp, err := t.client.Get(fmt.Sprintf("%s?%s=%s", u, api.URLParamPathToContents, p)) // TODO:
			if err != nil {
				return written, fmt.Errorf(
					"error getting shard source from target URL: %s, err: %v", rec.TargetURL, err)
			} else if resp.StatusCode >= http.StatusBadRequest {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					resp.Body.Close()
					return written, fmt.Errorf("failed to read response, err: %v", err)
				}
				resp.Body.Close()
				return written, fmt.Errorf("status code: %d in response, body: %s", resp.StatusCode, string(b))
			}
			n, err := io.Copy(tw, resp.Body)
			if err != nil {
				resp.Body.Close()
				return written, fmt.Errorf("failed to copy over response to tar writer, err: %v", err)
			}
			written += n
			resp.Body.Close()
		} else {
			var n int64
			v, ok := t.recordContents.Load(rec.PathToContents)
			if !ok {
				f, err := os.Open(rec.PathToContents)
				if err != nil {
					return written, err
				}
				n, err = io.Copy(tw, f)
				if err != nil {
					f.Close()
					return written, err
				}
				f.Close()
			} else {
				sgl := v.(*iosgl.SGL)
				n, err = io.Copy(tw, sgl)
				if err != nil {
					sgl.Free()
					return written, err
				}
				sgl.Free()
			}
			written += n
		}
	}
	return written, nil
}

func (t *tarExtractCreater) UsingCompression() bool {
	return t.gzipped
}

func (t *tarExtractCreater) RecordContents() *sync.Map {
	return t.recordContents
}

func (t *tarExtractCreater) acquireExtractSema() {
	if t.extractSema != nil {
		t.extractSema <- struct{}{}
	}
}

func (t *tarExtractCreater) releaseExtractSema() {
	if t.extractSema != nil {
		<-t.extractSema
	}
}

func (t *tarExtractCreater) acquireCreateSema() {
	if t.createSema != nil {
		t.createSema <- struct{}{}
	}
}

func (t *tarExtractCreater) releaseCreateSema() {
	if t.createSema != nil {
		<-t.createSema
	}
}
