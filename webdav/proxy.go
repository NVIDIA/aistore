// Helper functions for interfacing with DFC proxy
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package main

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

type proxyServer struct {
	url string
}

// createBucket creates a new bucket
func (p *proxyServer) createBucket(bucket string) error {
	baseParams := tutils.BaseAPIParams(p.url)
	return api.CreateLocalBucket(baseParams, bucket)
}

func (p *proxyServer) deleteBucket(bucket string) error {
	baseParams := tutils.BaseAPIParams(p.url)
	return api.DestroyLocalBucket(baseParams, bucket)
}

func (p *proxyServer) doesBucketExist(bucket string) bool {
	// note: webdav works with local bucket only (at least for now)
	// _, err := api.HeadBucket(p.url, bucket)
	// return err == nil
	baseParams := tutils.BaseAPIParams(p.url)
	bns, err := api.GetBucketNames(baseParams, true /* local */)
	if err != nil {
		return false
	}

	for _, b := range bns.Local {
		if b == bucket {
			return true
		}
	}

	return false
}

// listBuckets returns a slice of names of all buckets
func (p *proxyServer) listBuckets(local bool) ([]string, error) {
	if !local {
		return nil, nil
	}
	baseParams := tutils.BaseAPIParams(p.url)
	bns, err := api.GetBucketNames(baseParams, local)
	if err != nil {
		return nil, err
	}

	return bns.Local, nil
}

// doesObjectExists checks whether a resource exists by querying DFC.
func (p *proxyServer) doesObjectExist(bucket, prefix string) (bool, *fileInfo, error) {
	entries, err := p.listObjectsDetails(bucket, prefix, 1)
	if err != nil {
		return false, nil, err
	}

	if len(entries) == 0 {
		return false, nil, nil
	}

	if entries[0].Name == prefix {
		t, _ := time.Parse(time.RFC822, entries[0].Ctime)
		return true, &fileInfo{size: entries[0].Size, modTime: t.UTC()}, nil
	}

	if strings.HasPrefix(entries[0].Name, prefix+"/") {
		return true, &fileInfo{mode: os.ModeDir}, nil
	}

	return false, nil, nil
}

// putObject creates a new file reader and uses it to make a proxy put call to save a new
// object with xxHash enabled into a bucket.
func (p *proxyServer) putObject(localPath string, bucket string, prefix string) error {
	r, err := tutils.NewFileReaderFromFile(localPath, true /* xxhash */)
	if err != nil {
		return err
	}
	baseParams := tutils.BaseAPIParams(p.url)
	return api.PutObject(baseParams, bucket, prefix, r.XXHash(), r)
}

// getObject asks proxy to return an object and saves it into the io.Writer (for example, a local file).
func (p *proxyServer) getObject(bucket string, prefix string, w io.Writer) error {
	baseParams := tutils.BaseAPIParams(p.url)
	options := api.GetObjectInput{Writer: w}
	_, err := api.GetObjectWithValidation(baseParams, bucket, prefix, options)
	return err
}

func (p *proxyServer) deleteObject(bucket string, prefix string) error {
	return tutils.Del(p.url, bucket, prefix, nil /* wg */, nil /* errCh */, true /* silent */)
}

// listObjectsDetails returns details of all objects that matches the prefix in a bucket
func (p *proxyServer) listObjectsDetails(bucket string, prefix string, limit int) ([]*cmn.BucketEntry, error) {
	msg := &cmn.GetMsg{
		GetPrefix: prefix,
		GetProps:  "size, ctime",
	}
	baseParams := tutils.BaseAPIParams(p.url)
	bl, err := api.ListBucket(baseParams, bucket, msg, limit)
	if err != nil {
		return nil, err
	}

	return bl.Entries, err
}

// listObjectsNames returns names of all objects that matches the prefix in a bucket
func (p *proxyServer) listObjectsNames(bucket string, prefix string) ([]string, error) {
	return tutils.ListObjects(p.url, bucket, prefix, 0)
}

// deleteObjects deletes all objects in the list of names from a bucket
func (p *proxyServer) deleteObjects(bucket string, names []string) error {
	return tutils.DeleteList(p.url, bucket, names, true /* wait */, 0 /* deadline*/)
}
