// Helper functions for interfacing with DFC proxy
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package main

import (
	"io"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

type proxyServer struct {
	url string
}

// createBucket creates a new bucket
func (p *proxyServer) createBucket(bucket string) error {
	return client.CreateLocalBucket(p.url, bucket)
}

func (p *proxyServer) deleteBucket(bucket string) error {
	return client.DestroyLocalBucket(p.url, bucket)
}

func (p *proxyServer) doesBucketExist(bucket string) bool {
	_, err := client.HeadBucket(p.url, bucket)
	return err == nil
}

// doesObjectExists queries proxy to get a list of objects starting with a prefix.
// if any objects are returned, it implies the prefix exists.
func (p *proxyServer) doesObjectExist(bucket, prefix string) (bool, error) {
	objs, err := client.ListObjects(p.url, bucket, prefix, 1)
	if err != nil {
		return false, err
	}

	return len(objs) == 1, nil
}

// putObject creates a new file reader and uses it to make a proxy put call to save a new
// object with xxHash enabled into a bucket.
func (p *proxyServer) putObject(localPath string, bucket string, prefix string) error {
	r, err := readers.NewFileReaderFromFile(localPath, true /* xxhash */)
	if err != nil {
		return err
	}

	return client.Put(p.url, r, bucket, prefix, true /* silent */)
}

// getObject asks proxy to return an object and saves it into the io.Writer (for example, a local file).
func (p *proxyServer) getObject(bucket string, prefix string, w io.Writer) error {
	_, _, err := client.GetFile(p.url, bucket, prefix, nil /* wg */, nil, /* errch */
		true /* silent */, true /* validate */, w)
	return err
}

func (p *proxyServer) deleteObject(bucket string, prefix string) error {
	return client.Del(p.url, bucket, prefix, nil /* wg */, nil /* errch */, true /* silent */)
}

// listBuckets returns a slice of names of all buckets
func (p *proxyServer) listBuckets(local bool) ([]string, error) {
	if !local {
		return nil, nil
	}

	bns, err := client.ListBuckets(p.url, local)
	if err != nil {
		return nil, err
	}

	var buckets []string
	for _, b := range bns.Local {
		buckets = append(buckets, b)
	}

	return buckets, nil
}

// listObjectsDetails returns details of all objects that matches the prefix in a bucket
func (p *proxyServer) listObjectsDetails(bucket string, prefix string) ([]*dfc.BucketEntry, error) {
	msg := &dfc.GetMsg{
		GetPrefix: prefix,
		GetProps:  "size, ctime",
	}

	bl, err := client.ListBucket(p.url, bucket, msg, 0)
	if err != nil {
		return nil, err
	}

	return bl.Entries, err
}

// listObjectsNames returns names of all objects that matches the prefix in a bucket
func (p *proxyServer) listObjectsNames(bucket string, prefix string) ([]string, error) {
	return client.ListObjects(p.url, bucket, prefix, 0)
}

// getObjectInfo expects to find and return none or one object that matches the prefix in a bucket.
func (p *proxyServer) getObjectInfo(bucket string, prefix string) (bool, *dfc.BucketEntry, error) {
	msg := &dfc.GetMsg{
		GetPrefix: prefix,
		GetProps:  "size, ctime",
	}

	bl, err := client.ListBucket(p.url, bucket, msg, 0)
	if err != nil {
		return false, nil, err
	}

	if len(bl.Entries) == 0 {
		return false, nil, nil
	}

	for i := 0; i < len(bl.Entries); i++ {
		if bl.Entries[i].Name == prefix {
			return true, bl.Entries[i], nil
		}
	}

	return false, nil, nil
}

// deleteObjects deletes all objects in the list of names from a bucket
func (p *proxyServer) deleteObjects(bucket string, names []string) error {
	return client.DeleteList(p.url, bucket, names, true /* wait */, 0 /* deadline*/)
}
