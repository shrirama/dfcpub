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

// listObjectsDetails returns details of all objects that matches the prefix in a bucket
func (p *proxyServer) listObjectsDetails(bucket string, prefix string, limit int) ([]*dfc.BucketEntry, error) {
	msg := &dfc.GetMsg{
		GetPrefix: prefix,
		GetProps:  "size, ctime",
	}

	bl, err := client.ListBucket(p.url, bucket, msg, limit)
	if err != nil {
		return nil, err
	}

	return bl.Entries, err
}

// listObjectsNames returns names of all objects that matches the prefix in a bucket
func (p *proxyServer) listObjectsNames(bucket string, prefix string) ([]string, error) {
	return client.ListObjects(p.url, bucket, prefix, 0)
}

// deleteObjects deletes all objects in the list of names from a bucket
func (p *proxyServer) deleteObjects(bucket string, names []string) error {
	return client.DeleteList(p.url, bucket, names, true /* wait */, 0 /* deadline*/)
}
