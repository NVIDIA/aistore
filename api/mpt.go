// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Default values for multipart download
const (
	DefaultMptDownloadWorkers   = 16
	DefaultMptDownloadChunkSize = 8 * cos.MiB
)

type (
	PutPartArgs struct {
		UploadID   string // QparamMptUploadID
		PutArgs           // regular PUT args
		PartNumber int    // QparamMptPartNo
	}

	// MultipartDownloadArgs configures concurrent range-based download
	MultipartDownloadArgs struct {
		// Writer to write the downloaded content (required)
		Writer io.WriterAt
		// Number of concurrent download workers (default: 16)
		NumWorkers int
		// Size of each chunk/range to download (default: 8 MiB)
		ChunkSize int64
		// ObjectSize can be set to skip the HEAD request (optional, 0 means auto-detect)
		ObjectSize int64
	}

	// Internal: represents a chunk to download
	mptDownloadChunk struct {
		index  int   // chunk index for ordering
		offset int64 // start offset in the object
		length int64 // length of this chunk
	}
)

// CreateMultipartUpload creates a new multipart upload.
func CreateMultipartUpload(bp BaseParams, bck cmn.Bck, objName string) (uploadID string, err error) {
	q := qalloc()
	q = bck.AddToQuery(q)
	bp.Method = http.MethodPost
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMptUpload})
		reqParams.Query = q
	}
	_, err = reqParams.doReqStr(&uploadID)

	FreeRp(reqParams)
	qfree(q)
	return uploadID, err
}

// UploadPart uploads a part of a multipart upload.
// - uploadID: the ID of the multipart upload to upload the part to
// - partNumber: the part number to upload
func UploadPart(args *PutPartArgs) error {
	q := qalloc()
	q.Set(apc.QparamMptUploadID, args.UploadID)
	q.Set(apc.QparamMptPartNo, strconv.Itoa(args.PartNumber))
	q = args.Bck.AddToQuery(q)

	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodPut
		reqArgs.Base = args.BaseParams.URL
		reqArgs.Path = apc.URLPathObjects.Join(args.Bck.Name, args.ObjName)
		reqArgs.Query = q
		reqArgs.BodyR = args.Reader
		reqArgs.Header = args.Header
	}
	_, err := DoWithRetry(args.BaseParams.Client, args.put, reqArgs) //nolint:bodyclose // is closed inside
	cmn.FreeHra(reqArgs)
	qfree(q)
	return err
}

// CompleteMultipartUpload completes a multipart upload.
// - uploadID: the ID of the multipart upload to complete
// - partNumbers: the part numbers to complete
func CompleteMultipartUpload(bp BaseParams, bck cmn.Bck, objName, uploadID string, partNumbers []int) error {
	q := qalloc()
	q.Set(apc.QparamMptUploadID, uploadID)
	q = bck.AddToQuery(q)
	bp.Method = http.MethodPost

	completeMptUpload := make([]apc.MptCompletedPart, len(partNumbers))
	for i, partNumber := range partNumbers {
		completeMptUpload[i].PartNumber = partNumber
	}

	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMptComplete, Value: completeMptUpload})
		reqParams.Query = q
	}

	err := reqParams.DoRequest()
	FreeRp(reqParams)
	qfree(q)

	return err
}

// AbortMultipartUpload aborts a multipart upload.
// - uploadID: the ID of the multipart upload to abort
func AbortMultipartUpload(bp BaseParams, bck cmn.Bck, objName, uploadID string) error {
	q := qalloc()
	q.Set(apc.QparamMptUploadID, uploadID)
	q = bck.AddToQuery(q)
	bp.Method = http.MethodDelete

	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathObjects.Join(bck.Name, objName)
		reqParams.Body = cos.MustMarshal(apc.ActMsg{Action: apc.ActMptAbort})
		reqParams.Query = q
	}

	err := reqParams.DoRequest()
	FreeRp(reqParams)
	qfree(q)

	return err
}

// MultipartDownload performs concurrent range-based download of an object.
// It spawns multiple goroutines to download different byte ranges in parallel.
//
// The function:
// 1. Issues a HEAD request to get object size
// 2. Divides the object into chunks based on ChunkSize
// 3. Spawns NumWorkers goroutines to download chunks concurrently
// 4. Each worker issues a GET request with Range header
// 5. Results are written to the provided WriterAt at the correct offset
//
// Returns error if any chunk download fails.
func MultipartDownload(bp BaseParams, bck cmn.Bck, objName string, args *MultipartDownloadArgs) error {
	if args == nil || args.Writer == nil {
		return errors.New("MultipartDownload: Writer is required")
	}

	// Apply defaults
	numWorkers := args.NumWorkers
	if numWorkers <= 0 {
		numWorkers = DefaultMptDownloadWorkers
	}
	chunkSize := args.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultMptDownloadChunkSize
	}

	objectSize := args.ObjectSize
	if objectSize <= 0 {
		objProps, err := HeadObject(bp, bck, objName, HeadArgs{})
		if err != nil {
			return fmt.Errorf("failed to get object properties: %w", err)
		}
		objectSize = objProps.Size
	}
	if objectSize <= 0 {
		return fmt.Errorf("invalid object size: %d", objectSize)
	}

	var (
		numChunks = (objectSize + chunkSize - 1) / chunkSize // ceiling division
		chunkCh   = make(chan mptDownloadChunk, numChunks)
		errCh     = make(chan error, numChunks)
		errs      = make([]error, 0, numChunks)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range numChunks {
		offset := i * chunkSize
		length := chunkSize
		if offset+length > objectSize {
			length = objectSize - offset // last chunk may be smaller
		}
		chunkCh <- mptDownloadChunk{
			index:  int(i),
			offset: offset,
			length: length,
		}
	}
	close(chunkCh)

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			mptDownloadWorker(ctx, cancel, bp, bck, objName, args.Writer, chunkCh, errCh)
		})
	}

	// Wait for all workers to complete
	wg.Wait()
	close(errCh)

	for err := range errCh {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// mptDownloadWorker is a worker goroutine that downloads chunks from the work channel
func mptDownloadWorker(ctx context.Context, cancel context.CancelFunc, bp BaseParams, bck cmn.Bck, objName string, writer io.WriterAt, chunkCh <-chan mptDownloadChunk, errCh chan<- error) {
	for chunk := range chunkCh {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := mptDownloadChunkRange(bp, bck, objName, writer, chunk); err != nil {
			cancel() // signal others to stop
			errCh <- err
			return
		}
	}
}

// mptDownloadChunkRange downloads a single chunk using HTTP Range request
func mptDownloadChunkRange(bp BaseParams, bck cmn.Bck, objName string, writer io.WriterAt, chunk mptDownloadChunk) error {
	reader, _, err := GetObjectReader(bp, bck, objName, &GetArgs{
		Header: http.Header{cos.HdrRange: []string{cmn.MakeRangeHdr(chunk.offset, chunk.length)}},
	})
	if err != nil {
		return fmt.Errorf("chunk %d: failed to get reader: %w", chunk.index, err)
	}
	defer reader.Close()

	// Use SectionWriter to write at correct offset with bounded buffer
	sw := cos.NewSectionWriter(writer, chunk.offset)
	n, err := io.Copy(sw, reader)
	if err != nil {
		return fmt.Errorf("chunk %d: failed to copy: %w", chunk.index, err)
	}
	if n != chunk.length {
		return fmt.Errorf("chunk %d: failed to copy: expected %d bytes, got %d", chunk.index, chunk.length, n)
	}

	return nil
}
