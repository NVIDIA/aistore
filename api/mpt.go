// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
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
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
)

// Default values for multipart download
const (
	defaultMptDownloadWorkers   = 16
	defaultMptDownloadChunkSize = 8 * cos.MiB
	minMpdChunkSize             = 4 * cos.KiB
	maxMpdChunkSize             = 128 * cos.MiB
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
		// optional progress callback
		Callback  MpdCB
		CallAfter time.Duration
	}

	MpdCounter struct {
		callback  MpdCB
		startTime int64
		callAfter int64
		current   int64 // bytes downloaded (atomic)
		total     int64
		done      bool
	}
	MpdCB func(*MpdCounter)

	// Internal: represents a chunk to download
	mptDownloadChunk struct {
		index  int   // chunk index for ordering
		offset int64 // start offset in the object
		length int64 // length of this chunk
	}

	// Internal: worker context for multipart download
	mpdWorker struct {
		ctx     context.Context
		cancel  context.CancelFunc
		bp      BaseParams
		bck     cmn.Bck
		objName string
		writer  io.WriterAt
		chunkCh <-chan mptDownloadChunk
		errCh   chan<- error
		counter *MpdCounter
	}

	// MpdStreamArgs configures concurrent range-based download returning a reader.
	// BufferSize controls ring buffer memory: only BufferSize bytes are allocated, not the full object.
	MpdStreamArgs struct {
		NumWorkers int   // concurrent workers (default: 16)
		ChunkSize  int64 // per-chunk size (default: 8 MiB)
		ObjectSize int64 // optional, 0 = auto-detect via HEAD
		BufferSize int64 // ring buffer size (default: NumWorkers * ChunkSize)
	}

	// mpdReader is an io.ReadCloser backed by a fixed-size ring buffer.
	//
	// slot = chunkIndex % numSlots
	// Per-slot token channels (buffered 1) coordinate the handoff:
	//   slotFree[i]  — worker can write    slotReady[i] — reader can read
	//
	//  produce()        work() x N                mpdReader.Read() x 1
	//  -------------------------------------------------------------------
	//  chunk{0} ──┐     1. wait for slotFree[s]   1. wait for slotReady[s]
	//  chunk{1} ──┼──>  2. fetchChunk(s)          2. copy(p, buf[s])
	//  chunk{2} ──┘     3. signal slotReady[s]    3. signal slotFree[s]
	//     ...   chunkCh                 ring buf
	//                                  [s0|s1|..]
	mpdReader struct {
		bp      BaseParams
		bck     cmn.Bck
		objName string
		client  *http.Client // dedicated client for concurrent chunk downloads

		buf        []byte
		chunkSize  int64
		objectSize int64
		numSlots   int
		numChunks  int
		numWorkers int

		// Channels
		chunkCh   chan mptDownloadChunk
		slotFree  []chan struct{}
		slotReady []chan struct{}

		// Cancellation
		stop *cos.StopCh
		err  atomic.Pointer[error]

		// Reader state (single consumer — no lock needed)
		nextChunk int   // next chunk index to read
		readOff   int64 // bytes already read from current chunk
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
		numWorkers = defaultMptDownloadWorkers
	}

	var (
		chunkSize  = args.ChunkSize
		objectSize = args.ObjectSize
	)

	// Validate: either both must be provided (positive) or neither
	if chunkSize < 0 {
		return fmt.Errorf("invalid chunk size: %d", chunkSize)
	}
	if objectSize < 0 {
		return fmt.Errorf("invalid object size: %d", objectSize)
	}
	if chunkSize > 0 && objectSize == 0 {
		return errors.New("ChunkSize is set but ObjectSize is missing")
	}
	if objectSize > 0 && chunkSize == 0 {
		return errors.New("ObjectSize is set but ChunkSize is missing")
	}

	// Neither provided - do HEAD to get both
	if chunkSize == 0 {
		opV2, err := HeadObjectV2(bp, bck, objName, apc.GetPropsChunked, HeadArgs{})
		if err != nil {
			return fmt.Errorf("failed to get object properties: %w", err)
		}
		objectSize = opV2.Size
		if objectSize <= 0 {
			return fmt.Errorf("invalid object size: %d", objectSize)
		}
		if opV2.Chunks != nil && opV2.Chunks.MaxChunkSize > 0 {
			chunkSize = opV2.Chunks.MaxChunkSize
		} else {
			chunkSize = defaultMptDownloadChunkSize
		}
	}

	// Update args with resolved values
	args.NumWorkers = numWorkers
	args.ChunkSize = chunkSize
	args.ObjectSize = objectSize

	return multipartDownload(bp, bck, objName, args)
}

// multipartDownload performs the actual concurrent range-based download.
func multipartDownload(bp BaseParams, bck cmn.Bck, objName string, args *MultipartDownloadArgs) error {
	var (
		numChunks = (args.ObjectSize + args.ChunkSize - 1) / args.ChunkSize // ceiling division
		chunkCh   = make(chan mptDownloadChunk, min(numChunks, int64(args.NumWorkers*2)))
		errCh     = make(chan error, args.NumWorkers)
		errs      = make([]error, 0, args.NumWorkers)
		counter   *MpdCounter
	)
	if args.Callback != nil {
		counter = &MpdCounter{
			callback:  args.Callback,
			startTime: mono.NanoTime(),
			total:     args.ObjectSize,
		}
		counter.callAfter = counter.startTime + args.CallAfter.Nanoseconds()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &mpdWorker{
		ctx:     ctx,
		cancel:  cancel,
		bp:      bp,
		bck:     bck,
		objName: objName,
		writer:  args.Writer,
		chunkCh: chunkCh,
		errCh:   errCh,
		counter: counter,
	}

	// Start workers first so they can consume while we produce
	var wg sync.WaitGroup
	for range args.NumWorkers {
		wg.Go(w.run)
	}

	// Produce chunks - workers consume concurrently
	for i := range numChunks {
		offset := i * args.ChunkSize
		length := args.ChunkSize
		if offset+length > args.ObjectSize {
			length = args.ObjectSize - offset // last chunk may be smaller
		}
		chunkCh <- mptDownloadChunk{
			index:  int(i),
			offset: offset,
			length: length,
		}
	}
	close(chunkCh)

	// Wait for all workers to complete
	wg.Wait()
	close(errCh)

	for err := range errCh {
		errs = append(errs, err)
	}

	// final callback
	if counter != nil {
		counter.finish()
		counter.callback(counter)
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func (w *mpdWorker) run() {
	for chunk := range w.chunkCh {
		select {
		case <-w.ctx.Done():
			return
		default:
		}
		if err := mptDownloadChunkRange(w.bp, w.bck, w.objName, w.writer, chunk); err != nil {
			w.cancel() // signal others to stop
			w.errCh <- err
			return
		}
		// progress callback
		if w.counter != nil {
			atomic.AddInt64(&w.counter.current, chunk.length)
			if w.counter.mustCall() {
				w.counter.callback(w.counter)
			}
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

////////////////
// MpdCounter //
////////////////

func (c *MpdCounter) IsFinished() bool { return c.done }
func (c *MpdCounter) Current() int64   { return atomic.LoadInt64(&c.current) }
func (c *MpdCounter) Total() int64     { return c.total }

func (c *MpdCounter) mustCall() bool {
	// Only call if callAfter was explicitly set (callAfter > startTime) and time has elapsed
	return c.callAfter > c.startTime && mono.NanoTime() >= c.callAfter
}

func (c *MpdCounter) finish() { c.done = true }

////////////////////////////
// MultipartDownloadStream //
////////////////////////////

// MultipartDownloadStream performs concurrent range-based download and returns an io.ReadCloser
func MultipartDownloadStream(bp BaseParams, bck cmn.Bck, objName string, args *MpdStreamArgs) (r io.ReadCloser, oah ObjAttrs, err error) {
	if args == nil {
		args = &MpdStreamArgs{}
	}
	numWorkers := args.NumWorkers
	if numWorkers <= 0 {
		numWorkers = defaultMptDownloadWorkers
	}

	var (
		chunkSize  = args.ChunkSize
		objectSize = args.ObjectSize
	)
	// TODO: consider using server-suggested chunk size (opV2.Chunks.MaxChunkSize)
	if chunkSize <= 0 {
		chunkSize = defaultMptDownloadChunkSize
	}
	if objectSize <= 0 {
		opV2, err := HeadObjectV2(bp, bck, objName, apc.GetPropsSize+apc.LsPropsSepa+apc.GetPropsChecksum, HeadArgs{})
		if err != nil {
			return nil, oah, fmt.Errorf("head %s: %w", bck.Cname(objName), err)
		}
		objectSize = opV2.Size
		oah.wrespHeader = make(http.Header, 4)
		cmn.ToHeaderV2(&opV2.ObjAttrs, oah.wrespHeader, true /*cksum*/, false, false, false)
		if objectSize <= 0 {
			return nil, oah, fmt.Errorf("invalid object size: %d", objectSize)
		}
	}
	oah.n = objectSize

	if chunkSize < minMpdChunkSize {
		chunkSize = minMpdChunkSize
	}
	if chunkSize > maxMpdChunkSize {
		chunkSize = maxMpdChunkSize
	}
	// Single chunk — fall back to simple GET
	if chunkSize >= objectSize {
		reader, _, err := GetObjectReader(bp, bck, objName, nil)
		if err != nil {
			return nil, oah, err
		}
		return reader, oah, nil
	}
	numChunks := int((objectSize + chunkSize - 1) / chunkSize)
	if numWorkers > numChunks {
		numWorkers = numChunks
	}

	// Buffer size defaults and validation
	bufferSize := args.BufferSize
	if bufferSize <= 0 {
		bufferSize = int64(numWorkers) * chunkSize
	}
	if bufferSize < chunkSize {
		return nil, oah, fmt.Errorf("BufferSize (%d) must be >= ChunkSize (%d)", bufferSize, chunkSize)
	}
	numSlots := int(bufferSize / chunkSize) // round down
	if numWorkers > numSlots {
		numWorkers = numSlots
	}

	client := cmn.NewClient(cmn.TransportArgs{
		Timeout:          bp.Client.Timeout,
		IdleConnsPerHost: numWorkers,
		MaxIdleConns:     numWorkers,
	})
	bp.Client = client

	reader := &mpdReader{
		bp:         bp,
		bck:        bck,
		objName:    objName,
		client:     client,
		chunkSize:  chunkSize,
		objectSize: objectSize,
		numSlots:   numSlots,
		numChunks:  numChunks,
		numWorkers: numWorkers,

		// Ring buffer
		buf: make([]byte, int64(numSlots)*chunkSize),

		// Channels
		chunkCh:   make(chan mptDownloadChunk, min(numChunks, numWorkers*2)),
		slotFree:  make([]chan struct{}, numSlots),
		slotReady: make([]chan struct{}, numSlots),
		stop:      cos.NewStopCh(),
	}

	for i := range numSlots {
		reader.slotFree[i] = make(chan struct{}, 1)
		reader.slotFree[i] <- struct{}{} // all slots start free
		reader.slotReady[i] = make(chan struct{}, 1)
	}

	go reader.produce()

	return reader, oah, nil
}

///////////////
// mpdReader //
///////////////

func (r *mpdReader) Read(p []byte) (int, error) {
	if err := r.waitReady(); err != nil {
		return 0, err
	}

	var (
		slot = r.nextChunk % r.numSlots
		clen = r.chunkLen(r.nextChunk)
		off  = int64(slot)*r.chunkSize + r.readOff
	)
	n := copy(p, r.buf[off:off+clen-r.readOff]) // copy from ring buf to caller's buffer
	r.readOff += int64(n)

	// Chunk fully consumed — recycle slot
	if r.readOff >= clen {
		r.slotFree[slot] <- struct{}{}
		r.nextChunk++
		r.readOff = 0
	}
	return n, nil
}

// waitReady blocks until the current chunk's slot is ready
// Returns immediately if token already consumed or at EOF
func (r *mpdReader) waitReady() error {
	if r.nextChunk >= r.numChunks {
		return io.EOF
	}
	if r.readOff > 0 {
		return nil // already read some bytes from current chunk - continue
	}
	select {
	case <-r.slotReady[r.nextChunk%r.numSlots]:
		return nil
	case <-r.stop.Listen():
		if p := r.err.Load(); p != nil {
			return *p
		}
		return io.ErrClosedPipe
	}
}

// chunkLen returns the byte length of chunk at the given index.
func (r *mpdReader) chunkLen(idx int) int64 {
	if idx < r.numChunks-1 {
		return r.chunkSize
	}
	return r.objectSize - int64(idx)*r.chunkSize
}

func (r *mpdReader) setError(err error) {
	r.err.Store(&err)
	r.stop.Close()
}

func (r *mpdReader) Close() error {
	r.stop.Close()
	r.client.CloseIdleConnections()
	return nil
}

// produce spawns workers and feeds them chunks in order.
func (r *mpdReader) produce() {
	var wg sync.WaitGroup
	for range r.numWorkers {
		wg.Go(r.work)
	}

loop:
	for i := range r.numChunks {
		offset := int64(i) * r.chunkSize
		length := min(r.chunkSize, r.objectSize-offset)
		select {
		case r.chunkCh <- mptDownloadChunk{index: i, offset: offset, length: length}:
		case <-r.stop.Listen():
			break loop
		}
	}
	close(r.chunkCh)
	wg.Wait()
}

// work is the per-worker loop: acquire slot, download, release slot.
func (r *mpdReader) work() {
	for {
		select {
		case <-r.stop.Listen():
			return
		case chunk, ok := <-r.chunkCh:
			if !ok {
				return
			}
			slot := chunk.index % r.numSlots
			select {
			case <-r.slotFree[slot]: // acquire the slot
			case <-r.stop.Listen():
				return
			}
			if err := r.fetchChunk(slot, chunk); err != nil {
				r.setError(err)
				return
			}
			r.slotReady[slot] <- struct{}{} // release the slot
		}
	}
}

// fetchChunk downloads a single chunk directly into the ring buffer slot.
func (r *mpdReader) fetchChunk(slot int, chunk mptDownloadChunk) error {
	reader, _, err := GetObjectReader(r.bp, r.bck, r.objName, &GetArgs{
		Header: http.Header{cos.HdrRange: []string{cmn.MakeRangeHdr(chunk.offset, chunk.length)}},
	})
	if err != nil {
		return fmt.Errorf("chunk %d: get reader: %w", chunk.index, err)
	}
	defer reader.Close()

	off := int64(slot) * r.chunkSize
	if _, err := io.ReadFull(reader, r.buf[off:off+chunk.length]); err != nil {
		return fmt.Errorf("chunk %d: read: %w", chunk.index, err)
	}
	return nil
}
