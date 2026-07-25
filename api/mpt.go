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
	"math"
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
		// Writer receives non-overlapping ranges concurrently (required).
		// It must support concurrent WriteAt calls.
		Writer io.WriterAt
		// Optional progress callback.
		Callback MpdCB
		// Minimum interval between progress callbacks; <= 0 disables intermediate
		// callbacks. The final callback is always invoked.
		CallEvery time.Duration
		// Number of concurrent download workers (default: 16)
		NumWorkers int
		// Size of each chunk/range to download (default: 8 MiB)
		ChunkSize int64
		// ObjectSize can be set to skip the HEAD request (optional, 0 means auto-detect)
		ObjectSize int64
	}

	MpdCounter struct {
		callback  MpdCB
		callEvery int64
		total     int64

		callbackMu sync.Mutex
		nextCall   atomic.Int64
		current    atomic.Int64
		done       atomic.Bool
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
		writer  io.WriterAt
		cancel  context.CancelFunc
		chunkCh <-chan mptDownloadChunk
		errCh   chan<- error
		counter *MpdCounter
		bp      BaseParams
		bck     cmn.Bck
		objName string
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
	// slot = chunkIndex % numSlots
	// Per-slot token channels (buffered 1) coordinate the handoff:
	// slotFree[i]  - producer may dispatch chunk i into the slot
	// slotReady[i] - reader can read
	mpdReader struct {
		chunkCh      chan mptDownloadChunk
		client       *http.Client          // client for concurrent chunk downloads
		err          atomic.Pointer[error] // stop err
		ctx          context.Context
		cancel       context.CancelFunc
		bck          cmn.Bck
		bp           BaseParams
		objName      string
		slotReady    []chan struct{}
		slotFree     []chan struct{}
		buf          []byte
		numChunks    int
		numWorkers   int
		numSlots     int
		objectSize   int64
		chunkSize    int64
		nextChunk    int   // reader: next chunk index to read
		readOff      int64 // reader: bytes already read from current chunk
		ownTransport bool
		closeOnce    sync.Once
	}

	// mpdRoundTripper adds multipart-download cancellation while retaining the
	// caller's transport behavior.
	mpdRoundTripper struct {
		base http.RoundTripper
		ctx  context.Context
	}

	mpdResponseBody struct {
		io.ReadCloser
		stop   func() bool
		cancel context.CancelFunc
		once   sync.Once
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

// Complete multipart upload:
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

// Abort multipart upload.
// uploadID: the ID of the multipart upload to abort
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

// Perform concurrent range-based download:
// 1. Issue a HEAD request when ObjectSize is not provided
// 2. Divide the object into chunks based on ChunkSize
// 3. Spawn NumWorkers goroutines to download chunks concurrently
// 4. Each worker issues a GET request with Range header
// 5. Results are written to the provided WriterAt at the correct offset
// Return error if any chunk download fails.
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

	// Validate optional values independently.
	if chunkSize < 0 {
		return fmt.Errorf("invalid chunk size: %d", chunkSize)
	}
	if objectSize < 0 {
		return fmt.Errorf("invalid object size: %d", objectSize)
	}

	// Resolve a missing size via HEAD. Request chunk metadata at the same time so
	// it can supply the default chunk size.
	var opV2 *cmn.ObjectPropsV2
	if objectSize == 0 {
		props := apc.JoinProps(apc.GetPropsSize, apc.GetPropsChunked)
		var err error
		opV2, err = HeadObjectV2(bp, bck, objName, props, HeadArgs{})
		if err != nil {
			return fmt.Errorf("failed to get object properties: %w", err)
		}
		objectSize = opV2.Size
		if objectSize < 0 {
			return fmt.Errorf("invalid object size: %d", objectSize)
		}
	}

	if chunkSize == 0 {
		if opV2 != nil && opV2.Chunks != nil && opV2.Chunks.MaxChunkSize > 0 {
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

// perform concurrent range-based download
func multipartDownload(bp BaseParams, bck cmn.Bck, objName string, args *MultipartDownloadArgs) error {
	if args.ObjectSize == 0 {
		if args.Callback != nil {
			counter := &MpdCounter{callback: args.Callback}
			counter.finish()
		}
		return nil
	}

	numChunks, err := mpdNumChunks(args.ObjectSize, args.ChunkSize)
	if err != nil {
		return err
	}

	var (
		numWorkers = min(args.NumWorkers, numChunks)
		chunkCh    = make(chan mptDownloadChunk, mpdQueueCap(numChunks, numWorkers))
		errCh      = make(chan error, numWorkers)
		errs       = make([]error, 0, numWorkers)
		counter    *MpdCounter
	)
	if args.Callback != nil {
		counter = &MpdCounter{
			callback:  args.Callback,
			callEvery: args.CallEvery.Nanoseconds(),
			total:     args.ObjectSize,
		}
		if counter.callEvery > 0 {
			counter.nextCall.Store(mono.NanoTime() + counter.callEvery)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, ownTransport := newMpdClient(ctx, bp.Client, numWorkers, false /*dedicated*/)
	if ownTransport {
		defer client.CloseIdleConnections()
	}
	bp.Client = client

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

	// start workers
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(w.run)
	}

	// produce chunks
loop:
	for i := range numChunks {
		offset := int64(i) * args.ChunkSize
		length := args.ChunkSize
		if offset+length > args.ObjectSize {
			length = args.ObjectSize - offset // last chunk may be smaller
		}
		chunk := mptDownloadChunk{
			index:  i,
			offset: offset,
			length: length,
		}
		if ctx.Err() != nil {
			break loop
		}
		select {
		case chunkCh <- chunk:
		case <-ctx.Done():
			break loop
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
			w.counter.current.Add(chunk.length)
			w.counter.maybeCall()
		}
	}
}

// Download a single chunk
func mptDownloadChunkRange(bp BaseParams, bck cmn.Bck, objName string, writer io.WriterAt, chunk mptDownloadChunk) error {
	reader, size, err := GetObjectReader(bp, bck, objName, &GetArgs{
		Header: http.Header{cos.HdrRange: []string{cmn.MakeRangeHdr(chunk.offset, chunk.length)}},
	})
	if err != nil {
		return fmt.Errorf("chunk %d: failed to get reader: %w", chunk.index, err)
	}
	defer reader.Close()

	if size >= 0 && size != chunk.length {
		return fmt.Errorf("chunk %d: invalid response length: expected %d bytes, got %d",
			chunk.index, chunk.length, size)
	}

	// write at correct offset with bounded buffer
	sw := cos.NewSectionWriter(writer, chunk.offset)
	n, err := io.CopyN(sw, reader, chunk.length)
	if err != nil {
		return fmt.Errorf("chunk %d: failed to copy: expected %d bytes, got %d: %w",
			chunk.index, chunk.length, n, err)
	}

	// always read the trailing EOF
	var one [1]byte
	if n, err := io.ReadFull(reader, one[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("chunk %d: response exceeds expected length %d", chunk.index, chunk.length)
	}

	return nil
}

// requires objectSize >= 1 and chunkSize >= 1: objectSize == 0 silently yields 1
func mpdNumChunks(objectSize, chunkSize int64) (int, error) {
	n := (objectSize-1)/chunkSize + 1 // overflow-safe
	if n > math.MaxInt {
		return 0, fmt.Errorf("too many chunks: %d", n)
	}
	return int(n), nil
}

func mpdQueueCap(numChunks, numWorkers int) int {
	if numWorkers <= numChunks/2 {
		return numWorkers * 2
	}
	return numChunks
}

////////////////
// MpdCounter //
////////////////

func (c *MpdCounter) IsFinished() bool { return c.done.Load() }
func (c *MpdCounter) Current() int64   { return c.current.Load() }
func (c *MpdCounter) Total() int64     { return c.total }

func (c *MpdCounter) maybeCall() {
	if c.callEvery <= 0 {
		return
	}

	now := mono.NanoTime()
	if now < c.nextCall.Load() {
		return
	}

	// serialize user callbacks and recheck the deadline
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()

	now = mono.NanoTime()
	if now < c.nextCall.Load() {
		return
	}
	c.nextCall.Store(now + c.callEvery)
	c.callback(c)
}

func (c *MpdCounter) finish() {
	c.callbackMu.Lock()
	c.done.Store(true)
	c.callback(c)
	c.callbackMu.Unlock()
}

// Derive a client from the caller's client and, when possible,
// clones its transport to give the multipart download an independent connection
// pool. The wrapper additionally links every request to ctx so cancellation
// interrupts active response-body reads.
func newMpdClient(ctx context.Context, base *http.Client, numWorkers int, dedicated bool) (client *http.Client, ownTransport bool) {
	if base == nil {
		base = http.DefaultClient
	}

	cloned := *base // preserve CheckRedirect, Jar, and all other client behavior
	rt := base.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}

	if tr, ok := rt.(*http.Transport); dedicated && ok {
		tr = tr.Clone()
		if tr.MaxIdleConnsPerHost < numWorkers {
			tr.MaxIdleConnsPerHost = numWorkers
		}
		// Zero means unlimited; retain it.
		if tr.MaxIdleConns > 0 && tr.MaxIdleConns < numWorkers {
			tr.MaxIdleConns = numWorkers
		}
		rt = tr
		ownTransport = true
	}

	cloned.Transport = &mpdRoundTripper{base: rt, ctx: ctx}
	return &cloned, ownTransport
}

func (rt *mpdRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(req.Context())
	stop := context.AfterFunc(rt.ctx, cancel)

	resp, err := rt.base.RoundTrip(req.Clone(ctx))
	if err != nil {
		stop()
		cancel()
		return nil, err
	}
	if resp.Body == nil {
		stop()
		cancel()
		return resp, nil
	}

	resp.Body = &mpdResponseBody{
		ReadCloser: resp.Body,
		stop:       stop,
		cancel:     cancel,
	}
	return resp, nil
}

func (rt *mpdRoundTripper) CloseIdleConnections() {
	if ci, ok := rt.base.(interface{ CloseIdleConnections() }); ok {
		ci.CloseIdleConnections()
	}
}

func (body *mpdResponseBody) Read(p []byte) (n int, err error) {
	n, err = body.ReadCloser.Read(p)
	if err != nil {
		body.finish()
	}
	return
}

func (body *mpdResponseBody) Close() error {
	err := body.ReadCloser.Close()
	body.finish()
	return err
}

func (body *mpdResponseBody) finish() {
	body.once.Do(func() {
		body.stop()
		body.cancel()
	})
}

/////////////////////////////
// MultipartDownloadStream //
/////////////////////////////

// MultipartDownloadStream performs concurrent range-based download and returns
// an io.ReadCloser. Close cancels outstanding range requests.
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
		bufferSize = args.BufferSize
	)
	if chunkSize < 0 {
		return nil, oah, fmt.Errorf("invalid chunk size: %d", chunkSize)
	}
	if objectSize < 0 {
		return nil, oah, fmt.Errorf("invalid object size: %d", objectSize)
	}
	if bufferSize < 0 {
		return nil, oah, fmt.Errorf("invalid buffer size: %d", bufferSize)
	}
	// TODO: consider using server-suggested chunk size (opV2.Chunks.MaxChunkSize)
	if chunkSize == 0 {
		chunkSize = defaultMptDownloadChunkSize
	}
	if objectSize == 0 {
		reqProps := apc.JoinProps(apc.GetPropsSize, apc.GetPropsChecksum)
		opV2, err := HeadObjectV2(bp, bck, objName, reqProps, HeadArgs{})
		if err != nil {
			return nil, oah, fmt.Errorf("head %s: %w", bck.Cname(objName), err)
		}
		objectSize = opV2.Size
		oah.wrespHeader = make(http.Header, 4)
		cmn.ToHeaderV2(&opV2.ObjAttrs, oah.wrespHeader, true /*cksum*/, false, false, false)
		if objectSize < 0 {
			return nil, oah, fmt.Errorf("invalid object size: %d", objectSize)
		}
	}
	oah.n = objectSize
	if objectSize == 0 {
		return http.NoBody, oah, nil
	}

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
	numChunks, err := mpdNumChunks(objectSize, chunkSize)
	if err != nil {
		return nil, oah, err
	}
	if numWorkers > numChunks {
		numWorkers = numChunks
	}

	// Buffer size defaults and validation
	if bufferSize == 0 {
		bufferSize = int64(numWorkers) * chunkSize
	}
	if bufferSize < chunkSize {
		return nil, oah, fmt.Errorf("BufferSize (%d) must be >= ChunkSize (%d)", bufferSize, chunkSize)
	}
	numSlots := int(bufferSize / chunkSize) // round down
	if numWorkers > numSlots {
		numWorkers = numSlots
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, ownTransport := newMpdClient(ctx, bp.Client, numWorkers, true /*dedicated*/)
	bp.Client = client

	reader := &mpdReader{
		bp:           bp,
		bck:          bck,
		objName:      objName,
		client:       client,
		chunkSize:    chunkSize,
		objectSize:   objectSize,
		numSlots:     numSlots,
		numChunks:    numChunks,
		numWorkers:   numWorkers,
		ctx:          ctx,
		cancel:       cancel,
		ownTransport: ownTransport,

		// Ring buffer
		buf: make([]byte, int64(numSlots)*chunkSize),

		// Channels
		chunkCh:   make(chan mptDownloadChunk, mpdQueueCap(numChunks, numWorkers)),
		slotFree:  make([]chan struct{}, numSlots),
		slotReady: make([]chan struct{}, numSlots),
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
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.waitReady(); err != nil {
		return 0, err
	}

	var (
		slot = r.nextChunk % r.numSlots
		clen = r.chunkLen(r.nextChunk)
		off  = int64(slot)*r.chunkSize + r.readOff
	)
	n := copy(p, r.buf[off:off+clen-r.readOff]) // from ring buf to caller's buffer
	r.readOff += int64(n)

	// recycle slot
	if r.readOff >= clen {
		r.slotFree[slot] <- struct{}{}
		r.nextChunk++
		r.readOff = 0
	}
	return n, nil
}

// block until the current chunk's slot is ready
func (r *mpdReader) waitReady() error {
	if r.nextChunk >= r.numChunks {
		r.shutdown()
		return io.EOF
	}
	if r.readOff > 0 {
		return nil // already read some bytes from current chunk
	}
	select {
	case <-r.slotReady[r.nextChunk%r.numSlots]:
		return nil
	case <-r.ctx.Done():
		if p := r.err.Load(); p != nil {
			return *p
		}
		return io.ErrClosedPipe
	}
}

// return length of a chunk at the given index
func (r *mpdReader) chunkLen(idx int) int64 {
	if idx < r.numChunks-1 {
		return r.chunkSize
	}
	return r.objectSize - int64(idx)*r.chunkSize
}

func (r *mpdReader) setError(err error) {
	if r.ctx.Err() != nil {
		return // either Close or another worker already stopped
	}
	r.err.CompareAndSwap(nil, &err) // retain the first error
	r.shutdown()
}

func (r *mpdReader) Close() error {
	r.shutdown()
	return nil
}

func (r *mpdReader) shutdown() {
	r.closeOnce.Do(func() {
		r.cancel()
		if r.ownTransport {
			r.client.CloseIdleConnections()
		}
	})
}

// start worker and feed them chunks in order
func (r *mpdReader) produce() {
	var wg sync.WaitGroup
	for range r.numWorkers {
		wg.Go(r.work)
	}

loop:
	for i := range r.numChunks {
		var (
			offset = int64(i) * r.chunkSize
			length = min(r.chunkSize, r.objectSize-offset)
			slot   = i % r.numSlots
		)

		// reserve the slot before publishing the work
		select {
		case <-r.slotFree[slot]:
		case <-r.ctx.Done():
			break loop
		}
		select {
		case r.chunkCh <- mptDownloadChunk{index: i, offset: offset, length: length}:
		case <-r.ctx.Done():
			break loop
		}
	}

	close(r.chunkCh)
	wg.Wait()
}

// work is the per-worker loop: download into a producer-reserved slot.
func (r *mpdReader) work() {
	for {
		if r.ctx.Err() != nil {
			return
		}
		select {
		case <-r.ctx.Done():
			return
		case chunk, ok := <-r.chunkCh:
			if !ok {
				return
			}
			slot := chunk.index % r.numSlots
			if err := r.fetchChunk(slot, chunk); err != nil {
				r.setError(err)
				return
			}
			select {
			case r.slotReady[slot] <- struct{}{}:
			case <-r.ctx.Done():
				return
			}
		}
	}
}

// download a single chunk directly into the ring buffer slot
func (r *mpdReader) fetchChunk(slot int, chunk mptDownloadChunk) error {
	reader, size, err := GetObjectReader(r.bp, r.bck, r.objName, &GetArgs{
		Header: http.Header{cos.HdrRange: []string{cmn.MakeRangeHdr(chunk.offset, chunk.length)}},
	})
	if err != nil {
		return fmt.Errorf("chunk %d: get reader: %w", chunk.index, err)
	}
	defer reader.Close()

	if size >= 0 && size != chunk.length {
		return fmt.Errorf("chunk %d: invalid response length: expected %d bytes, got %d",
			chunk.index, chunk.length, size)
	}

	off := int64(slot) * r.chunkSize
	if _, err := io.ReadFull(reader, r.buf[off:off+chunk.length]); err != nil {
		return fmt.Errorf("chunk %d: read: %w", chunk.index, err)
	}

	// compare w/ mptDownloadChunkRange
	var one [1]byte
	if n, err := io.ReadFull(reader, one[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("chunk %d: response exceeds expected length %d", chunk.index, chunk.length)
	}
	return nil
}
