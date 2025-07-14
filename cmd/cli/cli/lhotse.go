// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/pierrec/lz4/v4"
	"github.com/urfave/cli"
)

// tunables
const (
	lhotseIniBufSize = 4 * cos.MiB
	lhotseMaxBufSize = 16 * cos.MiB

	lhotseNumEntries = 1024

	lhotseDefaultProvider = apc.AIS // `ais://`
)

type (
	lhotseCut struct {
		ID       string  `json:"id"`
		Start    float64 `json:"start"`    // seconds
		Duration float64 `json:"duration"` // ditto

		Recording struct {
			// modern layout: array of sources
			Sources []struct {
				Source string `json:"source"`
			} `json:"sources"`
			// alt layout: single path
			Path string `json:"path,omitempty"`
		} `json:"recording,omitempty"`

		// very old layout: top-level field
		AudioSource string `json:"audio_source,omitempty"`
	}
	lhotseReaderCtx struct {
		reader  io.Reader
		cleanup func() error
	}
)

/////////////////////
// lhotseReaderCtx //
/////////////////////

func (ctx *lhotseReaderCtx) Close() error {
	if ctx.cleanup != nil {
		return ctx.cleanup()
	}
	return nil
}

// open a Lhotse manifest, detect compression and
// return reader context with cleanup callback
func openLhotseReader(c *cli.Context) (*lhotseReaderCtx, error) {
	path := parseStrFlag(c, lhotseManifestFlag)
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Detect compression format
	var ext string
	switch {
	case strings.HasSuffix(path, archive.ExtGz) || strings.HasSuffix(path, ".gzip"):
		ext = archive.ExtGz
	case strings.HasSuffix(path, archive.ExtLz4):
		ext = archive.ExtLz4
	default:
		ext, err = archive.DetectCompression(fh)
		if err != nil {
			fh.Close()
			return nil, err
		}
	}

	// Setup appropriate reader based on compression
	var (
		r       io.Reader
		cleanup func() error
	)

	switch ext {
	case archive.ExtGz:
		gzr, err := gzip.NewReader(fh)
		if err != nil {
			fh.Close()
			return nil, err
		}
		r = gzr
		cleanup = func() error {
			gzr.Close()
			return fh.Close()
		}
	case archive.ExtLz4:
		r = lz4.NewReader(fh)
		cleanup = fh.Close
	default:
		r = fh // plain text
		cleanup = fh.Close
	}

	return &lhotseReaderCtx{
		reader:  r,
		cleanup: cleanup,
	}, nil
}

// single manifest => output batch
// return []apc.MossIn for a single batch req
// (compare with lhotseMultiBatch flow)
func loadAndParseLhotse(c *cli.Context) ([]apc.MossIn, error) {
	ctx, err := openLhotseReader(c)
	if err != nil {
		return nil, err
	}
	defer ctx.Close()

	var (
		lineNum int
		ins     = make([]apc.MossIn, 0, lhotseNumEntries)
		sc      = bufio.NewScanner(ctx.reader)
		buf     = make([]byte, 0, lhotseIniBufSize) // TODO: consider memsys.SGL and its NextLine() method
	)
	sc.Buffer(buf, lhotseMaxBufSize)

	for sc.Scan() {
		lineNum++
		var cut lhotseCut
		if err := json.Unmarshal(sc.Bytes(), &cut); err != nil {
			return nil, fmt.Errorf("bad cut json at line %d: %w", lineNum, err)
		}
		in, err := cut.toMossIn(c)
		if err != nil {
			return nil, err
		}
		ins = append(ins, *in)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return ins, nil
}

func (cut *lhotseCut) toMossIn(c *cli.Context) (in *apc.MossIn, err error) {
	var uri string
	if uri, err = cut.uri(); err != nil {
		return
	}

	in = &apc.MossIn{}

	var bck cmn.Bck
	bck, in.ObjName, err = parseLhotseSrc(uri)
	if err != nil {
		return
	}
	in.Bucket, in.Provider = bck.Name, bck.Provider

	if !flagIsSet(c, sampleRateFlag) {
		return
	}
	rate := int64(parseIntFlag(c, sampleRateFlag))
	if err := validateSampleRate(rate); err != nil {
		return nil, err
	}
	in.Start = int64(cut.Start) * rate
	in.Length = int64(cut.Duration) * rate
	return
}

// minor sanity check
func validateSampleRate(rate int64) error {
	const a, b = 1000, 384000
	if rate <= 0 {
		return fmt.Errorf("sample rate must be positive, got %d", rate)
	}
	if rate < a || rate > b {
		return fmt.Errorf("sample rate %d Hz is outside reasonable range [%d, %d]", rate, a, b)
	}
	return nil
}

// resolve Lhotse source URI across the three layouts
func (cut *lhotseCut) uri() (string, error) {
	if len(cut.Recording.Sources) > 0 && cut.Recording.Sources[0].Source != "" {
		return cut.Recording.Sources[0].Source, nil
	}
	if cut.Recording.Path != "" {
		return cut.Recording.Path, nil
	}
	if cut.AudioSource != "" {
		return cut.AudioSource, nil
	}
	return "", fmt.Errorf("cut %q: no audio source field found", cut.ID)
}

// e.g. "s3://bucket/dir/file.wav" => ("bucket", "dir/file.wav")
// - works for ais://, gs://, and plain "bucket/obj"
// - no scheme defaults to lhotseDefaultProvider
func parseLhotseSrc(uri string) (bck cmn.Bck, objName string, err error) {
	return cmn.ParseBckObjectURI(uri, cmn.ParseURIOpts{DefaultProvider: lhotseDefaultProvider})
}

//
// generate multiple batches in a streaming fashion
//

func lhotseMultiBatch(c *cli.Context, outCtx *mossReqParseCtx) error {
	inCtx, err := openLhotseReader(c)
	if err != nil {
		return err
	}
	defer inCtx.Close()

	var (
		lineNum      int
		currentBatch = make([]apc.MossIn, 0, outCtx.batchSize)
		sc           = bufio.NewScanner(inCtx.reader)
		buf          = make([]byte, 0, lhotseIniBufSize)
		totalCuts    int
		totalBatches int
	)
	sc.Buffer(buf, lhotseMaxBufSize)

	outCtx.pt.InitIter()

	for sc.Scan() {
		lineNum++
		var cut lhotseCut
		if err := json.Unmarshal(sc.Bytes(), &cut); err != nil {
			return fmt.Errorf("bad cut json at line %d: %w", lineNum, err)
		}

		in, err := cut.toMossIn(c)
		if err != nil {
			return err
		}

		currentBatch = append(currentBatch, *in)
		totalCuts++

		// when batch is full
		if len(currentBatch) >= outCtx.batchSize {
			// Get next filename from template
			shardName, hasNext := outCtx.pt.Next()
			if !hasNext {
				return fmt.Errorf("template exhausted at batch %d (generated %d batches so far)", totalBatches+1, totalBatches)
			}

			if err := genLhotseBatch(c, outCtx, currentBatch, shardName); err != nil {
				if err == errUserCancel {
					return nil
				}
				return fmt.Errorf("failed to process batch %d (%s): %w", totalBatches, shardName, err)
			}
			totalBatches++
			currentBatch = currentBatch[:0] // reset slice but keep capacity
		}
	}

	if err := sc.Err(); err != nil {
		return err
	}

	// remaining cuts in a final batch
	if len(currentBatch) > 0 {
		shardName, hasNext := outCtx.pt.Next()
		if !hasNext {
			return fmt.Errorf("template exhausted for final batch (generated %d batches so far)", totalBatches)
		}

		if err := genLhotseBatch(c, outCtx, currentBatch, shardName); err != nil {
			if err == errUserCancel {
				return nil
			}
			return fmt.Errorf("failed to process final batch %d (%s): %w", totalBatches, shardName, err)
		}
		totalBatches++
	}

	if !flagIsSet(c, nonverboseFlag) {
		actionDone(c, fmt.Sprintf("Generated %d batches from %d cuts", totalBatches, totalCuts))
	}

	return nil
}

func genLhotseBatch(c *cli.Context, outCtx *mossReqParseCtx, batch []apc.MossIn, outputFile string) error {
	// 1) create MossReq for this batch
	req := apc.MossReq{
		In:            batch,
		OutputFormat:  outCtx.req.OutputFormat,
		ContinueOnErr: flagIsSet(c, continueOnErrorFlag),
		StreamingGet:  flagIsSet(c, streamingGetFlag),
		OnlyObjName:   flagIsSet(c, omitSrcBucketNameFlag),
	}

	w, wfh, err := createDstFile(c, outputFile, false /*allow stdout*/)
	if err != nil {
		return err
	}

	// 2) execute
	resp, err := api.GetBatch(apiBP, outCtx.bck, &req, w)
	if wfh != nil {
		wfh.Close()
	}
	if err != nil {
		if wfh != nil {
			cos.RemoveFile(outputFile)
		}
		return fmt.Errorf("API call failed for %s: %w", outputFile, err)
	}

	debug.Assert(req.StreamingGet || len(resp.Out) == len(req.In))

	if !flagIsSet(c, nonverboseFlag) {
		var msg string
		if req.StreamingGet {
			msg = fmt.Sprintf("Streamed %d objects and/or archived files => %s", len(req.In), outputFile)
		} else {
			msg = fmt.Sprintf("Created %s with %d objects and/or archived files", outputFile, len(resp.Out))
		}
		fmt.Fprintln(c.App.Writer, msg)
	}

	return nil
}
