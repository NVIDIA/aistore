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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

// tunables
const (
	lhotseIniBufSize = 4 * cos.MiB
	lhotseMaxBufSize = 16 * cos.MiB

	lhotseNumEntries = 1024

	lhotseDefaultProvider = apc.AIS // `ais://`
)

type LhotseCut struct {
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

// TODO: it's a draft
func loadAndParseLhotse(c *cli.Context) ([]apc.MossIn, error) {
	path := parseStrFlag(c, lhotseCutsFlag)
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	var r io.Reader = fh
	//
	// TODO -- FIXME: on "bad cut json" failure use archive._detect() for `magicGzip`, and retry
	//
	if strings.HasSuffix(path, ".gz") || strings.HasSuffix(path, ".gzip") {
		gzr, err := gzip.NewReader(fh)
		if err != nil {
			return nil, err
		}
		defer gzr.Close()
		r = gzr
	}

	var (
		lineNum int
		ins     = make([]apc.MossIn, 0, lhotseNumEntries)
		sc      = bufio.NewScanner(r)
		buf     = make([]byte, 0, lhotseIniBufSize) // TODO -- FIXME: consider memsys.SGL and its NextLine() method
	)
	sc.Buffer(buf, lhotseMaxBufSize)

	for sc.Scan() {
		lineNum++
		var cut LhotseCut
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

func (cut *LhotseCut) toMossIn(c *cli.Context) (in *apc.MossIn, err error) {
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
func (cut *LhotseCut) uri() (string, error) {
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
