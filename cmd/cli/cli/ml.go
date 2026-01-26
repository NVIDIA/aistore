// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/urfave/cli"
)

const mlspec = `
	$ ais ml get-batch /tmp/output.tar --spec '{
	  "in": [
	    {"objname": "file1.wav", "bucket": "speech-data", "provider": "ais"},
	    {"objname": "file2.wav", "bucket": "speech-data", "provider": "ais"},
	    {"objname": "model.pth", "bucket": "models", "provider": "s3"}
	  ],
	  "output_format": "tar",
	  "streaming_get": false
	}'
`
const getBatchUsage = "Get multiple objects and/or archived files from different buckets and package into a consolidated archive.\n" +
	indent1 + "\tReturns TAR by default; supported formats include: " + archFormats + ".\n" +
	indent1 + "\tAlso supports cross-bucket operations, archived file extraction, byte ranges, streaming and multipart operation, and more.\n" +
	indent1 + "\tExamples - one-liners:\n" +
	indent1 + "\t- 'ais ml get-batch ais://dataset --list \"audio1.wav, audio2.wav\" training.tar'\t- specific objects as TAR;\n" +
	indent1 + "\t- 'ais ml get-batch ais://models --template \"checkpoint-{001..100}.pth\" model-v2.tgz'\t- range as compressed TAR;\n" +
	indent1 + "\t- 'ais ml get-batch ais://data/shard.tar/file.wav dataset.zip'\t- extract single file from archived shard;\n" +
	indent1 + "\t- 'ais ml get-batch training.tar --spec batch.json'\t- JSON spec with cross-bucket objects;\n" +
	indent1 + "\t- 'ais ml get-batch dataset.tar.lz4 --spec batch.yaml --streaming'\t- YAML spec with streaming mode;\n" +
	indent1 + "\tExample inline JSON/YAML spec:" + mlspec +
	indent1 + "\tSee [API](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go) for complete reference and most recent updates."

const lhotseGetBatchUsage = "Get multiple objects from Lhotse manifests and package into consolidated archive(s).\n" +
	indent1 + "\tReturns TAR by default; supported formats include: " + archFormats + ".\n" +
	indent1 + "\tSupports chunking, filtering, and multi-output generation from Lhotse cut manifests.\n" +
	indent1 + "\tLhotse manifest format: each line contains a single cut JSON object with recording sources;\n" +
	indent1 + "\tLhotse manifest may be plain (`.jsonl`), gzip‑compressed (`.jsonl.gz` / `.gzip`), or LZ4‑compressed (`.jsonl.lz4`).\n" +
	indent1 + "\tExamples:\n" +
	indent1 + "\t- 'ais ml lhotse-get-batch --cuts manifest.jsonl.gz output.tar'\t- entire manifest as single TAR;\n" +
	indent1 + "\t- 'ais ml lhotse-get-batch --cuts cuts.jsonl --sample-rate 16000 output.tar'\t- with sample rate conversion;\n" +
	indent1 + "\t- 'ais ml lhotse-get-batch --cuts m.jsonl.lz4 --batch-size 1000 --output-template \"a-{001..999}.tar\"'\t- generate 999 'a-*.tar' batches (1000 cuts each)\n" +
	indent1 + "\tSee [Lhotse docs](https://lhotse.readthedocs.io/) for manifest format details."

var (
	mlFlags = map[string][]cli.Flag{
		cmdGetBatch: {
			specFlag,
			listFlag,
			templateFlag,
			verbObjPrefixFlag,
			omitSrcBucketNameFlag,
			continueOnErrorFlag,
			streamingGetFlag,
			nonverboseFlag,
			yesFlag,
		},
	}

	mlCmdGetBatch = cli.Command{
		Name:         cmdGetBatch,
		Usage:        getBatchUsage,
		ArgsUsage:    getBatchSpecArgument,
		Flags:        sortFlags(mlFlags[cmdGetBatch]),
		Action:       getBatchHandler,
		BashComplete: bucketCompletions(bcmplop{multiple: true}),
	}

	// top-level
	mlCmd = cli.Command{
		Name:  commandML,
		Usage: "Machine learning operations: batch dataset operations, and ML-specific workflows",
		Subcommands: []cli.Command{
			mlCmdGetBatch,
			makeAlias(&mlCmdGetBatch, &mkaliasOpts{
				newName:   cmdLhotseGetBatch,
				addFlags:  []cli.Flag{lhotseManifestFlag, sampleRateFlag, batchSizeFlag, outputTemplateFlag},
				usage:     lhotseGetBatchUsage,
				argsUsage: getBatchLhotseSpecArgument,
			}),
		},
	}
)

type mossReqCtx struct {
	// get-batch API
	req apc.MossReq
	bck cmn.Bck

	// output file
	outFile string

	// additional/optional command line input
	names []string
	shift int

	// Lhotse-specific
	pt        *cos.ParsedTemplate
	batchSize int
}

func getBatchHandler(c *cli.Context) error {
	ctx, err := parseMlArgs(c)
	if err != nil {
		return err
	}
	if err := ctx.load(c); err != nil {
		return err
	}

	if ctx.batchSize > 0 {
		debug.Assert(flagIsSet(c, lhotseManifestFlag), "native (non-lhotse) batching not implemented yet")
		return lhotseMultiBatch(c, ctx)
	}
	outFile, bck := ctx.outFile, ctx.bck

	// output
	w, wfh, err := createDstFile(c, outFile, false /*allow stdout*/)
	if err != nil {
		if err == errUserCancel {
			return nil
		}
		return err
	}

	// do
	req := &ctx.req
	resp, err := api.GetBatch(apiBP, bck, req, w)

	if wfh != nil {
		wfh.Close()
	}
	if err == nil {
		debug.Assert(req.StreamingGet || len(resp.Out) == len(req.In))

		if !flagIsSet(c, nonverboseFlag) {
			var msg string
			if req.StreamingGet {
				msg = fmt.Sprintf("Streamed %d objects to %s", len(req.In), outFile)
			} else {
				msg = fmt.Sprintf("Created ML batch archive %s with %d objects", outFile, len(resp.Out))
			}
			actionDone(c, msg)
		}
		return nil
	}

	// cleanup
	if wfh != nil {
		cos.RemoveFile(outFile)
	}
	return err
}

// parse CLI arguments and setup context
func parseMlArgs(c *cli.Context) (ctx *mossReqCtx, _ error) {
	if err := errMutuallyExclusive(c, listFlag, templateFlag, specFlag); err != nil {
		return nil, err
	}
	if err := errMutuallyExclusive(c, listFlag, templateFlag, lhotseManifestFlag); err != nil {
		return nil, err
	}
	if err := errMutuallyExclusive(c, specFlag, lhotseManifestFlag); err != nil {
		return nil, err
	}

	// bucket [+list|template]
	bck, objNameOrTmpl, err := parseBckObjURI(c, c.Args().Get(0), true /*emptyObjnameOK*/)
	if flagIsSet(c, listFlag) && err != nil {
		return nil, fmt.Errorf("option %s requires bucket in the command line [err: %v]", qflprn(listFlag), err)
	}
	if flagIsSet(c, templateFlag) && err != nil {
		return nil, fmt.Errorf("option %s requires bucket in the command line [err: %v]", qflprn(templateFlag), err)
	}

	ctx = &mossReqCtx{bck: bck}
	if err == nil {
		ctx.shift++
		oltp, err := dopOLTP(c, bck, objNameOrTmpl)
		if err != nil {
			return nil, err
		}
		switch {
		case oltp.tmpl != "":
			pt, err := cos.NewParsedTemplate(oltp.tmpl)
			if err != nil {
				return nil, err
			}
			if err := pt.CheckIsRange(); err != nil {
				return nil, err
			}
			if ctx.names, err = pt.Expand(); err != nil {
				return nil, err
			}
		case oltp.list == "":
			if objNameOrTmpl != "" {
				ctx.names = []string{objNameOrTmpl}
			}
		default:
			ctx.names = splitCsv(oltp.list)
		}
	}

	if len(ctx.names) == 0 && !flagIsSet(c, specFlag) && !flagIsSet(c, lhotseManifestFlag) {
		return nil, fmt.Errorf("with no (%s, %s) options expecting object names and/or archived filenames in the command line",
			qflprn(specFlag), qflprn(lhotseManifestFlag))
	}

	// lhotse only: given (batchSizeFlag & outputTemplateFlag) outFile(s) are computed from the latter
	if c.NArg() > ctx.shift {
		// Note: may be unused in Lhotse multi-batch mode
		ctx.outFile = c.Args().Get(ctx.shift)
		outputFormat, err := archive.Strict("", cos.Ext(ctx.outFile))
		if err != nil {
			return nil, err
		}
		ctx.req.OutputFormat = outputFormat
	}

	// there's no real way to check these assorted overrides; common expectation, though,
	// is for command line to take precedence

	ctx.req.ContinueOnErr = flagIsSet(c, continueOnErrorFlag)
	ctx.req.StreamingGet = flagIsSet(c, streamingGetFlag)
	ctx.req.OnlyObjName = flagIsSet(c, omitSrcBucketNameFlag)

	return ctx, nil
}

// populate get-batch request from native (single-batch) spec or Lhotse manifest
func (ctx *mossReqCtx) load(c *cli.Context) error {
	// native spec
	if flagIsSet(c, specFlag) {
		specBytes, ext, err := loadSpec(c)
		if err != nil {
			return err
		}

		outputFormat := ctx.req.OutputFormat
		if err := parseSpec(ext, specBytes, &ctx.req); err != nil {
			return err
		}
		// warn
		if ctx.req.OutputFormat != "" && outputFormat != "" && ctx.req.OutputFormat != outputFormat {
			if !flagIsSet(c, nonverboseFlag) {
				warn := fmt.Sprintf("output format %s in the command line takes precedence (over %s specified %s)",
					outputFormat, qflprn(specFlag), ctx.req.OutputFormat)
				actionWarn(c, warn)
			}
		}
	}

	// lhotse spec
	if flagIsSet(c, lhotseManifestFlag) {
		var err error
		ctx.batchSize, ctx.pt, err = parseLhotseBatchFlags(c)
		if err != nil {
			return err
		}
		if ctx.batchSize > 0 {
			// note: early return to generate multiple apc.MossReq requests and batches
			if ctx.outFile != "" {
				warn := fmt.Sprintf("output file %s is ignored in multi-batch mode (batch size %d)", ctx.outFile, ctx.batchSize)
				actionWarn(c, warn)
			}
			return nil
		}
		if ctx.outFile == "" {
			return errors.New("output file is required for single-batch mode")
		}

		ins, err := loadAndParseLhotse(c)
		if err != nil {
			return err
		}
		ctx.req.In = ins
	} else if ctx.outFile == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	if len(ctx.names) > 0 {
		if len(ctx.req.In) == 0 {
			ctx.req.In = make([]apc.MossIn, 0, len(ctx.names))
		} else {
			warn := fmt.Sprintf("adding %d command-line defined name%s to the %d spec-defined",
				len(ctx.names), cos.Plural(len(ctx.names)), len(ctx.req.In))
			actionWarn(c, warn)
		}
		for _, o := range ctx.names {
			in := apc.MossIn{
				ObjName: o,
				// no need to insert command-line bck -
				// the latter is passed as the default bucket in api.GetBatch
			}
			if oname, archpath := splitArchivePath(o); archpath != "" {
				in.ObjName = oname
				in.ArchPath = archpath
			}
			ctx.req.In = append(ctx.req.In, in)
		}
	}

	if len(ctx.req.In) == 0 {
		return errors.New("empty get-batch request")
	}

	return nil
}
