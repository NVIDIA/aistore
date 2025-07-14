// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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
	indent1 + "\tExamples:\n" +
	indent1 + "\t- 'ais ml lhotse-get-batch --cuts manifest.jsonl.gz output.tar'\t- entire manifest as single TAR;\n" +
	indent1 + "\t- 'ais ml lhotse-get-batch --cuts cuts.jsonl --sample-rate 16000 output.tar'\t- with sample rate conversion;\n" +
	indent1 + "\t- 'ais ml lhotse-get-batch --cuts manifest.jsonl.gz --batch-size 1000 --output-template \"batch-{001..999}.tar\"'\t- chunked output;\n" +
	indent1 + "\tLhotse manifest format: each line contains a single cut JSON object with recording sources.\n" +
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
				newName:  cmdLhotseGetBatch,
				addFlags: []cli.Flag{lhotseCutsFlag, sampleRateFlag},
				usage:    lhotseGetBatchUsage,
			}),
		},
	}
)

type (
	mossReqParseCtx struct {
		req     apc.MossReq
		outFile string
		bck     cmn.Bck
	}
)

func buildMossReq(c *cli.Context) (*mossReqParseCtx, error) {
	var (
		req   apc.MossReq
		shift int
	)
	if c.NArg() < 1 {
		return nil, missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if err := errMutuallyExclusive(c, listFlag, templateFlag, specFlag); err != nil {
		return nil, err
	}
	if err := errMutuallyExclusive(c, listFlag, templateFlag, lhotseCutsFlag); err != nil {
		return nil, err
	}
	if err := errMutuallyExclusive(c, specFlag, lhotseCutsFlag); err != nil {
		return nil, err
	}

	// bucket [+list|template]
	var (
		names []string
	)
	bck, objNameOrTmpl, err := parseBckObjURI(c, c.Args().Get(0), true /*emptyObjnameOK*/)
	if flagIsSet(c, listFlag) && err != nil {
		return nil, fmt.Errorf("option %s requires bucket in the command line [err: %v]", qflprn(listFlag), err)
	}
	if flagIsSet(c, templateFlag) && err != nil {
		return nil, fmt.Errorf("option %s requires bucket in the command line [err: %v]", qflprn(templateFlag), err)
	}

	if err == nil {
		shift++
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
			if names, err = pt.Expand(); err != nil {
				return nil, err
			}
		case oltp.list == "":
			if objNameOrTmpl != "" {
				names = []string{objNameOrTmpl}
			}
		default:
			names = splitCsv(oltp.list)
		}
	}

	if len(names) == 0 && !flagIsSet(c, specFlag) && !flagIsSet(c, lhotseCutsFlag) {
		return nil, fmt.Errorf("with no (%s, %s) options expecting object names and/or archived filenames in the command line",
			qflprn(specFlag), qflprn(lhotseCutsFlag))
	}

	// native spec
	if flagIsSet(c, specFlag) {
		specBytes, ext, err := loadSpec(c)
		if err != nil {
			return nil, err
		}
		if err := parseSpec(ext, specBytes, &req); err != nil {
			return nil, err
		}
	}
	// lhotse spec
	if flagIsSet(c, lhotseCutsFlag) {
		ins, err := loadAndParseLhotse(c)
		if err != nil {
			return nil, err
		}
		req.In = ins
	}

	// output format
	var (
		outputFormat string // TODO: potential extension-less usage; can wait
		outFile      = c.Args().Get(shift)
	)
	outputFormat, err = archive.Strict("", cos.Ext(outFile))
	if err != nil {
		return nil, err
	}
	if req.OutputFormat != "" && outputFormat != "" && req.OutputFormat != outputFormat {
		if !flagIsSet(c, nonverboseFlag) {
			warn := fmt.Sprintf("output format %s in the command line takes precedence (over %s specified %s)",
				outputFormat, qflprn(specFlag), req.OutputFormat)
			actionWarn(c, warn)
		}
	}
	req.OutputFormat = outputFormat

	// NOTE: no real way to check these assorted overrides; common expectation, though,
	// is for command line to take precedence
	req.ContinueOnErr = flagIsSet(c, continueOnErrorFlag)
	req.StreamingGet = flagIsSet(c, streamingGetFlag)
	req.OnlyObjName = flagIsSet(c, omitSrcBucketNameFlag)

	if len(names) > 0 {
		if len(req.In) == 0 {
			req.In = make([]apc.MossIn, 0, len(names))
		} else {
			warn := fmt.Sprintf("adding %d command-line defined name%s to the %d spec-defined",
				len(names), cos.Plural(len(names)), len(req.In))
			actionWarn(c, warn)
		}
		for _, o := range names {
			in := apc.MossIn{
				ObjName: o,
				// no need to insert command-line bck -
				// the latter is passed as the default bucket in api.GetBatch
			}
			if oname, archpath := splitPrefixShardBoundary(o); archpath != "" {
				in.ObjName = oname
				in.ArchPath = archpath
			}
			req.In = append(req.In, in)
		}
	}

	if len(req.In) == 0 {
		return nil, errors.New("empty get-batch request")
	}

	return &mossReqParseCtx{
		req:     req,
		outFile: outFile,
		bck:     bck,
	}, nil
}

func getBatchHandler(c *cli.Context) error {
	ctx, err := buildMossReq(c)
	if err != nil {
		return err
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
