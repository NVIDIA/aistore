// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
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
	indent1 + "\t- 'ais ml get-batch ais://data/shard.tar/file.wav dataset.zip'\t- extract file from archived shard;\n" +
	indent1 + "\t- 'ais ml get-batch training.tar --spec batch.json'\t- JSON spec with cross-bucket objects;\n" +
	indent1 + "\t- 'ais ml get-batch dataset.tar.lz4 --spec batch.yaml --streaming'\t- YAML spec with streaming mode;\n" +
	indent1 + "\tExample inline spec:" + mlspec +
	indent1 + "\tSee https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go for complete JSON/YAML specification reference."

var (
	mlFlags = map[string][]cli.Flag{
		commandGetBatch: {
			specFlag,
			listFlag,
			templateFlag,
			verbObjPrefixFlag,
			inclSrcBucketNameFlag,
			continueOnErrorFlag,
			streamingGetFlag,
			nonverboseFlag,
		},
	}

	mlCmdGetBatch = cli.Command{
		Name:         commandGetBatch,
		Usage:        getBatchUsage,
		ArgsUsage:    getBatchSpecArgument,
		Flags:        sortFlags(mlFlags[commandGetBatch]),
		Action:       getBatchHandler,
		BashComplete: bucketCompletions(bcmplop{multiple: true}),
	}

	// top-level
	mlCmd = cli.Command{
		Name:  commandML,
		Usage: "Machine learning operations: batch dataset operations, and ML-specific workflows",
		Subcommands: []cli.Command{
			mlCmdGetBatch,
		},
	}
)

func getBatchHandler(c *cli.Context) error {
	var (
		req     apc.MossReq
		shift   int
		outFile string
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if flagIsSet(c, listFlag) && flagIsSet(c, specFlag) {
		return incorrectUsageMsg(c, fmt.Sprintf("%s and %s options are mutually exclusive",
			flprn(listFlag), flprn(specFlag)))
	}

	// bucket [+list]
	var names []string
	bck, objNameOrTmpl, err := parseBckObjURI(c, c.Args().Get(0), true /*emptyObjnameOK*/)

	if flagIsSet(c, listFlag) && err != nil {
		return fmt.Errorf("option %s requires bucket in the command line [err: %v]", flprn(listFlag), err)
	}

	if err == nil {
		shift++
		oltp, err := dopOLTP(c, bck, objNameOrTmpl)
		if err != nil {
			return err
		}
		if oltp.list == "" {
			if objNameOrTmpl != "" {
				names = []string{objNameOrTmpl}
			}
		} else {
			names = splitCsv(oltp.list)
		}
	}

	if len(names) == 0 && !flagIsSet(c, specFlag) {
		return fmt.Errorf("with no %s option expecting object and/or archived filenames in the command line", flprn(specFlag))
	}

	// output
	outFile = c.Args().Get(shift)
	var (
		discard      = discardOutput(outFile)
		outputFormat = archive.ExtTar
		w            io.Writer
	)
	if discard {
		w = io.Discard
	} else {
		outputFormat, err = archive.Strict("", cos.Ext(outFile))
		if err != nil {
			return err
		}
		// TODO -- FIXME: check existence; create parent dir(s); etc.
		wfh, err := cos.CreateFile(outFile)
		if err != nil {
			return err
		}
		defer wfh.Close()
		w = wfh
	}

	// spec
	if flagIsSet(c, specFlag) {
		specBytes, err := loadSpec(c)
		if err != nil {
			return err
		}
		if err := parseSpec(specBytes, &req); err != nil {
			return err
		}
	}

	// TODO -- FIXME: redundant vs spec: m.b. duplicating or conflicting
	req.OutputFormat = outputFormat
	req.ContinueOnErr = flagIsSet(c, continueOnErrorFlag)
	req.StreamingGet = flagIsSet(c, streamingGetFlag)
	req.OnlyObjName = !flagIsSet(c, inclSrcBucketNameFlag)

	{
		req.In = make([]apc.MossIn, 0, len(names))
		for _, o := range names {
			in := apc.MossIn{ObjName: o}
			if oname, archpath := splitPrefixShardBoundary(o); archpath != "" {
				in.ObjName = oname
				in.ArchPath = archpath
			}
			req.In = append(req.In, in)
		}
	}

	// do
	resp, err := api.GetBatch(apiBP, bck, &req, w)
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
	}
	return err
}
