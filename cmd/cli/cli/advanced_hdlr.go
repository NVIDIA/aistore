// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides advanced commands that are useful for testing or development but not everyday use.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"golang.org/x/sync/errgroup"
)

var (
	advancedCmdsFlags = map[string][]cli.Flag{
		commandGenShards: {
			fileSizeFlag,
			fileCountFlag,
			cleanupFlag,
			concurrencyFlag,
		},
	}

	advancedCmd = cli.Command{
		Name:  commandAdvanced,
		Usage: "special commands intended for development and advanced usage",
		Subcommands: []cli.Command{
			{
				Name: commandGenShards,
				Usage: "generate and write random shards " +
					"(e.g.: \"ais://dsort-testing/shard-{001..999}.tar\" - generate 999 shards)",
				ArgsUsage: `"BUCKET/TEMPLATE.EXT"`,
				Flags:     advancedCmdsFlags[commandGenShards],
				Action:    genShardsHandler,
			},
			{
				Name:         apc.ActResilver,
				Usage:        "start resilvering objects across all drives on one or all targets",
				ArgsUsage:    optionalTargetIDArgument,
				Flags:        startCmdsFlags[subcmdXaction],
				Action:       startXactionHandler,
				BashComplete: suggestTargetNodes,
			},
			{
				Name:         subcmdPreload,
				Usage:        "preload object metadata into in-memory cache",
				ArgsUsage:    bucketArgument,
				Action:       loadLomCacheHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:         subcmdRmSmap,
				Usage:        "immediately remove node from cluster map (advanced usage - potential data loss)",
				ArgsUsage:    daemonIDArgument,
				Action:       removeNodeFromSmap,
				BashComplete: suggestAllNodes,
			},
		},
	}
)

func genShardsHandler(c *cli.Context) error {
	const usage = "gen-shards \"ais://mybucket/trunk-{000..099}-abc.tar\""
	var (
		fileCnt   = parseIntFlag(c, fileCountFlag)
		concLimit = parseIntFlag(c, concurrencyFlag)
	)

	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing bucket and template, e.g. usage: %s", usage)
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c,
			"too many arguments, make sure to use quotation marks to prevent BASH brace expansion, e.g.: %s", usage)
	}

	// Expecting: "ais://bucket/shard-{00..99}.tar"
	bck, object, err := parseBckObjectURI(c, c.Args()[0])
	if err != nil {
		return err
	}
	var (
		ext      = filepath.Ext(object)
		template = strings.TrimSuffix(object, ext)
	)

	fileSize, err := parseByteFlagToInt(c, fileSizeFlag)
	if err != nil {
		return err
	}

	supportedExts := []string{".tar", ".tgz"}
	if !cos.StringInSlice(ext, supportedExts) {
		return fmt.Errorf("extension %q is invalid, should be one of %q", ext, strings.Join(supportedExts, ", "))
	}

	mem, err := memsys.NewMMSA("dsort-cli")
	if err != nil {
		return err
	}

	pt, err := cos.ParseBashTemplate(template)
	if err != nil {
		return err
	}

	if err := setupBucket(c, bck); err != nil {
		return err
	}

	var (
		// Progress bar
		text     = "Shards created: "
		progress = mpb.New(mpb.WithWidth(progressBarWidth))
		bar      = progress.AddBar(
			pt.Count(),
			mpb.PrependDecorators(
				decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
				decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
		)

		concSemaphore = make(chan struct{}, concLimit)
		group, ctx    = errgroup.WithContext(context.Background())
		shardNum      = 0
	)
	pt.InitIter()

CreateShards:
	for shardName, hasNext := pt.Next(); hasNext; shardName, hasNext = pt.Next() {
		select {
		case concSemaphore <- struct{}{}:
		case <-ctx.Done():
			break CreateShards
		}

		group.Go(func(i int, name string) func() error {
			return func() error {
				defer func() {
					bar.Increment()
					<-concSemaphore
				}()

				name := fmt.Sprintf("%s%s", name, ext)
				sgl := mem.NewSGL(fileSize * int64(fileCnt))
				defer sgl.Free()

				if err := createTar(sgl, ext, i*fileCnt, (i+1)*fileCnt, fileCnt, fileSize); err != nil {
					return err
				}

				putArgs := api.PutObjectArgs{
					BaseParams: apiBP,
					Bck:        bck,
					Object:     name,
					Reader:     sgl,
					SkipVC:     true,
				}
				return api.PutObject(putArgs)
			}
		}(shardNum, shardName))
		shardNum++
	}

	if err := group.Wait(); err != nil {
		bar.Abort(true)
		return err
	}

	progress.Wait()
	return nil
}

func loadLomCacheHandler(c *cli.Context) (err error) {
	var bck cmn.Bck

	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}

	if bck, err = parseBckURI(c, c.Args().First(), true /*require provider*/); err != nil {
		return err
	}

	return startXaction(c, apc.ActLoadLomCache, bck, "")
}

func removeNodeFromSmap(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}

	sid, sname, err := getNodeIDName(c, c.Args().First())
	if err != nil {
		return err
	}
	smap, err := getClusterMap(c)
	debug.AssertNoErr(err)
	node := smap.GetNode(sid)
	debug.Assert(node != nil)
	if smap.IsPrimary(node) {
		return fmt.Errorf("%s is primary (cannot remove the primary node)", sname)
	}
	return api.RemoveNodeFromSmap(apiBP, sid)
}
