// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides advanced commands that are useful for testing or development but not everyday use.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
				Name:      commandGenShards,
				Usage:     fmt.Sprintf("put randomly generated shards that can be used for %s testing", cmn.DSortName),
				ArgsUsage: `"BUCKET/TEMPLATE.EXT"`,
				Flags:     advancedCmdsFlags[commandGenShards],
				Action:    genShardsHandler,
			},
			{
				Name:         cmn.ActResilver,
				Usage:        "start resilvering objects across all drives on one or all targets",
				ArgsUsage:    optionalTargetIDArgument,
				Flags:        startCmdsFlags[subcmdStartXaction],
				Action:       startXactionHandler,
				BashComplete: daemonCompletions(completeTargets),
			},
			{
				Name:         subcmdPreload,
				Usage:        "preload object metadata into in-memory cache",
				ArgsUsage:    bucketArgument,
				Action:       loadLomCacheHandler,
				BashComplete: bucketCompletions(),
			},
			{
				Name:         subcmdRmSmap,
				Usage:        "immediately remove node from cluster map (advanced usage - potential data loss)",
				ArgsUsage:    daemonIDArgument,
				Action:       removeNodeFromSmap,
				BashComplete: daemonCompletions(completeAllDaemons),
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
		shardIt       = pt.Iter()
		shardNum      = 0
	)

CreateShards:
	for shardName, hasNext := shardIt(); hasNext; shardName, hasNext = shardIt() {
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
					BaseParams: defaultAPIParams,
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
		return incorrectUsageMsg(c, "missing bucket name")
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c, "too many arguments or unrecognized option '%+v'", c.Args()[1:])
	}

	if bck, err = parseBckURI(c, c.Args().First()); err != nil {
		return err
	}

	return startXaction(c, cmn.ActLoadLomCache, bck, "")
}

func removeNodeFromSmap(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing daemon ID")
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c, "too many arguments or unrecognized option '%+v'", c.Args()[1:])
	}
	daemonID := argDaemonID(c)
	smap, err := api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return err
	}
	node := smap.GetNode(daemonID)
	if node == nil {
		return fmt.Errorf("node %q does not exist (see 'ais show cluster')", daemonID)
	}
	if smap.IsPrimary(node) {
		return fmt.Errorf("node %s is primary: cannot remove", daemonID)
	}
	return api.RemoveNodeFromSmap(defaultAPIParams, daemonID)
}
