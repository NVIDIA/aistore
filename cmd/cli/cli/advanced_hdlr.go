// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides advanced commands that are useful for testing or development but not everyday use.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
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
		cmdGenShards: {
			cleanupFlag,
			concurrencyFlag,
			dsortFsizeFlag,
			dsortFcountFlag,
		},
	}

	advancedCmd = cli.Command{
		Name:  commandAdvanced,
		Usage: "special commands intended for development and advanced usage",
		Subcommands: []cli.Command{
			{
				Name: cmdGenShards,
				Usage: "generate and write random TAR shards, e.g.:\n" +
					indent4 + "\t- gen-shards 'ais://bucket1/shard-{001..999}.tar' - write 999 random shards (default sizes) to ais://bucket1\n" +
					indent4 + "\t- gen-shards 'gs://bucket2/shard-{01..20..2}.tgz' - 10 random gzipped tarfiles to Cloud bucket\n" +
					indent4 + "\t(notice quotation marks in both cases)",
				ArgsUsage: `"BUCKET/TEMPLATE.EXT"`,
				Flags:     advancedCmdsFlags[cmdGenShards],
				Action:    genShardsHandler,
			},
			jobStartResilver,
			{
				Name:         cmdPreload,
				Usage:        "preload object metadata into in-memory cache",
				ArgsUsage:    bucketArgument,
				Action:       loadLomCacheHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:         cmdRmSmap,
				Usage:        "immediately remove node from cluster map (advanced usage - potential data loss!)",
				ArgsUsage:    nodeIDArgument,
				Action:       removeNodeFromSmap,
				BashComplete: suggestAllNodes,
			},
			{
				Name:   cmdRandNode,
				Usage:  "print random node ID (by default, random target)",
				Action: randNode,
				BashComplete: func(c *cli.Context) {
					if c.NArg() == 0 {
						fmt.Println(apc.Proxy, apc.Target)
					}
				},
			},
		},
	}
)

func genShardsHandler(c *cli.Context) error {
	const usage = "gen-shards \"ais://mybucket/trunk-{000..099}-abc.tar\""
	var (
		fileCnt   = parseIntFlag(c, dsortFcountFlag)
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

	fileSize, err := parseSizeFlag(c, dsortFsizeFlag)
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
		progress = mpb.New(mpb.WithWidth(barWidth))
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

				putArgs := api.PutArgs{
					BaseParams: apiBP,
					Bck:        bck,
					ObjName:    name,
					Reader:     sgl,
					SkipVC:     true,
				}
				_, err := api.PutObject(putArgs)
				return err
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

func loadLomCacheHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() > 1 {
		return incorrectUsageMsg(c, "", c.Args()[1:])
	}
	bck, err := parseBckURI(c, c.Args().First(), true /*require provider*/)
	if err != nil {
		return err
	}
	return startXaction(c, apc.ActLoadLomCache, bck, "")
}

func removeNodeFromSmap(c *cli.Context) error {
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

func randNode(c *cli.Context) error {
	var (
		si        *cluster.Snode
		smap, err = getClusterMap(c)
	)
	if err != nil {
		return err
	}
	if c.NArg() > 0 && c.Args().Get(0) == apc.Proxy {
		si, err = smap.GetRandProxy(false) // _not_ excluding primary
	} else {
		si, err = smap.GetRandTarget()
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, si.ID())
	return nil
}
