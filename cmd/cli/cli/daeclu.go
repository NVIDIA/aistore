// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles cluster and daemon operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"text/tabwriter"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
)

type (
	targetDiskStats struct {
		targetID string
		stats    ios.AllDiskStats
	}

	targetRebSnap struct {
		tid  string
		snap *cluster.Snap
	}
)

func getBMD(c *cli.Context) error {
	usejs := flagIsSet(c, jsonFlag)
	bmd, err := api.GetBMD(apiBP)
	if err != nil {
		return err
	}
	if usejs {
		return tmpls.Print(bmd, c.App.Writer, "", nil, usejs)
	}

	tw := &tabwriter.Writer{}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)
	if !flagIsSet(c, noHeaderFlag) {
		fmt.Fprintln(tw, "PROVIDER\tNAMESPACE\tNAME\tBACKEND\tCOPIES\tEC(D/P, minsize)\tCREATED")
	}
	for provider, namespaces := range bmd.Providers {
		for nsUname, buckets := range namespaces {
			ns := cmn.ParseNsUname(nsUname)
			for bucket, props := range buckets {
				var copies, ec string
				if props.Mirror.Enabled {
					copies = strconv.Itoa(int(props.Mirror.Copies))
				}
				if props.EC.Enabled {
					ec = fmt.Sprintf("%d/%d, %s", props.EC.DataSlices,
						props.EC.ParitySlices, cos.B2S(props.EC.ObjSizeLimit, 0))
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					provider, ns, bucket, props.BackendBck, copies, ec,
					cos.FormatNanoTime(props.Created, ""))
			}
		}
	}
	tw.Flush()
	fmt.Fprintln(c.App.Writer)
	fmt.Fprintf(c.App.Writer, "Version:\t%d\n", bmd.Version)
	fmt.Fprintf(c.App.Writer, "UUID:\t\t%s\n", bmd.UUID)
	return nil
}

// Displays the status of the cluster or a node
func cluDaeStatus(c *cli.Context, smap *cluster.Smap, cfg *cmn.ClusterConfig, sid string, usejs, hideHeader bool) error {
	body := tmpls.StatusTemplateHelper{
		Smap:      smap,
		CluConfig: cfg,
		Status: tmpls.DaemonStatusTemplateHelper{
			Pmap: curPrxStatus,
			Tmap: curTgtStatus,
		},
	}
	if res, proxyOK := curPrxStatus[sid]; proxyOK {
		return tmpls.Print(res, c.App.Writer, tmpls.NewProxyTable(res, smap).Template(hideHeader), nil, usejs)
	} else if res, targetOK := curTgtStatus[sid]; targetOK {
		return tmpls.Print(res, c.App.Writer, tmpls.NewTargetTable(res).Template(hideHeader), nil, usejs)
	} else if sid == apc.Proxy {
		template := tmpls.NewProxiesTable(&body.Status, smap).Template(hideHeader)
		return tmpls.Print(body, c.App.Writer, template, nil, usejs)
	} else if sid == apc.Target {
		return tmpls.Print(body, c.App.Writer,
			tmpls.NewTargetsTable(&body.Status).Template(hideHeader), nil, usejs)
	} else if sid == "" {
		template := tmpls.NewProxiesTable(&body.Status, smap).Template(false) + "\n" +
			tmpls.NewTargetsTable(&body.Status).Template(false) + "\n" +
			tmpls.ClusterSummary
		return tmpls.Print(body, c.App.Writer, template, nil, usejs)
	}
	return fmt.Errorf("%s is not a valid NODE_ID nor NODE_TYPE", sid)
}

func daemonDiskStats(c *cli.Context, sid string) error {
	var (
		usejs      = flagIsSet(c, jsonFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
	)
	if _, err := fillNodeStatusMap(c); err != nil {
		return err
	}
	if _, ok := curPrxStatus[sid]; ok {
		return fmt.Errorf("node %q is a proxy (hint: \"%s %s %s\" works only for targets)",
			sid, cliName, commandShow, subcmdShowDisk)
	}
	if _, ok := curTgtStatus[sid]; sid != "" && !ok {
		return fmt.Errorf("target ID=%q does not exist", sid)
	}

	setLongRunParams(c)

	targets := stats.DaemonStatusMap{sid: {}}
	if sid == "" {
		targets = curTgtStatus
	}

	diskStats, err := getDiskStats(targets)
	if err != nil {
		return err
	}

	if hideHeader {
		err = tmpls.Print(diskStats, c.App.Writer, tmpls.DiskStatTmpl, nil, usejs)
	} else {
		err = tmpls.Print(diskStats, c.App.Writer, tmpls.DiskStatsFullTmpl, nil, usejs)
	}
	if err != nil {
		return err
	}

	return nil
}

func getDiskStats(targets stats.DaemonStatusMap) ([]tmpls.DiskStatsTemplateHelper, error) {
	var (
		allStats = make([]tmpls.DiskStatsTemplateHelper, 0, len(targets))
		wg, _    = errgroup.WithContext(context.Background())
		statsCh  = make(chan targetDiskStats, len(targets))
	)

	for targetID := range targets {
		wg.Go(func(targetID string) func() error {
			return func() (err error) {
				diskStats, err := api.GetTargetDiskStats(apiBP, targetID)
				if err != nil {
					return err
				}

				statsCh <- targetDiskStats{stats: diskStats, targetID: targetID}
				return nil
			}
		}(targetID))
	}

	err := wg.Wait()
	close(statsCh)
	if err != nil {
		return nil, err
	}
	for diskStats := range statsCh {
		targetID := diskStats.targetID
		for diskName, diskStat := range diskStats.stats {
			allStats = append(allStats,
				tmpls.DiskStatsTemplateHelper{TargetID: targetID, DiskName: diskName, Stat: diskStat})
		}
	}

	sort.Slice(allStats, func(i, j int) bool {
		if allStats[i].TargetID != allStats[j].TargetID {
			return allStats[i].TargetID < allStats[j].TargetID
		}
		if allStats[i].DiskName != allStats[j].DiskName {
			return allStats[i].DiskName < allStats[j].DiskName
		}
		return allStats[i].Stat.Util > allStats[j].Stat.Util
	})

	return allStats, nil
}
