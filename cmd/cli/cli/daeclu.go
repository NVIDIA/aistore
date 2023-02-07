// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles cluster and daemon operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
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
func cluDaeStatus(c *cli.Context, smap *cluster.Smap, cfg *cmn.ClusterConfig, sid string) error {
	var (
		usejs      = flagIsSet(c, jsonFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
	)
	body := tmpls.StatusTemplateHelper{
		Smap:      smap,
		CluConfig: cfg,
		Status: tmpls.DaemonStatusTemplateHelper{
			Pmap: curPrxStatus,
			Tmap: curTgtStatus,
		},
	}
	if res, proxyOK := curPrxStatus[sid]; proxyOK {
		table := tmpls.NewDaeStatus(res, smap, apc.Proxy)
		out := table.Template(hideHeader)
		return tmpls.Print(res, c.App.Writer, out, nil, usejs)
	} else if res, targetOK := curTgtStatus[sid]; targetOK {
		table := tmpls.NewDaeStatus(res, smap, apc.Target)
		out := table.Template(hideHeader)
		return tmpls.Print(res, c.App.Writer, out, nil, usejs)
	} else if sid == apc.Proxy {
		table := tmpls.NewDaeMapStatus(&body.Status, smap, apc.Proxy)
		out := table.Template(hideHeader)
		return tmpls.Print(body, c.App.Writer, out, nil, usejs)
	} else if sid == apc.Target {
		table := tmpls.NewDaeMapStatus(&body.Status, smap, apc.Target)
		out := table.Template(hideHeader)
		return tmpls.Print(body, c.App.Writer, out, nil, usejs)
	} else if sid == "" {
		tableP := tmpls.NewDaeMapStatus(&body.Status, smap, apc.Proxy)
		tableT := tmpls.NewDaeMapStatus(&body.Status, smap, apc.Target)

		out := tableP.Template(false) + "\n"
		out += tableT.Template(false) + "\n"
		out += tmpls.ClusterSummary
		return tmpls.Print(body, c.App.Writer, out, nil, usejs)
	}

	return fmt.Errorf("expecting a valid NODE_ID or node type (\"proxy\" or \"target\"), got %q", sid)
}

func daemonDiskStats(c *cli.Context, sid string) error {
	var (
		usejs      = flagIsSet(c, jsonFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
	)
	setLongRunParams(c)

	if _, err := fillNodeStatusMap(c, apc.Target); err != nil {
		return err
	}

	targets := stats.DaemonStatusMap{sid: {}}
	if sid == "" {
		targets = curTgtStatus
	}

	diskStats, err := getDiskStats(targets)
	if err != nil {
		return err
	}

	if hideHeader {
		err = tmpls.Print(diskStats, c.App.Writer, tmpls.DiskStatNoHdrTmpl, nil, usejs)
	} else {
		err = tmpls.Print(diskStats, c.App.Writer, tmpls.DiskStatsTmpl, nil, usejs)
	}
	return err
}
