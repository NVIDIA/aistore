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
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
)

func getBMD(c *cli.Context) error {
	usejs := flagIsSet(c, jsonFlag)
	bmd, err := api.GetBMD(apiBP)
	if err != nil {
		return err
	}
	if usejs {
		return teb.Print(bmd, "", teb.Jopts(usejs))
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
						props.EC.ParitySlices, cos.ToSizeIEC(props.EC.ObjSizeLimit, 0))
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

func cluDaeStatus(c *cli.Context, smap *cluster.Smap, tstatusMap, pstatusMap stats.DaemonStatusMap, cfg *cmn.ClusterConfig, sid string) error {
	var (
		usejs       = flagIsSet(c, jsonFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}
	body := teb.StatusHelper{
		Smap:      smap,
		CluConfig: cfg,
		Status: teb.DaemonStatusHelper{
			Pmap: pstatusMap,
			Tmap: tstatusMap,
		},
	}
	if res, proxyOK := pstatusMap[sid]; proxyOK {
		table := teb.NewDaeStatus(res, smap, apc.Proxy, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	} else if res, targetOK := tstatusMap[sid]; targetOK {
		table := teb.NewDaeStatus(res, smap, apc.Target, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	} else if sid == apc.Proxy {
		table := teb.NewDaeMapStatus(&body.Status, smap, apc.Proxy, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	} else if sid == apc.Target {
		table := teb.NewDaeMapStatus(&body.Status, smap, apc.Target, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	} else if sid == "" {
		tableP := teb.NewDaeMapStatus(&body.Status, smap, apc.Proxy, units)
		tableT := teb.NewDaeMapStatus(&body.Status, smap, apc.Target, units)

		out := tableP.Template(false) + "\n"
		out += tableT.Template(false) + "\n"
		out += teb.ClusterSummary
		return teb.Print(body, out, teb.Jopts(usejs))
	}

	return fmt.Errorf("expecting a valid NODE_ID or node type (\"proxy\" or \"target\"), got %q", sid)
}

func daemonDiskStats(c *cli.Context, sid string) error {
	var (
		usejs       = flagIsSet(c, jsonFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}
	setLongRunParams(c)

	_, tstatusMap, _, err := fillNodeStatusMap(c, apc.Target)
	if err != nil {
		return err
	}

	targets := stats.DaemonStatusMap{sid: {}}
	if sid == "" {
		targets = tstatusMap
	}

	diskStats, err := getDiskStats(targets)
	if err != nil {
		return err
	}

	opts := teb.Opts{AltMap: teb.FuncMapUnits(units), UseJSON: usejs}
	if hideHeader {
		err = teb.Print(diskStats, teb.DiskStatNoHdrTmpl, opts)
	} else {
		err = teb.Print(diskStats, teb.DiskStatsTmpl, opts)
	}
	return err
}
