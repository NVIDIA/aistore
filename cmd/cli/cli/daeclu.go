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
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

func getBMD(c *cli.Context) error {
	usejs := flagIsSet(c, jsonFlag)
	bmd, err := api.GetBMD(apiBP)
	if err != nil {
		return V(err)
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

func cluDaeStatus(c *cli.Context, smap *meta.Smap, tstatusMap, pstatusMap teb.StstMap,
	cfg *cmn.ClusterConfig, sid string) error {
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
		Status: teb.StatsAndStatusHelper{
			Pmap: pstatusMap,
			Tmap: tstatusMap,
		},
	}
	if res, ok := pstatusMap[sid]; ok {
		table := teb.NewDaeStatus(res, smap, apc.Proxy, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	}
	if res, ok := tstatusMap[sid]; ok {
		table := teb.NewDaeStatus(res, smap, apc.Target, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	}
	if sid == apc.Proxy {
		table := teb.NewDaeMapStatus(&body.Status, smap, apc.Proxy, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	}
	if sid == apc.Target {
		table := teb.NewDaeMapStatus(&body.Status, smap, apc.Target, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	}
	// `ais show cluster`
	if sid == "" {
		tableP := teb.NewDaeMapStatus(&body.Status, smap, apc.Proxy, units)
		tableT := teb.NewDaeMapStatus(&body.Status, smap, apc.Target, units)

		out := tableP.Template(false) + "\n"
		out += tableT.Template(false) + "\n"

		// summary
		title := fgreen("Summary:")
		if isRebalancing(body.Status.Tmap) {
			title = fcyan("Summary:")
		}
		out += title + "\n" + teb.ClusterSummary
		return teb.Print(body, out, teb.Jopts(usejs))
	}

	return fmt.Errorf("expecting a valid NODE_ID or node type (\"proxy\" or \"target\"), got %q", sid)
}
