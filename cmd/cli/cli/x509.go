// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/urfave/cli"
)

var (
	// 'show log' and 'log show'
	showTLS = cli.Command{
		Name:         commandTLS,
		ArgsUsage:    optionalNodeIDArgument,
		Usage:        "show TLS certificate's version, issuer's common name, from/to validity bounds",
		Action:       showCertHandler,
		BashComplete: suggestAllNodes,
	}
	loadTLS = cli.Command{
		Name:         cmdLoadTLS,
		Usage:        "load TLS certificate",
		ArgsUsage:    optionalNodeIDArgument,
		Action:       loadCertHandler,
		BashComplete: suggestAllNodes,
	}

	// top-level
	tlsCmd = cli.Command{
		Name:  commandTLS,
		Usage: "load or reload (an updated) TLS certificate; display information about currently deployed certificates",
		Subcommands: []cli.Command{
			makeAlias(showTLS, "", true, commandShow),
			loadTLS,
		},
	}
)

func showCertHandler(c *cli.Context) error {
	var (
		sid             []string
		node, sname, er = arg0Node(c)
	)
	if er != nil {
		return er
	}
	if node != nil {
		sid = append(sid, node.ID())
	}

	info, err := api.GetX509Info(apiBP, sid...)
	if err != nil {
		return err
	}

	if node != nil {
		actionCptn(c, "TLS certificate from: ", sname)
	}

	var nvs nvpairList
	for k, v := range info {
		nvs = append(nvs, nvpair{Name: k, Value: v})
	}

	switch {
	case flagIsSet(c, noHeaderFlag):
		return teb.Print(nvs, teb.PropValTmplNoHdr)
	default:
		return teb.Print(nvs, teb.PropValTmpl)
	}
}

func loadCertHandler(c *cli.Context) (err error) {
	s := "Done."
	if c.NArg() == 0 {
		err = api.LoadX509Cert(apiBP, c.Args()...)
		s = "Done: all nodes."
	} else {
		err = api.LoadX509Cert(apiBP, meta.N2ID(c.Args().Get(0)))
	}
	if err == nil {
		actionDone(c, s)
	}
	return err
}
