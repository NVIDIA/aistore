// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/urfave/cli"
)

var (
	// 'show log' and 'log show'
	showTLS = cli.Command{
		Name:         commandTLS,
		ArgsUsage:    optionalNodeIDArgument,
		Usage:        "Show TLS certificate's version, issuer's common name, from/to validity bounds",
		Action:       showCertHandler,
		BashComplete: suggestAllNodes,
	}
	loadTLS = cli.Command{
		Name:         cmdLoadTLS,
		Usage:        "Load TLS certificate",
		ArgsUsage:    optionalNodeIDArgument,
		Action:       loadCertHandler,
		BashComplete: suggestAllNodes,
	}
	validateTLS = cli.Command{
		Name:      cmdValidateTLS,
		Usage:     "Check that all TLS certificates are identical",
		ArgsUsage: optionalNodeIDArgument,
		Action:    validateCertHandler,
	}

	// top-level
	tlsCmd = cli.Command{
		Name:  commandTLS,
		Usage: "Load or reload (an updated) TLS certificate; display information about currently deployed certificates",
		Subcommands: []cli.Command{
			makeAlias(&showTLS, &mkaliasOpts{newName: commandShow}),
			loadTLS,
			validateTLS,
		},
	}
)

func showCertHandler(c *cli.Context) error {
	var (
		sid            []string
		node, sname, e = arg0Node(c)
	)
	if e != nil {
		return e
	}
	if node != nil {
		sid = append(sid, node.ID())
	}

	info, err := api.GetX509Info(apiBP, sid...)
	if err != nil {
		return err
	}

	if node != nil {
		actionCptn(c, "TLS certificate from:", sname)
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
	s := "Done: "
	if c.NArg() == 0 {
		err = api.LoadX509Cert(apiBP, c.Args()...)
		s += "all nodes."
	} else {
		node, sname, e := arg0Node(c)
		if e != nil {
			return e
		}
		s += sname
		err = api.LoadX509Cert(apiBP, node.ID())
	}
	if err == nil {
		actionDone(c, s)
	}
	return err
}

func validateCertHandler(c *cli.Context) error {
	smap, err := getClusterMap(c)
	if err != nil {
		return err
	}

	var (
		sid     = make([]string, 1)
		info, i cos.StrKVs
		cnt     int
	)
	sid[0] = smap.Primary.ID()
	info, err = api.GetX509Info(apiBP, sid...)
	if err != nil {
		return V(err)
	}
	cnt += checkCertExpiration(c, info, smap.Primary)
	for pid, snode := range smap.Pmap {
		if pid == smap.Primary.ID() {
			continue
		}
		sid[0] = pid
		i, err = api.GetX509Info(apiBP, sid...)
		if err != nil {
			actionWarn(c, fmt.Sprintf("%s returned error: %v", snode, V(err)))
			continue
		}
		cnt += checkCertExpiration(c, i, snode)
		cnt += compareCerts(c, info, i, smap.Primary, snode)
	}
	for tid, snode := range smap.Tmap {
		sid[0] = tid
		i, err = api.GetX509Info(apiBP, sid...)
		if err != nil {
			actionWarn(c, fmt.Sprintf("%s returned error: %v", snode, V(err)))
			continue
		}
		cnt += checkCertExpiration(c, i, snode)
		cnt += compareCerts(c, info, i, smap.Primary, snode)
	}

	if cnt == 0 {
		actionDone(c, "Done: all TLS certificates are identical and valid")
	} else if cnt > 1 {
		warn := fmt.Sprintf("\n==== %d differences overall ====", cnt)
		actionWarn(c, warn)
	}
	return nil
}

func compareCerts(c *cli.Context, info, i cos.StrKVs, pnode, snode *meta.Snode) int {
	for k, v1 := range info {
		v2, ok := i[k]
		if ok && v1 == v2 {
			continue
		}
		warn := fmt.Sprintf("primary %s and node %s have different TLS certificates: (%s, %q) != (%s, %q)",
			pnode, snode, k, v1, k, v2)
		actionWarn(c, warn)
		return 1
	}
	return 0
}

func checkCertExpiration(c *cli.Context, info cos.StrKVs, snode *meta.Snode) int {
	if errorMsg, hasError := info["error"]; hasError {
		warn := fmt.Sprintf("node %s certificate issue: %s", snode, errorMsg)
		actionWarn(c, warn)
		return 1
	}

	if warning, hasWarning := info["warning"]; hasWarning {
		warn := fmt.Sprintf("node %s certificate warning: %s", snode, warning)
		actionWarn(c, warn)
	}
	return 0
}
