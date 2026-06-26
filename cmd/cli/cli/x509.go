// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control and monitor TLS certificates.
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/urfave/cli"
)

// Props keys, as returned by certloader (cmn/certloader/certloader.go).
// A non-default loader contributes the same keys under its own prefix, e.g. "pub.error".
const (
	certKeyError   = "error"
	certKeyWarning = "warning"
)

var (
	// 'tls show' and 'show tls'
	showTLS = cli.Command{
		Name:         commandTLS,
		ArgsUsage:    optionalNodeIDArgument,
		Usage:        "Show TLS certificate's version, issuer's common name, from/to validity bounds",
		Action:       showCertHandler,
		BashComplete: suggestAllNodes,
	}
	loadTLS = cli.Command{
		Name:         cmdLoadTLS,
		Usage:        "Load TLS certificates (all configured certificates, on the selected node or cluster-wide)",
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

	// sorted: a node may report more than one certificate, and the per-loader
	// keys must not interleave (see certKeys)
	var nvs nvpairList
	for _, k := range certKeys(info) {
		nvs = append(nvs, nvpair{Name: k, Value: info[k]})
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

// TODO -- FIXME: revisit "all certificates are identical" and how/whether it applies to net.http.pub
//
// The invariant is inherited from the 4.x. There, identity
// across nodes is a requirement: the mesh authenticates node-to-node,
// and a node whose cert differs from the primary's is a real misconfiguration
// worth warning about.
//
// The user-facing (net.http.pub) certificate does not inherit that same requirement.
// Whether it must be identical cluster-wide depends on the deployment:
//
//   - single hostname / load balancer in front of the cluster: one shared pub
//     cert, and comparing across nodes is correct;
//   - nodes publicly addressed individually: pub certs legitimately differ by
//     CN/SAN, and comparing them warns on every node on every run.
//
// The second case is the one that does damage - not because the warning is
// wrong-but-harmless, but because an operator who learns to ignore this
// command's output stops reading the intra-cluster warnings mixed into it.
//
// Two ways out, to be decided when the pub loader is wired up:
//  1. declare pub certs cluster-uniform, document it, and keep comparing
//     everything (status quo below);
//  2. compare unprefixed (intra-cluster) keys only, and report per-loader
//     certs individually - expiration and validity per node, no cross-node
//     equality - which is what checkCertExpiration already does correctly.

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

// NOTE: compare the union of the two key sets (certificate configured on one node and not the other is a difference)
func compareCerts(c *cli.Context, info, i cos.StrKVs, pnode, snode *meta.Snode) int {
	for _, k := range certKeys(info, i) {
		v1, ok1 := info[k]
		v2, ok2 := i[k]
		if ok1 && ok2 && v1 == v2 {
			continue
		}
		warn := fmt.Sprintf("primary %s and node %s have different TLS certificates: (%s, %q) != (%s, %q)",
			pnode, snode, k, v1, k, v2)
		actionWarn(c, warn)
		return 1
	}
	return 0
}

// report every certificate the node has (not just the default)
// return the number of errors (and do not count warnings)
func checkCertExpiration(c *cli.Context, info cos.StrKVs, snode *meta.Snode) (cnt int) {
	for _, k := range certKeys(info) {
		switch {
		case isCertKey(k, certKeyError):
			warn := fmt.Sprintf("node %s certificate issue (%s): %s", snode, k, info[k])
			actionWarn(c, warn)
			cnt++
		case isCertKey(k, certKeyWarning):
			warn := fmt.Sprintf("node %s certificate warning (%s): %s", snode, k, info[k])
			actionWarn(c, warn)
		}
	}
	return cnt
}

// k is either the key itself (default loader) or "<prefix>.<key>"
func isCertKey(k, name string) bool {
	return k == name || strings.HasSuffix(k, "."+name)
}

// sorted union of the given key sets; sorting also groups each loader's
// prefixed keys together
func certKeys(kvs ...cos.StrKVs) []string {
	var n int
	for _, kv := range kvs {
		n += len(kv)
	}
	if n == 0 {
		return nil
	}
	seen := make(map[string]struct{}, n)
	keys := make([]string, 0, n)
	for _, kv := range kvs {
		for k := range kv {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}
