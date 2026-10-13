// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/urfave/cli"
)

// target placement weights (v5.2); see docs/placement.md

const (
	placementUsage      = "Show and set target placement weights"
	placementShowUsage  = "Show target placement weights and normalized shares"
	placementClearUsage = "Clear all target placement weights (starts global rebalance unless placement does not change)"

	placementWeightsArgument = "TARGET_ID=WEIGHT... [rest=WEIGHT] | rest=WEIGHT | none"
	placementRest            = "rest"
	placementNone            = apc.ResetToken
)

var placementSetUsage = "Set target placement weights (starts global rebalance unless placement does not change):\n" +
	indent1 + "\t- weights are positive integers, e.g. relative capacities (1, 3, 6) or sizes in GB (100, 300, 600);\n" +
	indent1 + "\t- rest=WEIGHT assigns that weight to all unlisted targets; explicit assignments take precedence;\n" +
	indent1 + "\t- unweighted cluster: specify a weight for every target or use rest=WEIGHT;\n" +
	indent1 + "\t- weighted cluster: unlisted targets keep their current weights unless rest=WEIGHT is given;\n" +
	indent1 + "\t- max/min ratio: warning at " + strconv.Itoa(meta.PlacementWeightWarnRatio) +
	"x, rejected at " + strconv.Itoa(meta.PlacementWeightMaxRatio) + "x;\n" +
	indent1 + "\t- TARGET_ID=none resets that target's weight to what a joining target would get (mean of the other weights);\n" +
	indent1 + "\t- 'none' alone, rest=none alone, or none for every target clears all weights - same as 'clear-weights';\n" +
	indent1 + "\t- requires all nodes to run the same software version;\n" +
	indent1 + "\t- e.g.: 'ais cluster placement set-weights t[abc]=123 rest=456';\n" +
	indent1 + "\t- e.g.: 'ais cluster placement set-weights t[abc]=3 t[def] 1 rest=2'\t- mixed forms;\n" +
	indent1 + "\t- e.g.: 'ais cluster placement set-weights t[abc]=none'\t- reset one weight;\n" +
	indent1 + "\t- e.g.: 'ais cluster placement set-weights none'\t- clear all weights.\n" +
	nvUsageRef

var showCmdPlacement = cli.Command{
	Name:   cmdPlacement,
	Usage:  placementShowUsage,
	Action: placementShowHandler,
}

var placementCmd = cli.Command{
	Name:   cmdPlacement,
	Usage:  placementUsage,
	Action: placementShowHandler, // 'show' is optional
	Subcommands: []cli.Command{
		makeAlias(&showCmdPlacement, &mkaliasOpts{newName: commandShow}),
		{
			Name:         cmdPlacementSet,
			Usage:        placementSetUsage,
			ArgsUsage:    placementWeightsArgument,
			Action:       placementSetHandler,
			BashComplete: placementWeightsCompletions,
		},
		{
			Name:   cmdPlacementClear,
			Usage:  placementClearUsage,
			Action: placementClearHandler,
		},
	},
}

func placementShowHandler(c *cli.Context) error {
	if c.NArg() > 0 {
		return incorrectUsageMsg(c, "unexpected argument %q", c.Args().Get(0))
	}
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return V(err)
	}
	if !smap.IsWeighted() {
		actionDone(c, "Placement: uniform (target weights are not set)")
		return nil
	}
	var (
		sum     float64
		first   int64
		uniform = true
	)
	for _, tsi := range smap.Tmap {
		w := tsi.PlacementWeight()
		if w == 0 {
			return fmt.Errorf("invalid placement weights: %s has weight 0 in a weighted cluster\n"+
				"(tip: use '%s %s %s %s %s=none' to reset this weight, or '%s %s %s %s' to clear all weights)",
				tsi.StringEx(), cliName, commandCluster, cmdPlacement, cmdPlacementSet, tsi.ID(),
				cliName, commandCluster, cmdPlacement, cmdPlacementClear)
		}
		if tsi.InMaintOrDecomm() {
			continue
		}
		sum += float64(w)
		if first == 0 {
			first = w
		} else if w != first {
			uniform = false
		}
	}
	if uniform {
		actionDone(c, "Placement: uniform (target weights are set and equal)")
	}
	tw := newTabWriter(c)
	fmt.Fprintln(tw, "TARGET\tWEIGHT\tSHARE")
	for _, tid := range slices.Sorted(maps.Keys(smap.Tmap)) {
		var (
			tsi   = smap.Tmap[tid]
			w     = tsi.PlacementWeight()
			share string
		)
		switch {
		case w == 0:
			share = "unassigned"
		case tsi.InMaintOrDecomm():
			share = "(maintenance)"
		case sum > 0:
			share = strconv.FormatFloat(float64(w)*100/sum, 'f', 1, 64) + "%"
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\n", tsi.StringEx(), w, share)
	}
	return tw.Flush()
}

func placementSetHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() == 1 && c.Args().Get(0) == placementNone {
		return clearPlacement(c)
	}
	nvs, err := placementPairs(c.Args())
	if err != nil {
		return err
	}
	if len(nvs) == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	var rest int64
	restVal, hasRest := nvs[placementRest]
	if hasRest {
		delete(nvs, placementRest)
		if restVal == placementNone {
			if len(nvs) > 0 {
				return fmt.Errorf("cannot combine %s=%s with other assignments: either all targets are weighted or none",
					placementRest, placementNone)
			}
			return clearPlacement(c)
		}
		if rest, err = parsePlacementWeight(restVal, placementRest); err != nil {
			return err
		}
	}
	// fresh Smap (never cached): the primary rejects stale snapshots
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return V(err)
	}
	var (
		weights = make(map[string]int64, len(smap.Tmap))
		orig    = make(map[string]int64, len(smap.Tmap))
		seen    = make(map[string]struct{}, len(nvs))
		resets  []string // TARGET_ID=none
		changed bool
	)
	for tid, tsi := range smap.Tmap {
		weights[tid] = tsi.PlacementWeight()
		orig[tid] = weights[tid]
	}
	for name, val := range nvs {
		tid := meta.N2ID(name)
		tsi := smap.GetTarget(tid)
		if tsi == nil {
			return &errDoesNotExist{
				what:   "target",
				name:   name,
				suffix: " (" + tabHelpOpt + ", check available targets by running 'ais show cluster target')",
			}
		}
		seen[tid] = struct{}{} // (duplicates rejected by placementPairs)
		if val == placementNone {
			resets = append(resets, tid)
			continue
		}
		w, err := parsePlacementWeight(val, tsi.StringEx())
		if err != nil {
			return err
		}
		weights[tid] = w
	}
	if hasRest {
		for tid := range weights {
			if _, explicit := seen[tid]; !explicit {
				weights[tid] = rest
			}
		}
	}
	if len(resets) > 0 {
		switch {
		case len(resets) == len(smap.Tmap):
			return clearPlacement(c) // none for every target
		case len(resets) == len(nvs) && !hasRest && !smap.IsWeighted():
			actionDone(c, "Placement weights are not set - nothing to do")
			return nil
		}
		if err := placementResets(smap, weights, resets); err != nil {
			return err
		}
	}
	for tid, w := range weights {
		if w != orig[tid] {
			changed = true
			break
		}
	}
	if !changed {
		actionDone(c, "Placement weights unchanged")
		return nil
	}
	// (after the no-op check, same as the primary)
	if len(smap.Tmap) < 2 {
		return errors.New("cannot set placement weights: cluster has a single target (weights require two or more)")
	}

	ratio, err := meta.CheckPlacementWeights(weights)
	if err != nil {
		if !smap.IsWeighted() && !hasRest && len(nvs) < len(smap.Tmap) {
			return fmt.Errorf("%v\n(cluster is not weighted: specify weights for all %d targets or use rest=WEIGHT)", err, len(smap.Tmap))
		}
		return err
	}
	if ratio >= meta.PlacementWeightWarnRatio {
		actionWarnf(c, "max/min placement weight ratio is %dx", ratio)
	}
	if n := smap.CountActiveTs(); n < 2 {
		actionWarnf(c, "%d of %d targets active - weights take effect when targets in maintenance rejoin", n, len(smap.Tmap))
	}
	return _setPlacement(c, smap, weights)
}

// assign each TARGET_ID=none the weight a joining target would get (see meta.Smap.MeanPlacementWeight)
func placementResets(smap *meta.Smap, weights map[string]int64, resets []string) error {
	for tid, w := range weights {
		smap.Tmap[tid].SetPlacementWeight(w)
	}
	for _, tid := range resets {
		smap.Tmap[tid].SetPlacementWeight(0) // exclude from the mean
	}
	mean := smap.MeanPlacementWeight()
	if mean == 0 {
		return fmt.Errorf("cannot reset %s=%s: no other target is weighted (to clear all weights, run '%s')",
			meta.Tname(resets[0]), placementNone, cliName+" "+commandCluster+" "+cmdPlacement+" "+cmdPlacementClear)
	}
	for _, tid := range resets {
		weights[tid] = mean
	}
	return nil
}

// parse name/value pairs; reject repeated target IDs and rest
func placementPairs(args []string) (cos.StrKVs, error) {
	pairs, err := parseNVPairs(args, nil)
	if err != nil {
		return nil, err
	}
	var (
		nvs   = make(cos.StrKVs, len(pairs))
		names = make(map[string]struct{}, len(pairs))
	)
	for _, pair := range pairs {
		id := meta.N2ID(pair.Name)
		if _, dup := names[id]; dup {
			return nil, fmt.Errorf("duplicate placement weight assignment for %q", pair.Name)
		}
		names[id] = struct{}{}
		nvs[pair.Name] = pair.Value
	}
	return nvs, nil
}

func parsePlacementWeight(val, name string) (int64, error) {
	w, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid placement weight %q for %s: expecting integer", val, name)
	}
	if w <= 0 {
		return 0, fmt.Errorf("invalid placement weight %d for %s: expecting positive integer (to clear all weights, run '%s')",
			w, name, cliName+" "+commandCluster+" "+cmdPlacement+" "+cmdPlacementClear)
	}
	return w, nil
}

func placementClearHandler(c *cli.Context) error {
	if c.NArg() > 0 {
		return incorrectUsageMsg(c, "unexpected argument %q", c.Args().Get(0))
	}
	return clearPlacement(c)
}

func clearPlacement(c *cli.Context) error {
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return V(err)
	}
	if !smap.IsWeighted() {
		actionDone(c, "Placement weights are not set - nothing to do")
		return nil
	}
	weights := make(map[string]int64, len(smap.Tmap))
	for tid := range smap.Tmap {
		weights[tid] = 0
	}
	return _setPlacement(c, smap, weights)
}

func _setPlacement(c *cli.Context, smap *meta.Smap, weights map[string]int64) error {
	for tid, w := range weights {
		smap.Tmap[tid].SetPlacementWeight(w)
	}
	xid, err := api.SetPlacementWeights(apiBP, smap)
	if err != nil {
		return V(err)
	}
	if xid != "" {
		fmt.Fprintf(c.App.Writer, fmtRebalanceStarted, xid)
		return nil
	}
	actionDone(c, "Placement weights updated (no rebalance required)")
	return nil
}
