// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles cluster and daemon operations.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

type (
	cluWarnNodes struct {
		alerts       []string
		warnings     []string
		maintenance  []string
		decommission []string
		rebalancing  []string
		totalNodes   int
	}
	cluAnalytics struct {
		// Performance metrics
		readRate, writeRate int64
		durationLabel       string

		// Error metrics
		totalDiskIOErrors     int64
		recentKeepAliveErrors bool

		// Latency metrics
		latencies []float64

		// Resource utilization
		cpuUsages  []float64
		diskUsages []float64

		// Storage metrics
		totalMountpaths   int
		healthyMountpaths int
		filesystemTypes   map[string]int
		mountpathWarnings []string

		// Network metrics
		totalOps int64

		// Cluster state
		nodeCount int
		tmap      teb.StstMap
		warnNodes *cluWarnNodes

		// Active jobs
		runningJobs []string
	}
)

func cluDaeStatus(c *cli.Context, smap *meta.Smap, tstatusMap, pstatusMap teb.StstMap, cfg *cmn.ClusterConfig, sid string, withRichAnalytics bool) error {
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
		Stst: teb.StatsAndStatusHelper{
			Pmap: pstatusMap,
			Tmap: tstatusMap,
		},
	}
	if res, ok := pstatusMap[sid]; ok {
		h := teb.StatsAndStatusHelper{Pmap: teb.StstMap{res.Snode.ID(): res}}
		table := h.MakeTabP(smap, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	}
	if res, ok := tstatusMap[sid]; ok {
		h := teb.StatsAndStatusHelper{Tmap: teb.StstMap{res.Snode.ID(): res}}
		table := h.MakeTabT(smap, units)
		out := table.Template(hideHeader)
		return teb.Print(res, out, teb.Jopts(usejs))
	}
	if sid == apc.Proxy {
		table := body.Stst.MakeTabP(smap, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	}
	if sid == apc.Target {
		table := body.Stst.MakeTabT(smap, units)
		out := table.Template(hideHeader)
		return teb.Print(body, out, teb.Jopts(usejs))
	}
	if sid != "" {
		return fmt.Errorf("expecting a valid NODE_ID or node type (\"proxy\" or \"target\"), got %q", sid)
	}

	//
	// `ais show cluster` (two tables and Summary)
	//
	tableP := body.Stst.MakeTabP(smap, units)
	tableT := body.Stst.MakeTabT(smap, units)

	// totals: num disks and capacity; software version and build time; backend detection
	body.NumDisks, body.Capacity = _totals(body.Stst.Tmap, units, cfg)
	body.Version, body.BuildTime = _clusoft(body.Stst.Tmap, body.Stst.Pmap)
	body.Backend = _detectBackend()
	body.Endpoint = getClusterEndpoint(gcfg)

	var out strings.Builder
	out.Grow(256)

	// For regular 'ais show cluster', include proxy and target tables
	// For 'ais show cluster summary', skip them
	if !withRichAnalytics {
		out.WriteString(tableP.Template(false))
		out.WriteString("\n")
		out.WriteString(tableT.Template(false))
		out.WriteString("\n")
	}

	// ais show cluster summary
	if withRichAnalytics && len(body.Stst.Tmap) > 0 {
		out.WriteString(_fmtStorageSummary(c, body.Stst.Tmap, units))
		out.WriteString("\n")

		// Add warning about refresh flag for better results
		if !flagIsSet(c, refreshFlag) {
			note := fmt.Sprintf("for more accurate performance results, use %s option and run several iterations\n", qflprn(refreshFlag))
			actionNote(c, note)
		}

		// Show detailed issues when verbose flag is set
		if flagIsSet(c, verboseFlag) {
			ca := newAnalytics(c, body.Stst.Tmap, units)
			out.WriteString("\n")
			out.WriteString(ca.warnNodes.GetDetailedStatus())
			out.WriteString("\n")
		}
	}

	// summary
	title := fblue("Cluster:")
	if isRebalancing(body.Stst.Tmap) {
		title = fcyan("Cluster:")
	}

	out.WriteString(title)
	out.WriteString("\n")
	out.WriteString(teb.ClusterSummary)
	return teb.Print(body, out.String(), teb.Jopts(usejs))
}

func _totals(tmap teb.StstMap, units string, cfg *cmn.ClusterConfig) (num int, cs string) {
	var used, avail int64
outer:
	for _, ds := range tmap {
		var (
			tcdf   = ds.Tcdf
			fsIDs  = make([]cos.FsID, 0, len(tcdf.Mountpaths))
			unique bool
		)
		for _, cdf := range tcdf.Mountpaths {
			fsIDs, unique = cos.AddUniqueFsID(fsIDs, cdf.FS.FsID)
			if !unique {
				continue
			}
			used += int64(cdf.Capacity.Used)
			avail += int64(cdf.Capacity.Avail)
			num += len(cdf.Disks)

			// [TODO]
			// - simplifying local-playground assumption and shortcut - won't work with loop devices, etc.
			// - ref: 152408
			if ds.DeploymentType == apc.DeploymentDev {
				break outer
			}
		}
	}
	if avail == 0 {
		debug.Assert(num == 0)
		return 0, ""
	}

	pctUsed := used * 100 / (used + avail)
	if pctUsed > 60 {
		// add precision
		fpct := math.Ceil(float64(used) * 100 / float64(used+avail))
		pctUsed = int64(fpct)
	}

	pct := fmt.Sprintf("%d%%", pctUsed)
	switch {
	case pctUsed >= cfg.Space.HighWM:
		pct = fred(pct)
	case pctUsed > cfg.Space.LowWM:
		pct = fcyan(pct)
	case pctUsed > cfg.Space.CleanupWM:
		pct = fblue(pct)
	default:
		pct = fgreen(pct)
	}
	cs = fmt.Sprintf("used %s (%s), available %s", teb.FmtSize(used, units, 2), pct, teb.FmtSize(avail, units, 2))

	return num, cs
}

func _clusoft(nodemaps ...teb.StstMap) (version, build string) {
	var multiver, multibuild bool
	for _, m := range nodemaps {
		for _, ds := range m {
			if !multiver {
				if version == "" {
					version = ds.Version
				} else if version != ds.Version {
					multiver = true
					version = ""
				}
			}
			if !multibuild {
				if build == "" {
					build = ds.BuildTime
				} else if build != ds.BuildTime {
					multibuild = true
					build = ""
				}
			}
		}
	}
	return version, build
}

// _getTrackerValue extracts values from tracker map
func _getTrackerValue(ds *stats.NodeStatus, key string) int64 {
	if val, ok := ds.Tracker[key]; ok {
		return val.Value
	}
	return 0
}

// _measureThroughput calculates actual throughput by taking two snapshots
func _measureThroughput(c *cli.Context, initialTmap teb.StstMap, duration time.Duration) (readRate, writeRate int64) {
	// Get initial byte counts
	initialRead, initialWrite := _getTotalBytes(initialTmap)

	time.Sleep(duration)

	_, currentTmap, _, err := fillStatusMapNoVersion(c, apc.Target)
	if err != nil {
		return 0, 0
	}

	finalRead, finalWrite := _getTotalBytes(currentTmap)

	// Calculate rates
	seconds := int64(duration.Seconds())
	if seconds > 0 {
		readRate = (finalRead - initialRead) / seconds
		writeRate = (finalWrite - initialWrite) / seconds
	}

	return readRate, writeRate
}

// sum up total bytes read/written across all nodes at that time
func _getTotalBytes(tmap teb.StstMap) (totalRead, totalWrite int64) {
	for _, ds := range tmap {
		totalRead += _getTrackerValue(ds, "get.size")
		totalWrite += _getTrackerValue(ds, "put.size")
	}
	return totalRead, totalWrite
}

// return true if there are any cluster issues
func (ci *cluWarnNodes) hasIssues() bool {
	return len(ci.alerts) > 0 || len(ci.warnings) > 0 || len(ci.maintenance) > 0 ||
		len(ci.decommission) > 0 || len(ci.rebalancing) > 0
}

func (ci *cluWarnNodes) summary() string {
	if !ci.hasIssues() {
		return fgreen("Operational")
	}

	var parts []string
	if l := len(ci.alerts); l > 0 {
		parts = append(parts, fmt.Sprintf("%d alert%s", l, cos.Plural(l)))
	}
	if l := len(ci.warnings); l > 0 {
		parts = append(parts, fmt.Sprintf("%d warning%s", l, cos.Plural(l)))
	}
	if l := len(ci.maintenance); l > 0 {
		parts = append(parts, fmt.Sprintf("%d in maintenance", l))
	}
	if l := len(ci.decommission); l > 0 {
		parts = append(parts, fmt.Sprintf("%d being decommissioned", l))
	}
	if l := len(ci.rebalancing); l > 0 {
		if l == ci.totalNodes {
			parts = append(parts, "cluster rebalancing")
		} else {
			parts = append(parts, fmt.Sprintf("%d node%s rebalancing", l, cos.Plural(l)))
		}
	}

	affectedNodes := make(map[string]bool, ci.totalNodes)
	for _, issues := range [][]string{ci.alerts, ci.warnings, ci.maintenance, ci.decommission, ci.rebalancing} {
		for _, node := range issues {
			affectedNodes[node] = true
		}
	}
	l := len(affectedNodes)
	return fmt.Sprintf("%d node%s report alerts and/or warnings: %v", l, cos.Plural(l), parts)
}

// GetDetailedStatus returns the full detailed breakdown (for dashboard --verbose)
func (ci *cluWarnNodes) GetDetailedStatus() string {
	if !ci.hasIssues() {
		return fgreen("All nodes operational")
	}

	var b strings.Builder
	b.Grow(256)
	b.WriteString(fblue("CLUSTER HEALTH DETAILS:"))
	b.WriteString("\n")

	addIssue := func(issueType string, colorFn func(...any) string, nodes []string) {
		if len(nodes) == 0 {
			return
		}
		nodeList := strings.Join(nodes, ", ")
		label := fmt.Sprintf("%s (%d/%d):", issueType, len(nodes), ci.totalNodes)
		b.WriteString(fmt.Sprintf("%-20s %s\n", colorFn(label), nodeList))
	}

	addIssue("Alerts", fred, ci.alerts)
	addIssue("Warnings", fcyan, ci.warnings)
	addIssue("Maintenance", fcyan, ci.maintenance)
	addIssue("Decommission", fred, ci.decommission)
	addIssue("Rebalancing", fcyan, ci.rebalancing)

	return b.String()
}

// _analyzeClusterHealth categorizes node states and returns detailed issue breakdown
func _analyzeClusterHealth(ca *cluAnalytics) *cluWarnNodes {
	ci := &cluWarnNodes{
		totalNodes: len(ca.tmap),
	}

	for _, ds := range ca.tmap {
		node := ds.Snode.StringEx()

		if ds.Snode.Flags.IsAnySet(meta.SnodeDecomm) {
			ci.decommission = append(ci.decommission, node)
		}
		if ds.Snode.Flags.IsAnySet(meta.SnodeMaint) {
			ci.maintenance = append(ci.maintenance, node)
		}
		if ds.Cluster.Flags.IsRed() {
			ci.alerts = append(ci.alerts, node)
		}
		if ds.Cluster.Flags.IsAnySet(cos.Rebalancing) {
			ci.rebalancing = append(ci.rebalancing, node)
		}
		if ds.Cluster.Flags.IsWarn() {
			ci.warnings = append(ci.warnings, node)
		}
	}

	return ci
}

func _calculateStats(values []float64) (minVal, maxVal, avg float64) {
	if len(values) == 0 {
		return
	}
	minVal, maxVal = values[0], values[0]
	var sum float64
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
		sum += v
	}
	avg = sum / float64(len(values))
	return
}

// newAnalytics collects all statistics from the target map
func newAnalytics(c *cli.Context, tmap teb.StstMap, _ string) *cluAnalytics {
	ca := &cluAnalytics{
		nodeCount:         len(tmap),
		tmap:              tmap,
		filesystemTypes:   make(map[string]int),
		mountpathWarnings: make([]string, 0, len(tmap)),
		cpuUsages:         make([]float64, 0, len(tmap)),
		diskUsages:        make([]float64, 0, len(tmap)),
	}

	ca.collectMetrics()
	ca.warnNodes = _analyzeClusterHealth(ca)
	ca.collectRunningJobs()
	ca.measureThroughput(c)
	return ca
}

// collectMetrics gathers all statistics from target nodes
func (ca *cluAnalytics) collectMetrics() {
	for _, ds := range ca.tmap {
		ca.collectErrorMetrics(ds)
		ca.collectLatencyMetrics(ds)
		ca.collectNetworkMetrics(ds)
		ca.collectResourceMetrics(ds)
		ca.collectStorageMetrics(ds)
	}
}

// collectErrorMetrics gathers error-related statistics
func (ca *cluAnalytics) collectErrorMetrics(ds *stats.NodeStatus) {
	if ds.Cluster.Flags&cos.KeepAliveErrors != 0 {
		ca.recentKeepAliveErrors = true
	}

	ca.totalDiskIOErrors += _getTrackerValue(ds, "err.io.get.n") +
		_getTrackerValue(ds, "err.io.put.n") +
		_getTrackerValue(ds, "err.io.del.n")
}

// collectLatencyMetrics gathers latency statistics
func (ca *cluAnalytics) collectLatencyMetrics(ds *stats.NodeStatus) {
	getCount := _getTrackerValue(ds, "get.n")
	putCount := _getTrackerValue(ds, "put.n")
	getLatency := _getTrackerValue(ds, "get.ns.total")
	putLatency := _getTrackerValue(ds, "put.ns.total")

	// Only calculate latency if there are operations
	if getCount > 0 {
		avgGetLatency := time.Duration(getLatency / getCount).Milliseconds()
		ca.latencies = append(ca.latencies, float64(avgGetLatency))
	}

	if putCount > 0 {
		avgPutLatency := time.Duration(putLatency / putCount).Milliseconds()
		ca.latencies = append(ca.latencies, float64(avgPutLatency))
	}
}

// collectNetworkMetrics gathers network-related statistics
func (ca *cluAnalytics) collectNetworkMetrics(ds *stats.NodeStatus) {
	ca.totalOps += _getTrackerValue(ds, "get.n")
	ca.totalOps += _getTrackerValue(ds, "put.n")
}

// collectResourceMetrics gathers CPU and resource utilization
func (ca *cluAnalytics) collectResourceMetrics(ds *stats.NodeStatus) {
	loadAvg := ds.MemCPUInfo.LoadAvg.One
	ca.cpuUsages = append(ca.cpuUsages, loadAvg)
}

// collectStorageMetrics gathers storage and filesystem statistics
func (ca *cluAnalytics) collectStorageMetrics(ds *stats.NodeStatus) {
	ca.totalMountpaths += len(ds.Tcdf.Mountpaths)
	diskUsages := ca.calculateNodeDiskUsage(ds)

	if len(diskUsages) > 0 {
		ca.diskUsages = append(ca.diskUsages, averageUsage(diskUsages))
	}
}

// calculateNodeDiskUsage calculates disk usage for a single node
func (ca *cluAnalytics) calculateNodeDiskUsage(ds *stats.NodeStatus) []float64 {
	var (
		nodediskUsages []float64
		fsIDs          []cos.FsID
		nodeHasWarning bool
	)

	for _, cdf := range ds.Tcdf.Mountpaths {
		if alert, _ := fs.HasAlert(cdf.Disks); alert == "" {
			ca.healthyMountpaths++
		} else {
			nodeHasWarning = true
		}
		ca.filesystemTypes[cdf.FS.FsType]++

		var unique bool
		fsIDs, unique = cos.AddUniqueFsID(fsIDs, cdf.FS.FsID)
		if unique && cdf.Capacity.PctUsed > 0 {
			nodediskUsages = append(nodediskUsages, float64(cdf.Capacity.PctUsed))
		}
	}

	if nodeHasWarning {
		ca.mountpathWarnings = append(ca.mountpathWarnings, ds.Snode.StringEx())
	}

	return nodediskUsages
}

// averageUsage calculates average from a slice of usage values
func averageUsage(usages []float64) float64 {
	var sum float64
	for _, usage := range usages {
		sum += usage
	}
	return sum / float64(len(usages))
}

// measureThroughput calculates current throughput
func (ca *cluAnalytics) measureThroughput(c *cli.Context) {
	if len(ca.tmap) == 0 {
		return
	}

	measurementDuration := time.Second
	if flagIsSet(c, refreshFlag) {
		measurementDuration = max(parseDurationFlag(c, refreshFlag), time.Second)
	}

	ca.readRate, ca.writeRate = _measureThroughput(c, ca.tmap, measurementDuration)
	ca.durationLabel = fmt.Sprintf("%.0fs avg", measurementDuration.Seconds())
}

// writeState writes cluster state information
func (ca *cluAnalytics) writeState(out *strings.Builder, isVerbose bool) {
	fmt.Fprintf(out, indent1+"State:\t\t%s\n", ca.warnNodes.summary())
	if ca.warnNodes.hasIssues() && !isVerbose {
		fmt.Fprintf(out, indent1+indent1+"Details:\t\tUse '--verbose' for detailed breakdown\n")
	}
}

// writeThroughput writes throughput information
func (ca *cluAnalytics) writeThroughput(out *strings.Builder, units string) {
	if ca.readRate > 0 || ca.writeRate > 0 {
		fmt.Fprintf(out, indent1+"Throughput:\t\tRead %s/s, Write %s/s (%s)\n",
			teb.FmtSize(ca.readRate, units, 1),
			teb.FmtSize(ca.writeRate, units, 1),
			ca.durationLabel)
	}
}

// writeErrors writes error information
func (ca *cluAnalytics) writeErrors(out *strings.Builder) {
	errorStr := fgreen("0")
	if ca.totalDiskIOErrors > 0 {
		errorStr = fred(strconv.FormatInt(ca.totalDiskIOErrors, 10))
	}
	fmt.Fprintf(out, indent1+"I/O Errors:\t\t%s\n", errorStr)
}

// writeResourceUtilization writes CPU and disk usage information
func (ca *cluAnalytics) writeResourceUtilization(out *strings.Builder) {
	if len(ca.cpuUsages) > 0 {
		minCPU, maxCPU, avgCPU := _calculateStats(ca.cpuUsages)
		fmt.Fprintf(out, indent1+"Load Avg:\t\tavg %.1f, min %.1f, max %.1f (1m)\n",
			avgCPU, minCPU, maxCPU)
	}
	if len(ca.diskUsages) > 0 {
		minDisk, maxDisk, avgDisk := _calculateStats(ca.diskUsages)
		fmt.Fprintf(out, indent1+"Disk Usage:\t\tavg %.1f%%, min %.1f%%, max %.1f%%\n",
			avgDisk, minDisk, maxDisk)
	}
}

// writeNetwork writes network health information
func (ca *cluAnalytics) writeNetwork(out *strings.Builder) {
	var networkStatus string
	switch {
	case ca.recentKeepAliveErrors:
		networkStatus = fred("keep-alive errors (last 5m)")
	case ca.totalOps > 0:
		networkStatus = fgreen("healthy")
	default:
		networkStatus = "cluster is idle"
	}
	fmt.Fprintf(out, indent1+"Network:\t\t%s\n", networkStatus)
}

// writeStorage writes storage information
func (ca *cluAnalytics) writeStorage(out *strings.Builder) {
	warnings := ca.totalMountpaths - ca.healthyMountpaths
	if warnings > 0 {
		fmt.Fprintf(out, indent1+"Storage:\t\t%d mountpaths (%d healthy, %d warnings)\n",
			ca.totalMountpaths, ca.healthyMountpaths, warnings)
		if len(ca.mountpathWarnings) > 0 {
			fmt.Fprintf(out, indent1+"   Warnings:\t\t%s\n", strings.Join(ca.mountpathWarnings, ", "))
		}
	} else {
		fmt.Fprintf(out, indent1+"Storage:\t\t%d mountpaths (all healthy)\n", ca.totalMountpaths)
	}
}

// writeFilesystems writes filesystem information
func (ca *cluAnalytics) writeFilesystems(out *strings.Builder) {
	if len(ca.filesystemTypes) == 0 {
		return
	}

	fsInfo := make([]string, 0, len(ca.filesystemTypes))
	for fsType, count := range ca.filesystemTypes {
		fsInfo = append(fsInfo, fmt.Sprintf("%s(%d)", fsType, count))
	}
	fmt.Fprintf(out, indent1+"Filesystems:\t\t%s\n", strings.Join(fsInfo, ", "))
}

func (ca *cluAnalytics) collectRunningJobs() {
	xargs := &xact.ArgsMsg{OnlyRunning: true}

	xs, err := api.QueryXactionSnaps(apiBP, xargs)
	if err != nil {
		return
	}

	// unique job types that are running
	jobTypes := cos.NewStrSet()
	for _, snaps := range xs {
		for _, snap := range snaps {
			jobTypes.Set(snap.Kind)
		}
	}
	ca.runningJobs = jobTypes.ToSlice()
}

// write active jobs information
func (ca *cluAnalytics) writeRunningJobs(out *strings.Builder) {
	if len(ca.runningJobs) == 0 {
		fmt.Fprintf(out, "%-24sNone\n", indent1+"Running Jobs:")
	} else {
		fmt.Fprintf(out, "%-24s%s\n", indent1+"Running Jobs:", strings.Join(ca.runningJobs, ", "))
	}
}

// details: storage, performance, network, error, and throughput
func _fmtStorageSummary(c *cli.Context, tmap teb.StstMap, units string) string {
	ca := newAnalytics(c, tmap, units)

	var out strings.Builder
	out.Grow(256)
	out.WriteString(fblue("Performance and Health:"))
	out.WriteString("\n")
	ca.writeState(&out, flagIsSet(c, verboseFlag))
	ca.writeThroughput(&out, units)
	ca.writeErrors(&out)
	ca.writeResourceUtilization(&out)
	ca.writeNetwork(&out)
	ca.writeStorage(&out)
	ca.writeFilesystems(&out)
	ca.writeRunningJobs(&out)

	return out.String()
}

// detect configured cloud backend providers
func _detectBackend() string {
	backends, err := api.GetConfiguredBackends(apiBP)
	if err != nil {
		return "N/A"
	}
	var cloudBackends []string
	for _, backend := range backends {
		if apc.IsCloudProvider(backend) {
			cloudBackends = append(cloudBackends, apc.DisplayProvider(backend))
		}
	}
	if len(cloudBackends) == 0 {
		return "None"
	}
	return strings.Join(cloudBackends, ", ")
}
