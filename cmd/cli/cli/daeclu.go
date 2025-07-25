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

	"github.com/urfave/cli"
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

	var out strings.Builder

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
			warn := fmt.Sprintf("for better throughput results, use %s option and/or run several iterations\n", qflprn(refreshFlag))
			actionWarn(c, warn)
		}
	}

	// summary
	title := fblue("Summary:")
	if isRebalancing(body.Stst.Tmap) {
		title = fcyan("Summary:")
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

// _getTotalBytes sums up total bytes read/written across all nodes at that time
func _getTotalBytes(tmap teb.StstMap) (totalRead, totalWrite int64) {
	for _, ds := range tmap {
		totalRead += _getTrackerValue(ds, "get.size")
		totalWrite += _getTrackerValue(ds, "put.size")
	}
	return totalRead, totalWrite
}

// _determineClusterState analyzes node states and returns comprehensive cluster status
func _determineClusterState(ca *ClusterAnalytics) string {
	var (
		critical, maintenance, decommission, rebalancing, warnings []string
		totalNodes                                                 = len(ca.TMap)
	)

	for _, ds := range ca.TMap {
		nodeID := ds.Snode.StringEx()

		if ds.Snode.Flags.IsAnySet(meta.SnodeDecomm) {
			decommission = append(decommission, nodeID)
		}
		if ds.Snode.Flags.IsAnySet(meta.SnodeMaint) {
			maintenance = append(maintenance, nodeID)
		}
		if ds.Cluster.Flags.IsRed() {
			critical = append(critical, nodeID)
		}
		if ds.Cluster.Flags.IsAnySet(cos.Rebalancing) {
			rebalancing = append(rebalancing, nodeID)
		}
		if ds.Cluster.Flags.IsWarn() {
			warnings = append(warnings, nodeID)
		}
	}

	// Build status string with all active conditions
	var b strings.Builder

	addIssue := func(issueType string, colorFn func(...any) string, nodes []string) {
		if len(nodes) == 0 {
			return
		}
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		nodeList := strings.Join(nodes, ", ")
		label := fmt.Sprintf("- %s (%d/%d):", issueType, len(nodes), totalNodes)
		b.WriteString(fmt.Sprintf(indent1+"%-29s %s", colorFn(label), nodeList)) // 29 is the width of the label - formatting
	}

	if len(critical) > 0 || len(decommission) > 0 || len(maintenance) > 0 || len(rebalancing) > 0 || len(warnings) > 0 {
		b.WriteString("Multiple issues")
		addIssue("Critical", fred, critical)
		addIssue("Decommission", fred, decommission)
		addIssue("Maintenance", fcyan, maintenance)
		addIssue("Rebalancing", fcyan, rebalancing)
		addIssue("Warning", fcyan, warnings)
		return b.String()
	}

	return fgreen("Operational")
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

// ClusterAnalytics holds collected cluster statistics
type ClusterAnalytics struct {
	// Performance metrics
	ReadRate, WriteRate int64
	DurationLabel       string

	// Error metrics
	TotalDiskIOErrors     int64
	RecentKeepAliveErrors bool

	// Latency metrics
	Latencies []float64

	// Resource utilization
	CPUUsages  []float64
	DiskUsages []float64

	// Storage metrics
	TotalMountpaths, HealthyMountpaths int
	FilesystemTypes                    map[string]int
	MountpathWarnings                  []string

	// Network metrics
	TotalOps int64

	// Cluster state
	NodeCount int
	TMap      teb.StstMap
}

// newClusterAnalytics collects all statistics from the target map
func newClusterAnalytics(c *cli.Context, tmap teb.StstMap, _ string) *ClusterAnalytics {
	ca := &ClusterAnalytics{
		NodeCount:         len(tmap),
		TMap:              tmap,
		FilesystemTypes:   make(map[string]int),
		MountpathWarnings: make([]string, 0, len(tmap)),
		CPUUsages:         make([]float64, 0, len(tmap)),
		DiskUsages:        make([]float64, 0, len(tmap)),
	}

	ca.collectMetrics()
	ca.measureThroughput(c)
	return ca
}

// collectMetrics gathers all statistics from target nodes
func (ca *ClusterAnalytics) collectMetrics() {
	for _, ds := range ca.TMap {
		ca.collectErrorMetrics(ds)
		ca.collectLatencyMetrics(ds)
		ca.collectNetworkMetrics(ds)
		ca.collectResourceMetrics(ds)
		ca.collectStorageMetrics(ds)
	}
}

// collectErrorMetrics gathers error-related statistics
func (ca *ClusterAnalytics) collectErrorMetrics(ds *stats.NodeStatus) {
	if ds.Cluster.Flags&cos.KeepAliveErrors != 0 {
		ca.RecentKeepAliveErrors = true
	}

	ca.TotalDiskIOErrors += _getTrackerValue(ds, "err.io.get.n") +
		_getTrackerValue(ds, "err.io.put.n") +
		_getTrackerValue(ds, "err.io.del.n")
}

// collectLatencyMetrics gathers latency statistics
func (ca *ClusterAnalytics) collectLatencyMetrics(ds *stats.NodeStatus) {
	getCount := _getTrackerValue(ds, "get.n")
	putCount := _getTrackerValue(ds, "put.n")
	getLatency := _getTrackerValue(ds, "get.ns.total")
	putLatency := _getTrackerValue(ds, "put.ns.total")

	// Only calculate latency if there are operations
	if getCount > 0 {
		avgGetLatency := time.Duration(getLatency / getCount).Milliseconds()
		ca.Latencies = append(ca.Latencies, float64(avgGetLatency))
	}

	if putCount > 0 {
		avgPutLatency := time.Duration(putLatency / putCount).Milliseconds()
		ca.Latencies = append(ca.Latencies, float64(avgPutLatency))
	}
}

// collectNetworkMetrics gathers network-related statistics
func (ca *ClusterAnalytics) collectNetworkMetrics(ds *stats.NodeStatus) {
	ca.TotalOps += _getTrackerValue(ds, "get.n") + _getTrackerValue(ds, "put.n")
}

// collectResourceMetrics gathers CPU and resource utilization
func (ca *ClusterAnalytics) collectResourceMetrics(ds *stats.NodeStatus) {
	loadAvg := ds.MemCPUInfo.LoadAvg.One
	ca.CPUUsages = append(ca.CPUUsages, loadAvg)
}

// collectStorageMetrics gathers storage and filesystem statistics
func (ca *ClusterAnalytics) collectStorageMetrics(ds *stats.NodeStatus) {
	ca.TotalMountpaths += len(ds.Tcdf.Mountpaths)
	diskUsages := ca.calculateNodeDiskUsage(ds)

	if len(diskUsages) > 0 {
		ca.DiskUsages = append(ca.DiskUsages, ca.averageUsage(diskUsages))
	}
}

// calculateNodeDiskUsage calculates disk usage for a single node
func (ca *ClusterAnalytics) calculateNodeDiskUsage(ds *stats.NodeStatus) []float64 {
	var (
		nodeDiskUsages []float64
		fsIDs          []cos.FsID
		nodeHasWarning bool
	)

	for _, cdf := range ds.Tcdf.Mountpaths {
		if alert, _ := fs.HasAlert(cdf.Disks); alert == "" {
			ca.HealthyMountpaths++
		} else {
			nodeHasWarning = true
		}
		ca.FilesystemTypes[cdf.FS.FsType]++

		var unique bool
		fsIDs, unique = cos.AddUniqueFsID(fsIDs, cdf.FS.FsID)
		if unique && cdf.Capacity.PctUsed > 0 {
			nodeDiskUsages = append(nodeDiskUsages, float64(cdf.Capacity.PctUsed))
		}
	}

	if nodeHasWarning {
		ca.MountpathWarnings = append(ca.MountpathWarnings, ds.Snode.StringEx())
	}

	return nodeDiskUsages
}

// averageUsage calculates average from a slice of usage values
func (*ClusterAnalytics) averageUsage(usages []float64) float64 {
	var sum float64
	for _, usage := range usages {
		sum += usage
	}
	return sum / float64(len(usages))
}

// measureThroughput calculates current throughput
func (ca *ClusterAnalytics) measureThroughput(c *cli.Context) {
	if len(ca.TMap) == 0 {
		return
	}

	measurementDuration := time.Second
	if flagIsSet(c, refreshFlag) {
		measurementDuration = max(parseDurationFlag(c, refreshFlag), time.Second)
	}

	ca.ReadRate, ca.WriteRate = _measureThroughput(c, ca.TMap, measurementDuration)
	ca.DurationLabel = fmt.Sprintf("%.0fs avg", measurementDuration.Seconds())
}

// writeState writes cluster state information
func (ca *ClusterAnalytics) writeState(out *strings.Builder) {
	clusterState := _determineClusterState(ca)
	fmt.Fprintf(out, indent1+"State:\t\t%s\n", clusterState)
}

// writeThroughput writes throughput information
func (ca *ClusterAnalytics) writeThroughput(out *strings.Builder, units string) {
	if ca.ReadRate > 0 || ca.WriteRate > 0 {
		fmt.Fprintf(out, indent1+"Throughput:\t\tRead %s/s, Write %s/s (%s)\n",
			teb.FmtSize(ca.ReadRate, units, 1),
			teb.FmtSize(ca.WriteRate, units, 1),
			ca.DurationLabel)
	} else {
		fmt.Fprintf(out, indent1+"Throughput:\t\t%s\n", fcyan("idle"))
	}
}

// writeErrors writes error information
func (ca *ClusterAnalytics) writeErrors(out *strings.Builder) {
	if ca.TotalDiskIOErrors > 0 {
		fmt.Fprintf(out, indent1+"I/O Errors:\t\t%s\n", fred(strconv.FormatInt(ca.TotalDiskIOErrors, 10)))
	} else {
		fmt.Fprintf(out, indent1+"I/O Errors:\t\t%s\n", fgreen("0"))
	}
}

// writeResourceUtilization writes CPU and disk usage information
func (ca *ClusterAnalytics) writeResourceUtilization(out *strings.Builder) {
	if len(ca.CPUUsages) > 0 {
		minCPU, maxCPU, avgCPU := _calculateStats(ca.CPUUsages)
		fmt.Fprintf(out, indent1+"Load Avg:\t\tavg %.1f, min %.1f, max %.1f (1m)\n",
			avgCPU, minCPU, maxCPU)
	}

	if len(ca.DiskUsages) > 0 {
		minDisk, maxDisk, avgDisk := _calculateStats(ca.DiskUsages)
		fmt.Fprintf(out, indent1+"Disk Usage:\t\tavg %.1f%%, min %.1f%%, max %.1f%%\n",
			avgDisk, minDisk, maxDisk)
	}
}

// writeNetwork writes network health information
func (ca *ClusterAnalytics) writeNetwork(out *strings.Builder) {
	var networkStatus string
	switch {
	case ca.RecentKeepAliveErrors:
		networkStatus = fred("keep-alive errors (last 5m)")
	case ca.TotalOps > 0:
		networkStatus = fgreen("healthy")
	default:
		networkStatus = fcyan("idle")
	}
	fmt.Fprintf(out, indent1+"Network:\t\t%s\n", networkStatus)
}

// writeStorage writes storage information
func (ca *ClusterAnalytics) writeStorage(out *strings.Builder) {
	warnings := ca.TotalMountpaths - ca.HealthyMountpaths
	if warnings > 0 {
		fmt.Fprintf(out, indent1+"Storage:\t\t%d mountpaths (%d healthy, %d warnings)\n",
			ca.TotalMountpaths, ca.HealthyMountpaths, warnings)
		if len(ca.MountpathWarnings) > 0 {
			fmt.Fprintf(out, indent1+"   Warnings:\t\t%s\n", strings.Join(ca.MountpathWarnings, ", "))
		}
	} else {
		fmt.Fprintf(out, indent1+"Storage:\t\t%d mountpaths (all healthy)\n", ca.TotalMountpaths)
	}
}

// writeFilesystems writes filesystem information
func (ca *ClusterAnalytics) writeFilesystems(out *strings.Builder) {
	if len(ca.FilesystemTypes) == 0 {
		return
	}

	fsInfo := make([]string, 0, len(ca.FilesystemTypes))
	for fsType, count := range ca.FilesystemTypes {
		fsInfo = append(fsInfo, fmt.Sprintf("%s(%d)", fsType, count))
	}
	fmt.Fprintf(out, indent1+"Filesystems:\t\t%s\n", strings.Join(fsInfo, ", "))
}

// _fmtStorageSummary provides enhanced storage, performance, network, error, and throughput details
func _fmtStorageSummary(c *cli.Context, tmap teb.StstMap, units string) string {
	ca := newClusterAnalytics(c, tmap, units)

	var out strings.Builder
	out.WriteString(fblue("Performance and Health:"))
	out.WriteString("\n")

	ca.writeState(&out)
	ca.writeThroughput(&out, units)
	ca.writeErrors(&out)
	ca.writeResourceUtilization(&out)
	ca.writeNetwork(&out)
	ca.writeStorage(&out)
	ca.writeFilesystems(&out)

	return out.String()
}

// _detectBackend detects the configured cloud backend providers
func _detectBackend() string {
	backends, err := api.GetConfiguredBackends(apiBP)
	if err != nil {
		return "N/A"
	}

	cloudBackends := []string{}
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
