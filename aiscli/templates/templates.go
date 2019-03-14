// Package templates provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package templates

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"text/template"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

// Templates for output
// ** Changing the structure of the objects server side needs to make sure that this will still work **
const (
	// List output
	ListHeader = "\nDaemonID\t Type\t SmapVersion\t Uptime(µs)\t "
	ListBody   = "{{$value.Snode.DaemonID}}\t {{$value.Snode.DaemonType}}\t {{$value.SmapVersion}}\t {{ExtractStat $value.Stats `up.µs.time`}}\t "

	ListTmpl        = ListHeader + "\n{{ range $key, $value := . }}" + ListBody + "\n{{end}}\n"
	ListTmplVerbose = ListHeader + "PublicURL\t IntraControlURL\t IntraDataURL\n" +
		"{{ range $key, $value := . }}" + ListBody +
		"{{$value.Snode.PublicNet.DirectURL}}\t {{$value.Snode.IntraControlNet.DirectURL}}\t {{$value.Snode.IntraDataNet.DirectURL}}\n" +
		"{{end}}\n"

	// Smap
	SmapHeader = "\nDaemonID\t Type\t PublicURL\t IntraControlURL\t IntraDataURL\n"
	SmapBody   = "{{$value.DaemonID}}\t {{$value.DaemonType}}\t {{$value.PublicNet.DirectURL}}\t " +
		"{{$value.IntraControlNet.DirectURL}}\t {{$value.IntraDataNet.DirectURL}}\n"

	SmapTmpl = SmapHeader +
		"{{ range $key, $value := .Pmap }}" + SmapBody + "{{end}}" +
		SmapHeader +
		"{{ range $key, $value := .Tmap }}" + SmapBody + "{{end}}\n" +
		"Non-Electable:\n" +
		"{{ range $key, $val := .NonElects }}Key: {{$key}}\t Value: {{$val}}\n {{end}}\n" +
		"PrimaryProxy: {{.ProxySI.DaemonID}}\t Proxies: {{len .Pmap}}\t Targets: {{len .Tmap}}\t Smap Version: {{.Version}}\n"

	// Proxy Info
	ProxyInfoHeader = "\nDaemonID\t Type\t MemUsed(%)\t CpuUsed(%)\t MemAvail\t Uptime(µs)\n"
	ProxyInfoBody   = "{{$value.Snode.DaemonID}}\t {{$value.Snode.DaemonType}}\t {{$value.SysInfo.PctMemUsed | printf `%0.5f`}}\t " +
		"{{$value.SysInfo.PctCPUUsed | printf `%0.2f`}}\t {{FormatBytesUnsigned $value.SysInfo.MemAvail 5}}\t " +
		"{{ExtractStat $value.Stats `up.µs.time`}}\n"

	ProxyInfoTmpl       = ProxyInfoHeader + "{{ range $key, $value := . }}" + ProxyInfoBody + "{{end}}\n"
	ProxyInfoSingleTmpl = ProxyInfoHeader + "{{$value := . }}" + ProxyInfoBody

	// Target Info
	TargetInfoHeader = "\nDaemonID\t Type\t MemUsed(%)\t CpuUsed(%)\t CapacityUsed(%)\t MemAvail\t CapacityAvail\t Throughput\n"
	TargetInfoBody   = "{{$value.Snode.DaemonID}}\t {{$value.Snode.DaemonType}}\t " +
		"{{$value.SysInfo.PctMemUsed | printf `%0.5f`}}\t {{$value.SysInfo.PctCPUUsed | printf `%0.2f`}}\t " +
		"{{CalcAvg $value `percent` | printf `%d`}}\t {{FormatBytesUnsigned $value.SysInfo.MemAvail 5}}\t " +
		"{{$capacity := CalcAvg $value `capacity`}}{{FormatBytesUnsigned $capacity 5}}\t " +
		"{{$statVal := ExtractStat $value.Stats `get.bps` }}{{FormatBytesSigned $statVal 2}}\n"

	TargetInfoTmpl       = TargetInfoHeader + "{{ range $key, $value := . }}" + TargetInfoBody + "{{end}}\n"
	TargetInfoSingleTmpl = TargetInfoHeader + "{{$value := . }}" + TargetInfoBody

	// Stats
	StatsHeader = "{{$obj := . }}\nDaemonID: {{ .Snode.DaemonID }}\t Type: {{ .Snode.DaemonType }}\n\nStats\n"
	StatsBody   = "{{range $key, $val := $obj.Stats.Tracker }}" +
		"{{$statVal := ExtractStat $obj.Stats $key}}" +
		"{{if (eq $statVal 0)}}{{else}}{{$key}}\t{{$statVal}}\n{{end}}" +
		"{{end}}\n"

	ProxyStatsTmpl  = StatsHeader + StatsBody
	TargetStatsTmpl = StatsHeader + StatsBody +
		"Mountpaths\t CapacityUsed(%)\t CapacityAvail\n" +
		"{{range $key, $val := $obj.Capacity}}" +
		"{{$key}}\t {{$val.Usedpct | printf `%0.2d`}}\t {{FormatBytesUnsigned $val.Avail 5}}\n" +
		"{{end}}\n"

	StatsTmpl = "{{$obj := .Proxy }}\nProxy Stats\n" +
		"{{range $key, $ := $obj.Tracker }}" +
		"{{$statVal := ExtractStat $obj $key}}" +
		"{{if (eq $statVal 0)}}{{else}}{{$key}}\t{{$statVal}}\n{{end}}" +
		"{{end}}\n" +
		"{{range $key, $val := .Target }}" +
		"Target: {{$key}}\n" +
		"{{range $statKey, $ := $val.Core.Tracker}}" +
		"{{$statVal := ExtractStat $val.Core $statKey}}" +
		"{{if (eq $statVal 0)}}{{else}}{{$statKey}}\t{{$statVal}}\n{{end}}" +
		"{{end}}\n" +
		"Mountpaths\t CapacityUsed(%)\t CapacityAvail\n" +
		"{{range $mount, $capa := $val.Capacity}}" +
		"{{$mount}}\t {{$capa.Usedpct | printf `%0.2d`}}\t {{FormatBytesUnsigned $capa.Avail 5}}\n" +
		"{{end}}\n\n" +
		"{{end}}"

	// Config
	MirrorConfTmpl = "\n{{$obj := .Mirror}}Mirror Config\n" +
		" Copies: {{$obj.Copies}}\n" +
		" Burst: {{$obj.Burst}}\n" +
		" UtilThresh: {{$obj.UtilThresh}}\n" +
		" OptimizePUT: {{$obj.OptimizePUT}}\n" +
		" Enabled: {{$obj.Enabled}}\n"
	ReadaheadConfTmpl = "\n{{$obj := .Readahead}}Readahead Config\n" +
		" ObjectMem: {{$obj.ObjectMem}}\n" +
		" TotalMem: {{$obj.TotalMem}}\n" +
		" ByProxy: {{$obj.ByProxy}}\n" +
		" Discard: {{$obj.Discard}}\n" +
		" Enabled: {{$obj.Discard}}\n"
	LogConfTmpl = "\n{{$obj := .Log}}Log Config\n" +
		" Dir: {{$obj.Dir}}\n" +
		" Level: {{$obj.Level}}\n" +
		" MaxSize: {{$obj.MaxSize}}\n" +
		" MaxTotal: {{$obj.MaxTotal}}\n"
	PeriodConfTmpl = "\n{{$obj := .Periodic}}Period Config\n" +
		" Stats Time: {{$obj.StatsTimeStr}}\n" +
		" IO Stats Time: {{$obj.IostatTimeStr}}\n" +
		" Retry Sync Time: {{$obj.RetrySyncTimeStr}}\n"
	TimeoutConfTmpl = "\n{{$obj := .Timeout}}Timeout Config\n" +
		" Default Timout: {{$obj.DefaultStr}}\n" +
		" Default Long Timeout: {{$obj.DefaultLongStr}}\n" +
		" Max Keep Alive: {{$obj.MaxKeepaliveStr}}\n" +
		" Proxy Ping: {{$obj.ProxyPingStr}}\n" +
		" Control Plane Operation: {{$obj.CplaneOperationStr}}\n" +
		" Send File Time: {{$obj.SendFileStr}}\n" +
		" Startup Time: {{$obj.StartupStr}}\n"
	ProxyConfTmpl = "\n{{$obj := .Proxy}}Proxy Config\n" +
		" Non Electable: {{$obj.NonElectable}}\n" +
		" Primary URL: {{$obj.PrimaryURL}}\n" +
		" Original URL: {{$obj.OriginalURL}}\n" +
		" Discovery URL: {{$obj.DiscoveryURL}}\n"
	LRUConfTmpl = "\n{{$obj := .LRU}}LRU Config\n" +
		" Low WM: {{$obj.LowWM}}\n" +
		" High WM: {{$obj.HighWM}}\n" +
		" Out-of-Space: {{$obj.OOS}}\n" +
		" Max Cache Entries: {{$obj.AtimeCacheMax}}\n" +
		" Dont Evict Time: {{$obj.DontEvictTimeStr}}\n" +
		" Capacity Update Time: {{$obj.CapacityUpdTimeStr}}\n" +
		" Local Buckets: {{$obj.LocalBuckets}}\n" +
		" Enabled: {{$obj.Enabled}}\n"
	XactionConfTmpl = "\n{{$obj := .Xaction}}Xaction Config\n" +
		" Disk Until Low WM: {{$obj.DiskUtilLowWM}}\n" +
		" Disk Until High WM: {{$obj.DiskUtilHighWM}}\n"
	RebalanceConfTmpl = "\n{{$obj := .Rebalance}}Rebalance Config\n" +
		" Destination Retry Time: {{$obj.DestRetryTimeStr}}\n" +
		" Enabled: {{$obj.Enabled}}\n"
	ReplicationConfTmpl = "\n{{$obj := .Replication}}Replication Config\n" +
		" On Cold Get: {{$obj.OnColdGet}}\n" +
		" On Put: {{$obj.OnPut}}\n" +
		" On LRU Eviction: {{$obj.OnLRUEviction}}\n"
	CksumConfTmpl = "\n{{$obj := .Cksum}}Checksum Config\n" +
		" Type: {{$obj.Type}}\n" +
		" Validate Cold Get: {{$obj.ValidateColdGet}}\n" +
		" Validate Warm Get: {{$obj.ValidateWarmGet}}\n" +
		" Validate Cluster Migration: {{$obj.ValidateClusterMigration}}\n" +
		" Enable Read Range: {{$obj.EnableReadRange}}\n"
	VerConfTmpl = "\n{{$obj := .Ver}}Version Config\n" +
		" Versioning: {{$obj.Versioning}}\n" +
		" Validate Warm Get: {{$obj.ValidateWarmGet}}\n"
	FSpathsConfTmpl = "\n{{$obj := .FSpaths}}File System Paths Config\n" +
		"{{range $key, $val := $obj}}" +
		"{{$key}}: {{$val}}\n" +
		"{{end}}\n"
	TestFSPConfTmpl = "\n{{$obj := .TestFSP}}Test File System Paths Config\n" +
		" Root: {{$obj.Root}}\n" +
		" Count: {{$obj.Count}}\n" +
		" Instance: {{$obj.Instance}}\n"
	NetConfTmpl = "\n{{$obj := .Net}}Network Config\n" +
		" IPv4: {{$obj.IPv4}}\n" +
		" IPv4 IntraControl: {{$obj.IPv4IntraControl}}\n" +
		" IPv4 IntraData: {{$obj.IPv4IntraData}}\n" +
		" Use IntraControl: {{$obj.UseIntraControl}}\n" +
		" Use IntraData: {{$obj.UseIntraData}}\n" +
		" \tHTTP\t \tL4\n" +
		" \tProtocol: {{$obj.HTTP.Proto}}\t \tProtocol: {{$obj.L4.Proto}}\n" +
		" \tReverse Proxy: {{$obj.HTTP.RevProxy}}\t \tPort: {{$obj.L4.PortStr}}\n" +
		" \tReverse Proxy Cache: {{$obj.HTTP.RevProxyCache}}\t \tIntraControl Port: {{$obj.L4.PortIntraControlStr}}\n" +
		" \tCertificate: {{$obj.HTTP.Certificate}}\t \tIntraData Port: {{$obj.L4.PortIntraDataStr}}\n" +
		" \tKey: {{$obj.HTTP.Key}}\t \t\n" +
		" \tMaxNumTargets: {{$obj.HTTP.MaxNumTargets}}\t \t\n" +
		" \tUseHTTPS: {{$obj.HTTP.UseHTTPS}}\t \t\n"
	FSHCConfTmpl = "\n{{$obj := .FSHC}}FSHC Config\n" +
		" Enabled: {{$obj.Enabled}}\n" +
		" Test File Count: {{$obj.TestFileCount}}\n" +
		" Error Limit: {{$obj.ErrorLimit}}\n"
	AuthConfTmpl = "\n{{$obj := .Auth}}Authentication Config\n" +
		" Secret: {{$obj.Secret}}\n" +
		" Enabled: {{$obj.Enabled}}\n" +
		" Credential Dir: {{$obj.CredDir}}\n"
	KeepaliveConfTmpl = "\n{{$obj := .KeepaliveTracker}}Keep Alive Tracker Config\n" +
		" Retry Factor:{{$obj.RetryFactor}}\t  Timeout Factor:{{$obj.TimeoutFactor}}\n" +
		" \tProxy\t \tTarget\n" +
		" Interval: \t{{$obj.Proxy.IntervalStr}}\t \t{{$obj.Target.IntervalStr}}\n" +
		" Name: \t{{$obj.Proxy.Name}}\t \t{{$obj.Target.Name}}\n" +
		" Factor: \t{{$obj.Proxy.Factor}}\t \t{{$obj.Target.Factor}}\n"
	DownloaderTmpl = "\n{{$obj := .Downloader}}Downloader Config\n" +
		" Timeout: {{$obj.TimeoutStr}}\n"

	ConfigTmpl = "\nConfig Directory: {{.Confdir}}\t Cloud Provider: {{.CloudProvider}}\n" +
		MirrorConfTmpl + ReadaheadConfTmpl + LogConfTmpl + PeriodConfTmpl + TimeoutConfTmpl +
		ProxyConfTmpl + LRUConfTmpl + XactionConfTmpl + RebalanceConfTmpl +
		ReplicationConfTmpl + CksumConfTmpl + VerConfTmpl + FSpathsConfTmpl +
		TestFSPConfTmpl + NetConfTmpl + FSHCConfTmpl + AuthConfTmpl + KeepaliveConfTmpl + DownloaderTmpl
)

var (
	funcMap = template.FuncMap{
		"ExtractStat":         ExtractStat,
		"FormatBytesSigned":   cmn.B2S,
		"FormatBytesUnsigned": cmn.UnsignedB2S,
		"CalcAvg":             CalcAvg,
	}
)

// Gets the associated value from CoreStats
func ExtractStat(daemon *stats.CoreStats, statName string) int64 {
	return daemon.Tracker[statName].Value
}

func CalcAvg(daemon *stats.DaemonStatus, option string) (total uint64) {
	for _, fs := range daemon.Capacity {
		switch option {
		case "capacity":
			total += fs.Avail
		case "percent":
			total += uint64(fs.Usedpct)
		}
	}
	return total / uint64(len(daemon.Capacity))
}

// Displays the output in either JSON or tabular form
func DisplayOutput(object interface{}, outputTemplate string, formatJSON ...bool) error {
	useJSON := false
	if len(formatJSON) > 0 {
		useJSON = formatJSON[0]
	}

	if useJSON {
		out, err := json.MarshalIndent(object, "", "    ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return nil
	}

	// Template
	tmpl, err := template.New("DisplayTemplate").Funcs(funcMap).Parse(outputTemplate)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 0, '\t', 0)
	if err := tmpl.Execute(w, object); err != nil {
		return err
	}
	w.Flush()
	return nil
}
