// Package templates provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package templates

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
)

// Templates for output
// ** Changing the structure of the objects server side needs to make sure that this will still work **
const (
	primarySuffix      = "[P]"
	nonElectableSuffix = "[-]"

	// Smap
	SmapHeader = "DaemonID\t Type\t PublicURL" +
		"{{ if (eq $.ExtendedURLs true) }}\t IntraControlURL\t IntraDataURL{{end}}" +
		"\n"
	SmapBody = "{{FormatDaemonID $value.DaemonID $.Smap}}\t {{$value.DaemonType}}\t {{$value.PublicNet.DirectURL}}" +
		"{{ if (eq $.ExtendedURLs true) }}\t {{$value.IntraControlNet.DirectURL}}\t {{$value.IntraDataNet.DirectURL}}{{end}}" +
		"\n"

	SmapTmpl = SmapHeader +
		"{{ range $key, $value := .Smap.Pmap }}" + SmapBody + "{{end}}\n" +
		SmapHeader +
		"{{ range $key, $value := .Smap.Tmap }}" + SmapBody + "{{end}}\n" +
		"Non-Electable:\n" +
		"{{ range $key, $ := .Smap.NonElects }} ProxyID: {{$key}}\n{{end}}\n" +
		"PrimaryProxy: {{.Smap.ProxySI.DaemonID}}\t Proxies: {{len .Smap.Pmap}}\t Targets: {{len .Smap.Tmap}}\t Smap Version: {{.Smap.Version}}\n"

	// Proxy Info
	ProxyInfoHeader = "Proxy\t %MemUsed\t MemAvail\t %CpuUsed\t Uptime\n"
	ProxyInfoBody   = "{{$value.Snode.DaemonID}}\t {{$value.SysInfo.PctMemUsed | printf `%6.2f`}}\t " +
		"{{FormatBytesUnsigned $value.SysInfo.MemAvail 2}}\t {{$value.SysInfo.PctCPUUsed | printf `%6.2f`}}\t " +
		"{{FormatDur (ExtractStat $value.Stats `up.µs.time`)}}\n"

	ProxyInfoBodyTmpl       = "{{ range $key, $value := . }}" + ProxyInfoBody + "{{end}}"
	ProxyInfoTmpl           = ProxyInfoHeader + ProxyInfoBodyTmpl
	ProxyInfoSingleBodyTmpl = "{{$value := . }}" + ProxyInfoBody
	ProxyInfoSingleTmpl     = ProxyInfoHeader + ProxyInfoSingleBodyTmpl

	AllProxyInfoBody = "{{FormatDaemonID $value.Snode.DaemonID $.Smap}}\t {{$value.SysInfo.PctMemUsed | printf `%6.2f`}}\t " +
		"{{FormatBytesUnsigned $value.SysInfo.MemAvail 2}}\t {{$value.SysInfo.PctCPUUsed | printf `%6.2f`}}\t " +
		"{{FormatDur (ExtractStat $value.Stats `up.µs.time`)}}\n"
	AllProxyInfoBodyTmpl = "{{ range $key, $value := .Status }}" + AllProxyInfoBody + "{{end}}"
	AllProxyInfoTmpl     = ProxyInfoHeader + AllProxyInfoBodyTmpl

	// Target Info
	TargetInfoHeader = "Target\t %MemUsed\t MemAvail\t %CapUsed\t CapAvail\t %CpuUsed\t Rebalance\n"
	TargetInfoBody   = "{{$value.Snode.DaemonID}}\t " +
		"{{$value.SysInfo.PctMemUsed | printf `%6.2f`}}\t {{FormatBytesUnsigned $value.SysInfo.MemAvail 2}}\t " +
		"{{CalcCap $value `percent` | printf `%d`}}\t {{$capacity := CalcCap $value `capacity`}}{{FormatBytesUnsigned $capacity 3}}\t " +
		"{{$value.SysInfo.PctCPUUsed | printf `%6.2f`}}\t " +
		"{{FormatXactStatus $value.TStatus }}\n"

	TargetInfoBodyTmpl       = "{{ range $key, $value := . }}" + TargetInfoBody + "{{end}}"
	TargetInfoTmpl           = TargetInfoHeader + TargetInfoBodyTmpl
	TargetInfoSingleBodyTmpl = "{{$value := . }}" + TargetInfoBody
	TargetInfoSingleTmpl     = TargetInfoHeader + TargetInfoSingleBodyTmpl

	ClusterSummary = "Summary:\n Proxies:\t{{len .Pmap}} ({{len .NonElects}} - unelectable)\n Targets:\t{{len .Tmap}}\n Primary Proxy:\t{{.ProxySI.DaemonID}}\n Smap Version:\t{{.Version}}\n"

	// Stats
	StatsHeader = "{{$obj := . }}\nDaemon: {{ .Snode.DaemonID }}\t Type: {{ .Snode.DaemonType }}\n\nStats\n"
	StatsBody   = "{{range $key, $val := $obj.Stats.Tracker }}" +
		"{{$statVal := ExtractStat $obj.Stats $key}}" +
		"{{if (eq $statVal 0)}}{{else}}{{$key}}\t{{$statVal}}\n{{end}}" +
		"{{end}}\n"

	ProxyStatsTmpl  = StatsHeader + StatsBody
	TargetStatsTmpl = StatsHeader + StatsBody +
		"Mountpaths\t %CapacityUsed\t CapacityAvail\n" +
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
		"Mountpaths\t %CapacityUsed\t CapacityAvail\n" +
		"{{range $mount, $capa := $val.Capacity}}" +
		"{{$mount}}\t {{$capa.Usedpct | printf `%0.2d`}}\t {{FormatBytesUnsigned $capa.Avail 5}}\n" +
		"{{end}}\n\n" +
		"{{end}}"

	// Disk Stats
	DiskStatsHeader = "Target\t" +
		"Disk\t" +
		"Read\t" +
		"Write\t" +
		"%Util\n"

	DiskStatsBody = "{{ $value.TargetID }}\t" +
		"{{ $value.DiskName }}\t" +
		"{{ $stat := $value.Stat }}" +
		"{{ FormatBytesSigned $stat.RBps 2 }}/s\t" +
		"{{ FormatBytesSigned $stat.WBps 2 }}/s\t" +
		"{{ $stat.Util }}\n"

	DiskStatBodyTmpl  = "{{ range $key, $value := . }}" + DiskStatsBody + "{{ end }}"
	DiskStatsFullTmpl = DiskStatsHeader + DiskStatBodyTmpl

	// Config
	MirrorConfTmpl = "\n{{$obj := .Mirror}}Mirror Config\n" +
		" Copies:\t{{$obj.Copies}}\n" +
		" Burst:\t{{$obj.Burst}}\n" +
		" Utilization Threshold:\t{{$obj.UtilThresh}}\n" +
		" Optimize PUT:\t{{$obj.OptimizePUT}}\n" +
		" Enabled:\t{{$obj.Enabled}}\n"
	LogConfTmpl = "\n{{$obj := .Log}}Log Config\n" +
		" Dir:\t{{$obj.Dir}}\n" +
		" Level:\t{{$obj.Level}}\n" +
		" Maximum Log File Size:\t{{$obj.MaxSize}}\n" +
		" Maximum Total Size:\t{{$obj.MaxTotal}}\n"
	PeriodConfTmpl = "\n{{$obj := .Periodic}}Period Config\n" +
		" Stats Time:\t{{$obj.StatsTimeStr}}\n" +
		" Retry Sync Time:\t{{$obj.RetrySyncTimeStr}}\n"
	TimeoutConfTmpl = "\n{{$obj := .Timeout}}Timeout Config\n" +
		" Default Timeout:\t{{$obj.DefaultStr}}\n" +
		" Default Long Timeout:\t{{$obj.DefaultLongStr}}\n" +
		" Max Keep Alive:\t{{$obj.MaxKeepaliveStr}}\n" +
		" Proxy Ping:\t{{$obj.ProxyPingStr}}\n" +
		" Control Plane Operation:\t{{$obj.CplaneOperationStr}}\n" +
		" List Time:\t{{$obj.ListBucketStr}}\n" +
		" Send File Time:\t{{$obj.SendFileStr}}\n" +
		" Startup Time:\t{{$obj.StartupStr}}\n"
	ProxyConfTmpl = "\n{{$obj := .Proxy}}Proxy Config\n" +
		" Non Electable:\t{{$obj.NonElectable}}\n" +
		" Primary URL:\t{{$obj.PrimaryURL}}\n" +
		" Original URL:\t{{$obj.OriginalURL}}\n" +
		" Discovery URL:\t{{$obj.DiscoveryURL}}\n"
	LRUConfTmpl = "\n{{$obj := .LRU}}LRU Config\n" +
		" Low WM:\t{{$obj.LowWM}}\n" +
		" High WM:\t{{$obj.HighWM}}\n" +
		" Out-of-Space:\t{{$obj.OOS}}\n" +
		" Don't Evict Time:\t{{$obj.DontEvictTimeStr}}\n" +
		" Capacity Update Time:\t{{$obj.CapacityUpdTimeStr}}\n" +
		" Evict AIS Buckets:\t{{$obj.EvictAISBuckets}}\n" +
		" Enabled:\t{{$obj.Enabled}}\n"
	DiskConfTmpl = "\n{{$obj := .Disk}}Disk Config\n" +
		" Disk Utilization Low WM:\t{{$obj.DiskUtilLowWM}}\n" +
		" Disk Utilization High WM:\t{{$obj.DiskUtilHighWM}}\n" +
		" Disk Utilization Max WM:\t{{$obj.DiskUtilMaxWM}}\n" +
		" IO Stats Time Long:\t{{$obj.IostatTimeLongStr}}\n" +
		" IO Stats Time Short:\t{{$obj.IostatTimeShortStr}}\n"
	RebalanceConfTmpl = "\n{{$obj := .Rebalance}}Rebalance Config\n" +
		" Destination Retry Time:\t{{$obj.DestRetryTimeStr}}\n" +
		" Enabled:\t{{$obj.Enabled}}\n" +
		" Multiplier:\t{{$obj.Multiplier}}\n" +
		" Compression:\t{{$obj.Compression}}\n"
	CksumConfTmpl = "\n{{$obj := .Cksum}}Checksum Config\n" +
		" Type:\t{{$obj.Type}}\n" +
		" Validate On Cold Get:\t{{$obj.ValidateColdGet}}\n" +
		" Validate On Warm Get:\t{{$obj.ValidateWarmGet}}\n" +
		" Validate On Object Migration:\t{{$obj.ValidateObjMove}}\n" +
		" Enable For Read Range:\t{{$obj.EnableReadRange}}\n"
	VerConfTmpl = "\n{{$obj := .Ver}}Version Config\n" +
		" Enabled:\t{{$obj.Enabled}}\n" +
		" Type:\t{{$obj.Type}}\n" +
		" Validate Warm Get:\t{{$obj.ValidateWarmGet}}\n"
	FSpathsConfTmpl = "\nFile System Paths Config\n" +
		"{{$obj := .FSpaths.Paths}}" +
		"{{range $key, $val := $obj}}" +
		"{{$key}}:\t{{$val}}\n" +
		"{{end}}"
	TestFSPConfTmpl = "\n{{$obj := .TestFSP}}Test File System Paths Config\n" +
		" Root:\t{{$obj.Root}}\n" +
		" Count:\t{{$obj.Count}}\n" +
		" Instance:\t{{$obj.Instance}}\n"
	NetConfTmpl = "\n{{$obj := .Net}}Network Config\n" +
		" IPv4:\t{{$obj.IPv4}}\n" +
		" IPv4 IntraControl:\t{{$obj.IPv4IntraControl}}\n" +
		" IPv4 IntraData:\t{{$obj.IPv4IntraData}}\n\n" +
		" HTTP\n" +
		" Protocol:\t{{$obj.HTTP.Proto}}\n" +
		" Reverse Proxy:\t{{$obj.HTTP.RevProxy}}\n" +
		" Reverse Proxy Cache:\t{{$obj.HTTP.RevProxyCache}}\n" +
		" Certificate:\t{{$obj.HTTP.Certificate}}\n" +
		" Key:\t{{$obj.HTTP.Key}}\n" +
		" UseHTTPS:\t{{$obj.HTTP.UseHTTPS}}\n" +
		" Chunked Transfer:\t{{$obj.HTTP.Chunked}}\n\n" +
		" L4\n" +
		" Protocol:\t{{$obj.L4.Proto}}\n" +
		" Port:\t{{$obj.L4.PortStr}}\n" +
		" IntraControl Port:\t{{$obj.L4.PortIntraControlStr}}\n" +
		" IntraData Port:\t{{$obj.L4.PortIntraDataStr}}\n" +
		" Send/Receive buffer size:\t{{$obj.L4.SndRcvBufSize}}\n"
	FSHCConfTmpl = "\n{{$obj := .FSHC}}FSHC Config\n" +
		" Enabled:\t{{$obj.Enabled}}\n" +
		" Test File Count:\t{{$obj.TestFileCount}}\n" +
		" Error Limit:\t{{$obj.ErrorLimit}}\n"
	AuthConfTmpl = "\n{{$obj := .Auth}}Authentication Config\n" +
		" Secret:\t{{$obj.Secret}}\n" +
		" Enabled:\t{{$obj.Enabled}}\n" +
		" Credential Dir:\t{{$obj.CredDir}}\n"
	KeepaliveConfTmpl = "\n{{$obj := .KeepaliveTracker}}Keep Alive Tracker Config\n" +
		" Retry Factor:{{$obj.RetryFactor}}\t  Timeout Factor:{{$obj.TimeoutFactor}}\n" +
		" \tProxy\t \tTarget\n" +
		" Interval: \t{{$obj.Proxy.IntervalStr}}\t \t{{$obj.Target.IntervalStr}}\n" +
		" Name: \t{{$obj.Proxy.Name}}\t \t{{$obj.Target.Name}}\n" +
		" Factor: \t{{$obj.Proxy.Factor}}\t \t{{$obj.Target.Factor}}\n"
	ECConfTmpl = "\n{{$obj := .EC}}EC Config\t \t\n" +
		" Object Size Limit:{{$obj.ObjSizeLimit}}\t  Data Slices:{{$obj.DataSlices}}\n" +
		" Parity Slice:{{$obj.ParitySlices}}\t Enabled:{{$obj.Enabled}}\n"
	BucketVerConfTmpl = "\n{{$obj := .Versioning}}Bucket Versioning\n" +
		" Type:{{$obj.Type}}\n Validate Warm Get:{{$obj.ValidateWarmGet}}\n Enabled:{{$obj.Enabled}}\n"
	DownloaderConfTmpl = "\n{{$obj := .Downloader}}Downloader Config\n" +
		" Timeout: {{$obj.TimeoutStr}}\n"
	DSortConfTmpl = "\n{{$obj := .DSort}}Distributed Sort Config\n" +
		" Duplicated Records:\t{{$obj.DuplicatedRecords}}\n" +
		" Missing Shards:\t{{$obj.MissingShards}}\n" +
		" EKM Malformed Line:\t{{$obj.EKMMalformedLine}}\n" +
		" EKM Missing Key:\t{{$obj.EKMMissingKey}}\n" +
		" Call Timeout:\t{{$obj.CallTimeoutStr}}\n" +
		" Compression:\t{{$obj.Compression}}\n"
	CompressionTmpl = "\n{{$obj := .Compression}}Compression\n" +
		" BlockSize:\t{{$obj.BlockMaxSize}}\n" +
		" Checksum:\t{{$obj.Checksum}}\n"
	ECTmpl = "\n{{$obj := .EC}}EC\n" +
		" Enabled:\t{{$obj.Enabled}}\n" +
		" Minimum object size for EC:\t{{$obj.ObjSizeLimit}}\n" +
		" Number of data slices:\t{{$obj.DataSlices}}\n" +
		" Number of parity slices:\t{{$obj.ParitySlices}}\n" +
		" Compression options:\t{{$obj.Compression}}\n"
	GlobalConfTmpl = "Config Directory: {{.Confdir}}\nCloud Provider: {{.CloudProvider}}\n"

	// hidden config sections: replication and readahead.
	// Application Config has this sections but setup/config.sh does not expose them
	ReplicationConfTmpl = "\n{{$obj := .Replication}}Replication Config\n" +
		" On Cold Get:\t{{$obj.OnColdGet}}\n" +
		" On Put:\t{{$obj.OnPut}}\n" +
		" On LRU Eviction:\t{{$obj.OnLRUEviction}}\n"
	ReadaheadConfTmpl = "\n{{$obj := .Readahead}}Readahead Config\n" +
		" ObjectMem:\t{{$obj.ObjectMem}}\n" +
		" TotalMem:\t{{$obj.TotalMem}}\n" +
		" ByProxy:\t{{$obj.ByProxy}}\n" +
		" Discard:\t{{$obj.Discard}}\n" +
		" Enabled:\t{{$obj.Discard}}\n"

	ConfigTmpl = GlobalConfTmpl +
		MirrorConfTmpl + ReadaheadConfTmpl + LogConfTmpl + PeriodConfTmpl + TimeoutConfTmpl +
		ProxyConfTmpl + LRUConfTmpl + DiskConfTmpl + RebalanceConfTmpl +
		ReplicationConfTmpl + CksumConfTmpl + VerConfTmpl + FSpathsConfTmpl +
		TestFSPConfTmpl + NetConfTmpl + FSHCConfTmpl + AuthConfTmpl + KeepaliveConfTmpl +
		DownloaderConfTmpl + DSortConfTmpl +
		CompressionTmpl + ECTmpl

	BucketPropsTmpl = "\nCloud Provider: {{.CloudProvider}}\n" +
		BucketVerConfTmpl + CksumConfTmpl + LRUConfTmpl + MirrorConfTmpl + ECConfTmpl

	BucketPropsSimpleTmpl = "Property\tValue\n" +
		"{{range $p := . }}" +
		"{{$p.Name}}\t{{$p.Val}}\n" +
		"{{end}}\n"

	DownloadListHeader = "JobID\t Status\t Errors\t Description\n"
	DownloadListBody   = "{{$value.ID}}\t " +
		"{{if (eq $value.Aborted true) }}Aborted" +
		"{{else}}{{if (eq $value.NumPending 0) }}Finished{{else}}{{$value.NumPending}} pending{{end}}" +
		"{{end}}\t {{$value.NumErrors}}\t {{$value.Description}}\n"
	DownloadListTmpl = DownloadListHeader + "{{ range $key, $value := . }}" + DownloadListBody + "{{end}}"

	DSortListHeader = "JobID\t Status\t Start\t Finish\t Description\n"
	DSortListBody   = "{{$value.ID}}\t " +
		"{{if (eq $value.Aborted true) }}Aborted" +
		"{{else if (eq $value.Archived true) }}Finished" +
		"{{else}}Running" +
		"{{end}}\t {{FormatTime $value.StartedTime}}\t {{FormatTime $value.FinishTime}} \t {{$value.Description}}\n"
	DSortListTmpl = DSortListHeader + "{{ range $value := . }}" + DSortListBody + "{{end}}"

	XactionBaseStatsHeader = "Daemon\t Kind\t Bucket\t Objects\t Bytes\t Start\t End\t Aborted\n"
	XactionBaseBody        = "{{$key}}\t {{$xact.KindX}}\t {{$xact.BucketX}}\t " +
		"{{if (eq $xact.ObjCountX 0) }}-{{else}}{{$xact.ObjCountX}}{{end}} \t" +
		"{{if (eq $xact.BytesCountX 0) }}-{{else}}{{FormatBytesSigned $xact.BytesCountX 2}}{{end}} \t {{FormatTime $xact.StartTimeX}}\t " +
		"{{if (IsUnsetTime $xact.EndTimeX)}}-{{else}}{{FormatTime $xact.EndTimeX}}{{end}} \t {{$xact.AbortedX}}\n"
	XactionExtBody = "{{if $xact.Ext}}" + // if not nil
		"Kind: {{$xact.KindX}}\n" +
		"{{range $name, $val := $xact.Ext}}" +
		"{{$name}}: {{$val | printf `%0.0f`}}\t " +
		"{{end}}" +
		"{{end}}{{if $xact.Ext}}\n{{end}}"
	XactStatsTmpl = XactionBaseStatsHeader +
		"{{range $key, $daemon := .}}" + // iterate through the entire map
		"{{range $xact := $daemon}}" + // for each daemon's xactions, print BaseXactStats
		XactionBaseBody +
		"{{end}}" +

		"{{range $xact := $daemon}}" + // for each daemon's xactions, print BaseXactExtStats
		XactionExtBody +
		"{{end}}" +
		"{{end}}"
	BucketsSummariesTmpl = "Name\tObjects\tSize\tUsed(%)\tProvider\n{{range $k, $v := . }}{{$v.Name}}\t{{$v.ObjCount}}\t{{FormatBytesUnsigned $v.Size 2}}\t{{$v.UsedPct}}%\t{{$v.Provider}}\n{{end}}"

	// For `object put` mass uploader. A caller adds to the template
	// total count and size. That is why the template ends with \t
	ExtensionTmpl = "Files to upload:\nExtension\tCount\tSize\n" +
		"{{range $k, $v := . }}" +
		"{{$k}}\t{{$v.Cnt}}\t{{FormatBytesSigned $v.Size 2}}\n" +
		"{{end}}" +
		"TOTAL\t"
)

var (
	// ObjectPropsMap matches BucketEntry field
	ObjectPropsMap = map[string]string{
		"name":      "{{$obj.Name}}\t",
		"size":      "{{FormatBytesSigned $obj.Size 2}}\t",
		"checksum":  "{{$obj.Checksum}}\t",
		"type":      "{{$obj.Type}}\t",
		"atime":     "{{$obj.Atime}}\t",
		"bucket":    "{{$obj.Bucket}}\t",
		"version":   "{{$obj.Version}}\t",
		"targetURL": "{{$obj.TargetURL}}\t",
		"status":    "{{FormatObjStatus $obj}}\t",
		"copies":    "{{$obj.Copies}}\t",
		"iscached":  "{{FormatObjIsCached $obj}}\t",
	}

	ObjStatMap = map[string]string{
		"ais":      "{{ .BckIsAIS }}\t",
		"iscached": "{{ .Present }}\t",
		"size":     "{{ FormatBytesSigned .Size 2 }}\t",
		"version":  "{{ .Version }}\t",
		"atime":    "{{ if IsUnsetTime .Atime }}-{{else}}{{ FormatObjTime .Atime }}{{end}}\t",
		"copies":   "{{ if .NumCopies }}{{ .NumCopies }}{{else}}-{{end}}\t",
		"checksum": "{{ if .Checksum }}{{ .Checksum }}{{else}}-{{end}}\t",
	}

	funcMap = template.FuncMap{
		"ExtractStat":         extractStat,
		"FormatBytesSigned":   cmn.B2S,
		"FormatBytesUnsigned": cmn.UnsignedB2S,
		"CalcCap":             calcCap,
		"IsUnsetTime":         isUnsetTime,
		"FormatTime":          fmtTime,
		"FormatObjTime":       fmtObjTime,
		"FormatDur":           fmtDuration,
		"FormatXactStatus":    fmtXactStatus,
		"FormatObjStatus":     fmtObjStatus,
		"FormatObjIsCached":   fmtObjIsCached,
		"FormatDaemonID":      fmtDaemonID,
	}
)

type DiskStatsTemplateHelper struct {
	TargetID string
	DiskName string
	Stat     *ios.SelectedDiskStats
}

type ObjectStatTemplateHelper struct {
	Name  string
	Props *cmn.ObjectProps
}

type SmapTemplateHelper struct {
	Smap         *cluster.Smap
	ExtendedURLs bool
}

type StatusTemplateHelper struct {
	Smap   *cluster.Smap
	Status map[string]*stats.DaemonStatus
}

// Gets the associated value from CoreStats
func extractStat(daemon *stats.CoreStats, statName string) int64 {
	return daemon.Tracker[statName].Value
}

func calcCap(daemon *stats.DaemonStatus, option string) (total uint64) {
	for _, fs := range daemon.Capacity {
		switch option {
		case "capacity":
			total += fs.Avail
		case "percent":
			total += uint64(fs.Usedpct)
		}
	}

	switch option {
	case "capacity":
		return total
	case "percent":
		return total / uint64(len(daemon.Capacity))
	}

	return 0
}

func fmtXactStatus(tStatus *stats.TargetStatus) string {
	if tStatus == nil || tStatus.GlobalRebalanceStats == nil {
		return "not started"
	}

	tStats := tStatus.GlobalRebalanceStats
	if tStats.Aborted() {
		return fmt.Sprintf("aborted; %d objs moved (%s)", tStats.ObjCount(), cmn.B2S(tStats.BytesCount(), 1))
	}
	if tStats.EndTime().IsZero() {
		return fmt.Sprintf("running; %d objs moved (%s)", tStats.ObjCount(), cmn.B2S(tStats.BytesCount(), 1))
	}
	return fmt.Sprintf("finished; %d objs moved (%s)", tStats.ObjCount(), cmn.B2S(tStats.BytesCount(), 1))
}

func fmtObjStatus(obj *cmn.BucketEntry) string {
	if obj.IsStatusOK() {
		return ""
	}
	return "Moved"
}

var (
	ConfigSectionTmpl = map[string]string{
		"global":               GlobalConfTmpl,
		"mirror":               MirrorConfTmpl,
		"log":                  LogConfTmpl,
		"periodic":             PeriodConfTmpl,
		"timeout":              TimeoutConfTmpl,
		"proxy":                ProxyConfTmpl,
		"lru":                  LRUConfTmpl,
		"disk":                 DiskConfTmpl,
		"rebalance":            RebalanceConfTmpl,
		"checksum":             CksumConfTmpl,
		"versioning":           VerConfTmpl,
		"fspath":               FSpathsConfTmpl,
		"testfs":               TestFSPConfTmpl,
		"network":              NetConfTmpl,
		"fshc":                 FSHCConfTmpl,
		"auth":                 AuthConfTmpl,
		"keepalive":            KeepaliveConfTmpl,
		"downloader":           DownloaderConfTmpl,
		cmn.DSortNameLowercase: DSortConfTmpl,
		"compression":          CompressionTmpl,
		"ec":                   ECTmpl,
		"readahead":            ReadaheadConfTmpl,
		"replication":          ReplicationConfTmpl,
	}
)

func fmtObjIsCached(obj *cmn.BucketEntry) string {
	if obj.IsCached() {
		return "true"
	}
	return "false"
}

func isUnsetTime(t time.Time) bool {
	return t.IsZero()
}

func fmtTime(t time.Time) string {
	return t.Format("01-02 15:04:05")
}

func fmtObjTime(t time.Time) string {
	return t.Format(time.RFC822)
}

func fmtDuration(d int64) string {
	dNano := time.Duration(d * int64(time.Microsecond))
	return dNano.Round(time.Second).String()
}

func fmtDaemonID(id string, smap cluster.Smap) string {
	if id == smap.ProxySI.DaemonID {
		return id + primarySuffix
	}
	if _, ok := smap.NonElects[id]; ok {
		return id + nonElectableSuffix
	}
	return id
}

// Displays the output in either JSON or tabular form
func DisplayOutput(object interface{}, writer io.Writer, outputTemplate string, formatJSON ...bool) error {
	useJSON := false
	if len(formatJSON) > 0 {
		useJSON = formatJSON[0]
	}

	if useJSON {
		out, err := json.MarshalIndent(object, "", "    ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(writer, string(out))
		return err
	}

	// Template
	tmpl, err := template.New("DisplayTemplate").Funcs(funcMap).Parse(outputTemplate)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(writer, 0, 8, 1, '\t', 0)
	if err := tmpl.Execute(w, object); err != nil {
		return err
	}

	return w.Flush()
}
