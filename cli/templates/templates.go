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

	"github.com/NVIDIA/aistore/ios"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

// Templates for output
// ** Changing the structure of the objects server side needs to make sure that this will still work **
const (
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
	ProxyInfoHeader = "Proxy\t %MemUsed\t MemAvail\t %CpuUsed\t Uptime\n"
	ProxyInfoBody   = "{{$value.Snode.DaemonID}}\t {{$value.SysInfo.PctMemUsed | printf `%6.2f`}}\t " +
		"{{FormatBytesUnsigned $value.SysInfo.MemAvail 2}}\t {{$value.SysInfo.PctCPUUsed | printf `%6.2f`}}\t " +
		"{{FormatDur (ExtractStat $value.Stats `up.µs.time`)}}\n"

	ProxyInfoBodyTmpl       = "{{ range $key, $value := . }}" + ProxyInfoBody + "{{end}}"
	ProxyInfoTmpl           = ProxyInfoHeader + ProxyInfoBodyTmpl
	ProxyInfoSingleBodyTmpl = "{{$value := . }}" + ProxyInfoBody
	ProxyInfoSingleTmpl     = ProxyInfoHeader + ProxyInfoSingleBodyTmpl

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
		" Retry Sync Time: {{$obj.RetrySyncTimeStr}}\n"
	TimeoutConfTmpl = "\n{{$obj := .Timeout}}Timeout Config\n" +
		" Default Timeout: {{$obj.DefaultStr}}\n" +
		" Default Long Timeout: {{$obj.DefaultLongStr}}\n" +
		" Max Keep Alive: {{$obj.MaxKeepaliveStr}}\n" +
		" Proxy Ping: {{$obj.ProxyPingStr}}\n" +
		" Control Plane Operation: {{$obj.CplaneOperationStr}}\n" +
		" List Time: {{$obj.ListBucketStr}}\n" +
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
		" Dont Evict Time: {{$obj.DontEvictTimeStr}}\n" +
		" Capacity Update Time: {{$obj.CapacityUpdTimeStr}}\n" +
		" Local Buckets: {{$obj.LocalBuckets}}\n" +
		" Enabled: {{$obj.Enabled}}\n"
	DiskConfTmpl = "\n{{$obj := .Disk}}Disk Config\n" +
		" Disk Until Low WM: {{$obj.DiskUtilLowWM}}\n" +
		" Disk Until High WM: {{$obj.DiskUtilHighWM}}\n" +
		" IO Stats Time Long: {{$obj.IostatTimeLongStr}}\n" +
		" IO Stats Time Short: {{$obj.IostatTimeShortStr}}\n"
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
		" Validate Object Migration: {{$obj.ValidateObjMove}}\n" +
		" Enable Read Range: {{$obj.EnableReadRange}}\n"
	VerConfTmpl = "\n{{$obj := .Ver}}Version Config\n" +
		" Validate Warm Get: {{$obj.ValidateWarmGet}}\n"
	FSpathsConfTmpl = "\nFile System Paths Config\n" +
		"{{$obj := .FSpaths.Paths}}" +
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
	ECConfTmpl = "\n{{$obj := .EC}}EC Config\t \t\n" +
		" Object Size Limit:{{$obj.ObjSizeLimit}}\t  Data Slices:{{$obj.DataSlices}}\n" +
		" Parity Slice:{{$obj.ParitySlices}}\t Enabled:{{$obj.Enabled}}\n"
	BucketVerConfTmpl = "\n{{$obj := .Versioning}}Bucket Versioning\n" +
		" Type:{{$obj.Type}}\n Validate Warm Get:{{$obj.ValidateWarmGet}}\t Enabled:{{$obj.Enabled}}\n"
	DownloaderConfTmpl = "\n{{$obj := .Downloader}}Downloader Config\n" +
		" Timeout: {{$obj.TimeoutStr}}\n"
	DSortConfTmpl = "\n{{$obj := .DSort}}Distributed Sort Config\n" +
		" Duplicated Records: {{$obj.DuplicatedRecords}}\n" +
		" Missing Shards: {{$obj.MissingShards}}\n"
	CompressionTmpl = "\n{{$obj := .Compression}}Compression\n" +
		" BlockSize:\t{{$obj.BlockMaxSize}}\n" +
		" Checksum:\t{{$obj.Checksum}}\n"
	ECTmpl = "\n{{$obj := .EC}}EC\n" +
		" Enabled:\t{{$obj.Enabled}}\n" +
		" Minimum object for EC:\t{{$obj.ObjSizeLimit}}\n" +
		" Number of data slices:\t{{$obj.DataSlices}}\n" +
		" Number of parity slices:\t{{$obj.ParitySlices}}\n" +
		" Compression options:\t{{$obj.Compression}}\n"

	ConfigTmpl = "Config Directory: {{.Confdir}}\nCloud Provider: {{.CloudProvider}}\n" +
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

	DownloadListHeader = "JobID\t Status\t Description\n"
	DownloadListBody   = "{{$value.ID}}\t " +
		"{{if (eq $value.Aborted true) }}Aborted" +
		"{{else}}{{if (eq $value.NumPending 0) }}Finished{{else}}{{$value.NumPending}} pending{{end}}" +
		"{{end}} \t {{$value.Description}}\n"
	DownloadListTmpl = DownloadListHeader + "{{ range $key, $value := . }}" + DownloadListBody + "{{end}}"

	DSortListHeader = "JobID\t Status\t Description\t Start\t Finish\n"
	DSortListBody   = "{{$value.ID}}\t " +
		"{{if (eq $value.Aborted true) }}Aborted" +
		"{{else if (eq $value.Archived true) }}Finished" +
		"{{else}}Running" +
		"{{end}} \t {{$value.Description}}\t {{FormatTime $value.StartedTime}}\t {{FormatTime $value.FinishTime}}\n"
	DSortListTmpl = DSortListHeader + "{{ range $key, $value := . }}" + DSortListBody + "{{end}}"

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
	BucketStatTmpl = "Value\tTotal\tData\tLocal\n{{range $p := . }}{{$p.Name}}\t{{$p.Total}}\t{{$p.Data}}\t{{$p.Local}}\n{{end}}\n"
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
		"status":    "{{$obj.Status}}\t",
		"copies":    "{{$obj.Copies}}\t",
		"iscached":  "{{$obj.IsCached}}\t",
	}

	ObjStatMap = map[string]string{
		"local":    "{{ .BucketLocal }}\t",
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

func isUnsetTime(t time.Time) bool {
	return t.IsZero()
}

func fmtTime(t time.Time) string {
	return t.Format("01-02 15:04:05.000")
}

func fmtObjTime(t time.Time) string {
	return t.Format(time.RFC822)
}

func fmtDuration(d int64) string {
	// Convert to nanoseconds
	dNano := time.Duration(d * int64(time.Microsecond))
	return dNano.String()
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
