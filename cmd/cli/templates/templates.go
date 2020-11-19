// Package templates provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package templates

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"k8s.io/apimachinery/pkg/util/duration"
)

// Templates for output
// ** Changing the structure of the objects server side needs to make sure that this will still work **
const (
	primarySuffix      = "[P]"
	nonElectableSuffix = "[-]"

	// Smap
	SmapHeader = "DAEMON ID\t TYPE\t PUBLIC URL" +
		"{{ if (eq $.ExtendedURLs true) }}\t INTRA CONTROL URL\t INTRA DATA URL{{end}}" +
		"\n"
	SmapBody = "{{FormatDaemonID $value.ID $.Smap}}\t {{$value.DaemonType}}\t {{$value.PublicNet.DirectURL}}" +
		"{{ if (eq $.ExtendedURLs true) }}\t {{$value.IntraControlNet.DirectURL}}\t {{$value.IntraDataNet.DirectURL}}{{end}}" +
		"\n"

	SmapTmpl = SmapHeader +
		"{{ range $key, $value := .Smap.Pmap }}" + SmapBody + "{{end}}\n" +
		SmapHeader +
		"{{ range $key, $value := .Smap.Tmap }}" + SmapBody + "{{end}}\n" +
		"Non-Electable:\n" +
		"{{ range $key, $si := .Smap.Pmap }} " +
		"{{ if (eq $si.NonElectable true) }} ProxyID: {{$key}}\n{{end}}{{end}}\n" +
		"Primary Proxy: {{.Smap.Primary.ID}}\nProxies: {{len .Smap.Pmap}}\t Targets: {{len .Smap.Tmap}}\t Smap Version: {{.Smap.Version}}\n"

	// Proxy Info
	ProxyInfoHeader = "PROXY\t MEM USED %\t MEM AVAIL\t CPU USED %\t UPTIME\t STATUS\n"
	ProxyInfoBody   = "{{$value.Snode.ID}}\t {{$value.SysInfo.PctMemUsed | printf `%.2f`}}\t " +
		"{{FormatBytesUnsigned $value.SysInfo.MemAvail 2}}\t {{$value.SysInfo.PctCPUUsed | printf `%.2f`}}\t " +
		"{{FormatDur (ExtractStat $value.Stats `up.ns.time`)}}\t " +
		"{{$value.Status}}\n"

	ProxyInfoBodyTmpl       = "{{ range $key, $value := .Status.Pmap }}" + ProxyInfoBody + "{{end}}"
	ProxyInfoTmpl           = ProxyInfoHeader + ProxyInfoBodyTmpl
	ProxyInfoSingleBodyTmpl = "{{$value := . }}" + ProxyInfoBody
	ProxyInfoSingleTmpl     = ProxyInfoHeader + ProxyInfoSingleBodyTmpl

	AllProxyInfoBody = "{{FormatDaemonID $value.Snode.ID $.Smap}}\t {{$value.SysInfo.PctMemUsed | printf `%.2f`}}\t " +
		"{{FormatBytesUnsigned $value.SysInfo.MemAvail 2}}\t {{$value.SysInfo.PctCPUUsed | printf `%.2f`}}\t " +
		"{{FormatDur (ExtractStat $value.Stats `up.ns.time`)}}\t " +
		"{{$value.Status}}\n"
	AllProxyInfoBodyTmpl = "{{ range $key, $value := .Status.Pmap }}" + AllProxyInfoBody + "{{end}}"
	AllProxyInfoTmpl     = ProxyInfoHeader + AllProxyInfoBodyTmpl

	// Target Info
	TargetInfoHeader   = "TARGET\t MEM USED %\t MEM AVAIL\t CAP USED %\t CAP AVAIL\t CPU USED %\t REBALANCE\t UPTIME\t STATUS\n"
	TargetInfoIDSingle = "{{$value.Snode.ID}}\t "
	TargetInfoIDAll    = "{{FormatDaemonID $value.Snode.ID $.Smap}}\t "
	TargetInfoBody     = "{{$value.SysInfo.PctMemUsed | printf `%.2f`}}\t {{FormatBytesUnsigned $value.SysInfo.MemAvail 2}}\t " +
		"{{CalcCap $value `percent` | printf `%d`}}\t {{$capacity := CalcCap $value `capacity`}}{{FormatBytesUnsigned $capacity 3}}\t " +
		"{{$value.SysInfo.PctCPUUsed | printf `%.2f`}}\t " +
		"{{FormatXactStatus $value.TStatus }}\t " +
		"{{FormatDur (ExtractStat $value.Stats `up.ns.time`)}}\t " +
		"{{$value.Status}}\n"

	TargetInfoBodyTmpl       = "{{ range $key, $value := .Status.Tmap }}" + TargetInfoIDAll + TargetInfoBody + "{{end}}"
	TargetInfoTmpl           = TargetInfoHeader + TargetInfoBodyTmpl
	TargetInfoSingleBodyTmpl = "{{$value := . }}" + TargetInfoIDSingle + TargetInfoBody
	TargetInfoSingleTmpl     = TargetInfoHeader + TargetInfoSingleBodyTmpl

	ClusterSummary = "Summary:\n Proxies:\t{{len .Smap.Pmap}} ({{CountNonElectable .Smap.Pmap}} - unelectable)\n " +
		"Targets:\t{{len .Smap.Tmap}}\n Primary Proxy:\t{{.Smap.Primary.ID}}\n Smap Version:\t{{.Smap.Version}}\n"

	ClusterInfoTmpl = AllProxyInfoTmpl + "\n" + TargetInfoTmpl + "\n" + ClusterSummary
	// Disk Stats
	DiskStatsHeader = "TARGET\t DISK\t READ\t WRITE\t UTIL %\n"

	DiskStatsBody = "{{ $value.TargetID }}\t " +
		"{{ $value.DiskName }}\t " +
		"{{ $stat := $value.Stat }}" +
		"{{ FormatBytesSigned $stat.RBps 2 }}/s\t " +
		"{{ FormatBytesSigned $stat.WBps 2 }}/s\t " +
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
		" Max Keep Alive:\t{{$obj.MaxKeepaliveStr}}\n" +
		" Control Plane Operation:\t{{$obj.CplaneOperationStr}}\n" +
		" Max Host Busy:\t{{$obj.MaxHostBusyStr}}\n" +
		" Send File Time:\t{{$obj.SendFileStr}}\n" +
		" Startup Time:\t{{$obj.StartupStr}}\n"
	ClientConfTmpl = "\n{{$obj := .Client}}Client Config\n" +
		" Timeout:\t{{$obj.TimeoutStr}}\n" +
		" Long Timeout:\t{{$obj.TimeoutLongStr}}\n" +
		" List Time:\t{{$obj.ListObjectsStr}}\n" +
		" Flags:\t{{FormatFeatureFlags $obj.Features}}\n"
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
	VerConfTmpl = "\n{{$obj := .Versioning}}Version Config\n" +
		" Enabled:\t{{$obj.Enabled}}\n" +
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
		" Enabled:\t{{$obj.Enabled}}\n"
	KeepaliveConfTmpl = "\n{{$obj := .KeepaliveTracker}}Keep Alive Tracker Config\n" +
		" Retry Factor:{{$obj.RetryFactor}}\t  Timeout Factor:{{$obj.TimeoutFactor}}\n" +
		" \tProxy\t \tTarget\n" +
		" Interval: \t{{$obj.Proxy.IntervalStr}}\t \t{{$obj.Target.IntervalStr}}\n" +
		" Name: \t{{$obj.Proxy.Name}}\t \t{{$obj.Target.Name}}\n" +
		" Factor: \t{{$obj.Proxy.Factor}}\t \t{{$obj.Target.Factor}}\n"
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
		" Rebalance batch size:\t{{$obj.BatchSize}}\n" +
		" Compression options:\t{{$obj.Compression}}\n"
	GlobalConfTmpl = "Config Directory: {{.Confdir}}\nCloud Providers: {{ range $key := .Cloud.Providers}} {{$key}} {{end}}\n"

	// hidden config sections: replication
	// Application Config has this sections but /deploy/dev/local/aisnode_config.sh does not expose them
	ReplicationConfTmpl = "\n{{$obj := .Replication}}Replication Config\n" +
		" On Cold Get:\t{{$obj.OnColdGet}}\n" +
		" On Put:\t{{$obj.OnPut}}\n" +
		" On LRU Eviction:\t{{$obj.OnLRUEviction}}\n"

	ConfigTmpl = GlobalConfTmpl +
		MirrorConfTmpl + LogConfTmpl + ClientConfTmpl + PeriodConfTmpl + TimeoutConfTmpl +
		ProxyConfTmpl + LRUConfTmpl + DiskConfTmpl + RebalanceConfTmpl +
		ReplicationConfTmpl + CksumConfTmpl + VerConfTmpl + FSpathsConfTmpl +
		TestFSPConfTmpl + NetConfTmpl + FSHCConfTmpl + AuthConfTmpl + KeepaliveConfTmpl +
		DownloaderConfTmpl + DSortConfTmpl +
		CompressionTmpl + ECTmpl

	BucketPropsSimpleTmpl = "PROPERTY\t VALUE\n" +
		"{{range $p := . }}" +
		"{{$p.Name}}\t {{$p.Value}}\n" +
		"{{end}}"

	DownloadListHeader = "JOB ID\t STATUS\t ERRORS\t DESCRIPTION\n"
	DownloadListBody   = "{{$value.ID}}\t " +
		"{{if $value.Aborted}}Aborted" +
		"{{else}}{{if $value.JobFinished}}Finished{{else}}{{$value.PendingCnt}} pending{{end}}" +
		"{{end}}\t {{$value.ErrorCnt}}\t {{$value.Description}}\n"
	DownloadListTmpl = DownloadListHeader + "{{ range $key, $value := . }}" + DownloadListBody + "{{end}}"

	DSortListHeader = "JOB ID\t STATUS\t START\t FINISH\t DESCRIPTION\n"
	DSortListBody   = "{{$value.ID}}\t " +
		"{{if $value.Aborted}}Aborted" +
		"{{else if $value.Archived}}Finished" +
		"{{else}}Running" +
		"{{end}}\t {{FormatTime $value.StartedTime}}\t {{FormatTime $value.FinishTime}} \t {{$value.Description}}\n"
	DSortListTmpl = DSortListHeader + "{{ range $value := . }}" + DSortListBody + "{{end}}"

	// Xactions templates
	XactionsBodyTmpl     = XactionsBaseBodyTmpl + XactionsExtBodyTmpl
	XactionsBaseBodyTmpl = XactionStatsHeader +
		"{{range $daemon := $.Stats }}" + XactionBody + "{{end}}"
	XactionStatsHeader = "DAEMON ID\t ID\t KIND\t BUCKET\t OBJECTS\t BYTES\t START\t END\t ABORTED\n"
	XactionBody        = "{{range $key, $xact := $daemon.Stats}}" + XactionStatsBody + "{{end}}" +
		"{{if $daemon.Stats}}\t \t \t \t \t \t \t \t{{if $.Verbose}} \t {{end}}\n{{end}}"
	XactionStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xact.IDX}}{{$xact.IDX}}{{else}}-{{end}}\t " +
		"{{$xact.KindX}}\t " +
		"{{if $xact.BckX.Name}}{{$xact.BckX.Name}}{{else}}-{{end}}\t " +
		"{{if (eq $xact.ObjCountX 0) }}-{{else}}{{$xact.ObjCountX}}{{end}}\t " +
		"{{if (eq $xact.BytesCountX 0) }}-{{else}}{{FormatBytesSigned $xact.BytesCountX 2}}{{end}}\t " +
		"{{FormatTime $xact.StartTimeX}}\t " +
		"{{if (IsUnsetTime $xact.EndTimeX)}}-{{else}}{{FormatTime $xact.EndTimeX}}{{end}}\t " +
		"{{$xact.AbortedX}}\n"
	XactionsExtBodyTmpl = "{{if $.Verbose }}" + // if not nil
		"\n{{range $daemon := $.Stats }}" +
		"{{if $daemon.Stats}}DAEMON ID\t {{$daemon.DaemonID}}\n" +
		"{{range $key, $xact := $daemon.Stats}}" +
		"{{range $name,$val := $xact.Ext}}{{ $name }}\t {{$val}}\n{{end}}" +
		"{{end}}\n" +
		"{{end}}" +
		"{{end}}{{end}}"

	// Buckets templates
	BucketsSummariesFastTmpl = "NAME\t EST. OBJECTS\t EST. SIZE\t EST. USED %\n" + bucketsSummariesBody
	BucketsSummariesTmpl     = "NAME\t OBJECTS\t SIZE \t USED %\n" + bucketsSummariesBody
	bucketsSummariesBody     = "{{range $k, $v := . }}" +
		"{{$v.Bck}}\t {{$v.ObjCount}}\t {{FormatBytesUnsigned $v.Size 2}}\t {{FormatFloat $v.UsedPct}}%\n" +
		"{{end}}"

	// For `object put` mass uploader. A caller adds to the template
	// total count and size. That is why the template ends with \t
	ExtensionTmpl = "Files to upload:\nEXTENSION\t COUNT\t SIZE\n" +
		"{{range $k, $v := . }}" +
		"{{$k}}\t {{$v.Cnt}}\t {{FormatBytesSigned $v.Size 2}}\n" +
		"{{end}}" +
		"TOTAL\t"

	ShortUsageTmpl = "{{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}}{{if .VisibleFlags}} [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{end}} - {{.Usage}}\n" +
		"\n\tCOMMANDS:\t" +
		"{{range .VisibleCategories}}" +
		"{{ range $index, $element := .VisibleCommands}}" +
		"{{if $index}}, {{end}}" +
		"{{if and ( eq ( Mod $index 13 ) 0 ) ( ne $index 0 ) }}\n\t\t{{end}}" + // don't print everything in a single line
		"{{$element.Name}}" +
		"{{end}}{{end}}\n" +
		"{{if .VisibleFlags}}\tOPTIONS:\t" +
		"{{ range $index, $flag := .VisibleFlags}}" +
		"{{if $index}}, {{end}}" +
		"--{{FlagName $flag }}" +
		"{{end}}{{end}}\n"

	AuthNClusterTmpl = "CLUSTER ID\tALIAS\tURLs\n" +
		"{{ range $clu := . }}" +
		"{{ $clu.ID }}\t{{ $clu.Alias }}\t{{ JoinList $clu.URLs }}\n" +
		"{{end}}"

	AuthNRoleTmpl = "ROLE\tDESCRIPTION\n" +
		"{{ range $role := . }}" +
		"{{ $role.Name }}\t{{ $role.Desc }}\n" +
		"{{end}}"

	AuthNUserTmpl = "NAME\tROLES\n" +
		"{{ range $user := . }}" +
		"{{ $user.ID }}\t{{ JoinList $user.Roles }}\n" +
		"{{end}}"

	// Command `search`
	SearchTmpl = "{{ JoinListNL . }}\n"

	// Command `transform`
	TransformListTmpl = "ID\tNAME\n" +
		"{{range $transform := .}}" +
		"{{$transform.ID}}\t{{$transform.Name}}\n" +
		"{{end}}"

	// Command `show mountpath`
	TargetMpathListTmpl = "{{range $p := . }}" +
		"{{ $p.DaemonID }}\n" +
		"{{if and (eq (len $p.Available) 0) (eq (len $p.Disabled) 0)}}" +
		"\tNo mountpath\n" +
		"{{else}}" +
		"{{if ne (len $p.Available) 0}}" +
		"\tAvailable:\n" +
		"{{range $mp := $p.Available }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{if ne (len $p.Disabled) 0}}" +
		"\tDisabled:\n" +
		"{{range $mp := $p.Disabled }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{end}}{{end}}"
)

var (
	// ObjectPropsMap matches BucketEntry field
	ObjectPropsMap = map[string]string{
		"name":       "{{$obj.Name}}",
		"size":       "{{FormatBytesSigned $obj.Size 2}}",
		"checksum":   "{{$obj.Checksum}}",
		"type":       "{{$obj.Type}}",
		"atime":      "{{$obj.Atime}}",
		"version":    "{{$obj.Version}}",
		"target_url": "{{$obj.TargetURL}}",
		"status":     "{{FormatObjStatus $obj}}",
		"copies":     "{{$obj.Copies}}",
		"cached":     "{{FormatObjIsCached $obj}}",
	}

	ObjStatMap = map[string]string{
		"name":     "{{.Bck.String}}/{{.Name}}",
		"cached":   "{{FormatBool .Present}}",
		"size":     "{{FormatBytesSigned .Size 2}}",
		"version":  "{{.Version}}",
		"atime":    "{{if (eq .Atime 0)}}-{{else}}{{FormatUnixNano .Atime}}{{end}}",
		"copies":   "{{if .NumCopies}}{{.NumCopies}}{{else}}-{{end}}",
		"checksum": "{{if .Checksum.Value}}{{.Checksum.Value}}{{else}}-{{end}}",
		"ec":       "{{if (eq .DataSlices 0)}}-{{else}}{{FormatEC .DataSlices .ParitySlices .IsECCopy}}{{end}}",
	}

	funcMap = template.FuncMap{
		"ExtractStat":         extractStat,
		"FormatBytesSigned":   cmn.B2S,
		"FormatBytesUnsigned": cmn.UnsignedB2S,
		"CalcCap":             calcCap,
		"IsUnsetTime":         isUnsetTime,
		"FormatTime":          fmtTime,
		"FormatUnixNano":      func(t int64) string { return cmn.FormatUnixNano(t, "") },
		"FormatEC":            fmtEC,
		"FormatDur":           fmtDuration,
		"FormatXactStatus":    fmtXactStatus,
		"FormatObjStatus":     fmtObjStatus,
		"FormatObjIsCached":   fmtObjIsCached,
		"FormatDaemonID":      fmtDaemonID,
		"FormatFloat":         func(f float64) string { return fmt.Sprintf("%.2f", f) },
		"FormatBool":          fmtBool,
		"JoinList":            fmtStringList,
		"JoinListNL":          func(lst []string) string { return fmtStringListGeneric(lst, "\n") },
		"FormatFeatureFlags":  fmtFeatureFlags,
		"CountNonElectable":   countNonElectable,
	}

	HelpTemplateFuncMap = template.FuncMap{
		"FlagName": func(f cli.Flag) string { return strings.SplitN(f.GetName(), ",", 2)[0] },
		"Mod":      func(a, mod int) int { return a % mod },
	}
)

type (
	// Used to return specific fields/objects for marshaling (MarshalIdent).
	forMarshaler interface {
		forMarshal() interface{}
	}

	DiskStatsTemplateHelper struct {
		TargetID string
		DiskName string
		Stat     *ios.SelectedDiskStats
	}

	ObjectStatTemplateHelper struct {
		Name  string
		Props *cmn.ObjectProps
	}

	SmapTemplateHelper struct {
		Smap         *cluster.Smap
		ExtendedURLs bool
	}

	DaemonStatusTemplateHelper struct {
		Pmap map[string]*stats.DaemonStatus `json:"pmap"`
		Tmap map[string]*stats.DaemonStatus `json:"tmap"`
	}

	StatusTemplateHelper struct {
		Smap   *cluster.Smap              `json:"smap"`
		Status DaemonStatusTemplateHelper `json:"status"`
	}
)

// interface guard
var _ forMarshaler = SmapTemplateHelper{}

func (sth SmapTemplateHelper) forMarshal() interface{} {
	return sth.Smap
}

// Gets the associated value from CoreStats
func extractStat(daemon *stats.CoreStats, statName string) int64 {
	if daemon == nil {
		return 0
	}
	return daemon.Tracker[statName].Value
}

func calcCap(daemon *stats.DaemonStatus, option string) (total uint64) {
	for _, fs := range daemon.Capacity {
		switch option {
		case "capacity":
			total += fs.Avail
		case "percent":
			total += uint64(fs.PctUsed)
		}
	}

	switch option {
	case "capacity":
		return total
	case "percent":
		if len(daemon.Capacity) == 0 {
			return 0
		}
		return total / uint64(len(daemon.Capacity))
	}

	return 0
}

func fmtXactStatus(tStatus *stats.TargetStatus) string {
	if tStatus == nil || tStatus.RebalanceStats == nil {
		return "not started"
	}

	tStats := tStatus.RebalanceStats
	if tStats.Aborted() {
		return fmt.Sprintf("aborted; %d moved (%s)", tStats.ObjCount(), cmn.B2S(tStats.BytesCount(), 1))
	}
	if tStats.EndTime().IsZero() {
		return fmt.Sprintf("running; %d moved (%s)", tStats.ObjCount(), cmn.B2S(tStats.BytesCount(), 1))
	}
	return fmt.Sprintf("finished; %d moved (%s)", tStats.ObjCount(), cmn.B2S(tStats.BytesCount(), 1))
}

func fmtObjStatus(obj *cmn.BucketEntry) string {
	if obj.IsStatusOK() {
		return "ok"
	}
	return "moved"
}

var ConfigSectionTmpl = map[string]string{
	"global":               GlobalConfTmpl,
	"mirror":               MirrorConfTmpl,
	"log":                  LogConfTmpl,
	"client":               ClientConfTmpl,
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
	"replication":          ReplicationConfTmpl,
}

func fmtObjIsCached(obj *cmn.BucketEntry) string {
	return fmtBool(obj.CheckExists())
}

func fmtBool(t bool) string {
	if t {
		return "yes"
	}
	return "no"
}

func isUnsetTime(t time.Time) bool {
	return t.IsZero()
}

func fmtTime(t time.Time) string {
	return t.Format("01-02 15:04:05")
}

func fmtEC(data, parity int, isCopy bool) string {
	info := fmt.Sprintf("%d:%d", data, parity)
	if isCopy {
		info += "[replicated]"
	} else {
		info += "[encoded]"
	}
	return info
}

func fmtDuration(ns int64) string { return duration.HumanDuration(time.Duration(ns)) }

func fmtDaemonID(id string, smap cluster.Smap) string {
	si := smap.GetNode(id)
	if id == smap.Primary.ID() {
		return id + primarySuffix
	}
	if si.Flags.IsSet(cluster.SnodeNonElectable) {
		return id + nonElectableSuffix
	}
	return id
}

// Displays the output in either JSON or tabular form
// if formatJSON == true, outputTemplate is omitted
func DisplayOutput(object interface{}, writer io.Writer, outputTemplate string, formatJSON ...bool) error {
	useJSON := false
	if len(formatJSON) > 0 {
		useJSON = formatJSON[0]
	}

	if useJSON {
		if o, ok := object.(forMarshaler); ok {
			object = o.forMarshal()
		}
		out, err := jsoniter.MarshalIndent(object, "", "    ")
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

func fmtStringList(lst []string) string {
	if len(lst) == 0 {
		return "-"
	}
	return fmtStringListGeneric(lst, ",")
}

func fmtStringListGeneric(lst []string, sep string) string {
	var s strings.Builder
	for idx, url := range lst {
		if idx != 0 {
			fmt.Fprint(&s, sep)
		}
		fmt.Fprint(&s, url)
	}
	return s.String()
}

func fmtFeatureFlags(flags cmn.FeatureFlags) string {
	if flags == 0 {
		return "-"
	}
	return fmt.Sprintf("%s(%s)", flags, flags.Describe())
}

func countNonElectable(nodeMap cluster.NodeMap) int {
	cnt := 0
	for _, si := range nodeMap {
		if si.NonElectable() {
			cnt++
		}
	}
	return cnt
}
