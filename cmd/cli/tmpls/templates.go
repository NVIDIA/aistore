// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"k8s.io/apimachinery/pkg/util/duration"
)

const (
	unknownVal = "-"
	NotSetVal  = "-"
)

const rebalanceExpirationTime = 5 * time.Minute

// Templates for output
// ** Changing the structure of the objects server side needs to make sure that this will still work **
const (
	primarySuffix      = "[P]"
	nonElectableSuffix = "[-]"

	xactStateFinished = "Finished"
	xactStateRunning  = "Running"
	xactStateIdle     = "Idle"
	xactStateAborted  = "Aborted"

	// Smap
	SmapHeader = "NODE\t TYPE\t PUBLIC URL" +
		"{{ if (eq $.ExtendedURLs true) }}\t INTRA CONTROL URL\t INTRA DATA URL{{end}}" +
		"\n"
	SmapBody = "{{FormatDaemonID $value.ID $.Smap}}\t {{$value.DaeType}}\t {{$value.PubNet.URL}}" +
		"{{ if (eq $.ExtendedURLs true) }}\t {{$value.ControlNet.URL}}\t {{$value.DataNet.URL}}{{end}}" +
		"\n"

	SmapTmpl = SmapHeader +
		"{{ range $key, $value := .Smap.Pmap }}" + SmapBody + "{{end}}\n" +
		SmapHeader +
		"{{ range $key, $value := .Smap.Tmap }}" + SmapBody + "{{end}}\n" +
		"Non-Electable:\n" +
		"{{ range $key, $si := .Smap.Pmap }} " +
		"{{ $nonElect := $.Smap.NonElectable $si }}" +
		"{{ if (eq $nonElect true) }} ProxyID: {{$key}}\n{{end}}{{end}}\n" +
		"Primary Proxy: {{.Smap.Primary.ID}}\nProxies: {{len .Smap.Pmap}}\t Targets: {{len .Smap.Tmap}}\t Smap Version: {{.Smap.Version}}\n"

	//////////////////
	// Cluster info //
	//////////////////

	ClusterSummary = "Summary:\n  Proxies:\t{{len .Smap.Pmap}} ({{ .Smap.CountNonElectable }} unelectable)\n  " +
		"Targets:\t{{len .Smap.Tmap}}\n  " +
		"Primary:\t{{.Smap.Primary.StringEx}}\n  " +
		"Smap:\t{{FormatSmapVersion .Smap.Version}}\n  " +
		"Deployment:\t{{ ( Deployments .Status) }}\n  " +
		"Status:\t{{ ( OnlineStatus .Status) }}\n  " +
		"Rebalance:\t{{ ( Rebalance .Status) }}\n  " +
		"Authentication:\t{{ .CluConfig.Auth.Enabled }}\n  " +
		"Version:\t{{ ( Versions .Status) }}\n  " +
		"Build:\t{{ ( BuildTimes .Status) }}\n"

	// Disk Stats
	DiskStatsHeader = "TARGET\t DISK\t READ\t WRITE\t UTIL %\n"

	DiskStatsBody = "{{ $value.TargetID }}\t " +
		"{{ $value.DiskName }}\t " +
		"{{ $stat := $value.Stat }}" +
		"{{ FormatBytesSig $stat.RBps 2 }}/s\t " +
		"{{ FormatBytesSig $stat.WBps 2 }}/s\t " +
		"{{ $stat.Util }}%\n"

	DiskStatBodyTmpl  = "{{ range $key, $value := . }}" + DiskStatsBody + "{{ end }}"
	DiskStatsFullTmpl = DiskStatsHeader + DiskStatBodyTmpl

	// Config
	ConfigTmpl = "PROPERTY\t VALUE\n{{range $item := .}}" +
		"{{ $item.Name }}\t {{ $item.Value }}\n" +
		"{{end}}\n"

	DaemonConfigTmpl = "{{ if .ClusterConfigDiff }}PROPERTY\t VALUE\t DEFAULT\n{{range $item := .ClusterConfigDiff }}" +
		"{{ $item.Name }}\t {{ $item.Current }}\t {{ $item.Old }}\n" +
		"{{end}}\n{{end}}" +
		"{{ if .LocalConfigPairs }}PROPERTY\t VALUE\n" +
		"{{range $item := .LocalConfigPairs }}" +
		"{{ $item.Name }}\t {{ $item.Value }}\n" +
		"{{end}}\n{{end}}"

	PropsSimpleTmpl = "PROPERTY\t VALUE\n" +
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
	XactionsBodyTmpl = XactionStatsHeader + XactionsBodyNoHeaderTmpl

	XactionsBodyNoHeaderTmpl = "{{range $daemon := . }}" + XactionBody + "{{end}}"
	XactionStatsHeader       = "NODE\t ID\t KIND\t BUCKET\t OBJECTS\t BYTES\t START\t END\t STATE\n"
	XactionBody              = "{{range $key, $xctn := $daemon.XactSnaps}}" + XactionStatsBody + "{{end}}"
	XactionStatsBody         = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{$xctn.Kind}}\t " +
		"{{if $xctn.Bck.Name}}{{FormatBckName $xctn.Bck}}{{else}}-{{end}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +
		"{{FormatTime $xctn.StartTime}}\t " +
		"{{if (IsUnsetTime $xctn.EndTime)}}-{{else}}{{FormatTime $xctn.EndTime}}{{end}}\t " +
		"{{FormatXactState $xctn}}\n"

	XactionECGetStatsHeader = "NODE\t ID\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t START\t END\t ABORTED\n"
	XactionECGetBodyTmpl    = XactionECGetStatsHeader +
		"{{range $daemon := . }}" + XactionECGetBody + "{{end}}"
	XactionECGetBody      = "{{range $key, $xctn := $daemon.XactSnaps}}" + XactionECGetStatsBody + "{{end}}"
	XactionECGetStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{if $xctn.Bck.Name}}{{FormatBckName $xctn.Bck}}{{else}}-{{end}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +

		"{{ $ext := ExtECGetStats $xctn }}" +
		"{{if (eq $ext.ErrCount 0) }}-{{else}}{{$ext.ErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +

		"{{FormatTime $xctn.StartTime}}\t " +
		"{{if (IsUnsetTime $xctn.EndTime)}}-{{else}}{{FormatTime $xctn.EndTime}}{{end}}\t " +
		"{{$xctn.AbortedX}}\n"

	XactionECPutStatsHeader = "NODE\t ID\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t ENC TIME\t START\t END\t ABORTED\n"
	XactionECPutBodyTmpl    = XactionECPutStatsHeader +
		"{{range $daemon := . }}" + XactionECPutBody + "{{end}}"
	XactionECPutBody      = "{{range $key, $xctn := $daemon.XactSnaps}}" + XactionECPutStatsBody + "{{end}}"
	XactionECPutStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{if $xctn.Bck.Name}}{{FormatBckName $xctn.Bck}}{{else}}-{{end}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +

		"{{ $ext := ExtECPutStats $xctn }}" +
		"{{if (eq $ext.EncodeErrCount 0) }}-{{else}}{{$ext.EncodeErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +
		"{{if (eq $ext.AvgEncodeTime 0) }}-{{else}}{{FormatMilli $ext.AvgEncodeTime}}{{end}}\t " +

		"{{FormatTime $xctn.StartTime}}\t " +
		"{{if (IsUnsetTime $xctn.EndTime)}}-{{else}}{{FormatTime $xctn.EndTime}}{{end}}\t " +
		"{{$xctn.AbortedX}}\n"

	ListBucketsHeader = "NAME\t PRESENT\t OBJECTS (cached, remote)\t TOTAL SIZE (apparent, objects)\t USAGE(%)\n"
	ListBucketsBody   = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{FormatBool $v.Info.IsBckPresent}}\t " +
		"{{if (IsFalse $v.Info.IsBckPresent)}}-{{else}}{{$v.Info.ObjCount.Present}} {{$v.Info.ObjCount.Remote}}{{end}}\t " +
		"{{if (IsFalse $v.Info.IsBckPresent)}}-{{else}}{{FormatBytesUns $v.Info.TotalSize.OnDisk 2}} {{FormatBytesUns $v.Info.TotalSize.PresentObjs 2}}{{end}}\t " +
		"{{if (IsFalse $v.Info.IsBckPresent)}}-{{else}}{{$v.Info.UsedPct}}%{{end}}\n" +
		"{{end}}"
	ListBucketsTmpl = ListBucketsHeader + ListBucketsBody

	ListBucketsHeaderNoSummary = "NAME\t PRESENT\n"
	ListBucketsBodyNoSummary   = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{FormatBool $v.Info.IsBckPresent}}\n" +
		"{{end}}"
	ListBucketsTmplNoSummary = ListBucketsHeaderNoSummary + ListBucketsBodyNoSummary

	// Bucket summary templates
	BucketsSummariesFastTmpl = "NAME\t APPARENT SIZE\t USAGE(%)\n" + bucketsSummariesFastBody
	bucketsSummariesFastBody = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{FormatBytesUns $v.TotalSize.OnDisk 2}}\t {{$v.UsedPct}}%\n" +
		"{{end}}"
	BucketsSummariesTmpl = "NAME\t OBJECTS (cached, remote)\t OBJECT SIZES (min, avg, max)\t TOTAL OBJECT SIZE (cached, remote)\t USAGE(%)\n" +
		bucketsSummariesBody
	bucketsSummariesBody = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{$v.ObjCount.Present}} {{$v.ObjCount.Remote}}\t " +
		"{{FormatMAM $v.ObjSize.Min}} {{FormatMAM $v.ObjSize.Avg}} {{FormatMAM $v.ObjSize.Max}}\t " +
		"{{FormatBytesUns $v.TotalSize.PresentObjs 2}} {{FormatBytesUns $v.TotalSize.RemoteObjs 2}}\t {{$v.UsedPct}}%\n" +
		"{{end}}"

	BucketSummaryValidateTmpl = "BUCKET\t OBJECTS\t MISPLACED\t MISSING COPIES\n" + bucketSummaryValidateBody
	bucketSummaryValidateBody = "{{range $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{$v.ObjectCnt}}\t {{$v.Misplaced}}\t {{$v.MissingCopies}}\n" +
		"{{end}}"

	// For `object put` mass uploader. A caller adds to the template
	// total count and size. That is why the template ends with \t
	ExtensionTmpl = "Files to upload:\nEXTENSION\t COUNT\t SIZE\n" +
		"{{range $k, $v := . }}" +
		"{{$k}}\t {{$v.Cnt}}\t {{FormatBytesSig $v.Size 2}}\n" +
		"{{end}}" +
		"TOTAL\t"

	ShortUsageTmpl = "{{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}}{{if .VisibleFlags}} [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{end}} - {{.Usage}}\n" +
		"\n\tCOMMANDS:\t" +
		"{{range .VisibleCategories}}" +
		"{{ range $index, $element := .VisibleCommands}}" +
		"{{if $index}}, {{end}}" +
		"{{if ( eq ( Mod $index 13 ) 12 ) }}\n\t\t{{end}}" + // limit the number printed per line
		"{{$element.Name}}" +
		"{{if ( eq $element.Name \"search\" ) }}\n\t\t{{end}}" + // circumvent $index wrap around for aliases
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
		"{{ $role.ID }}\t{{ $role.Desc }}\n" +
		"{{end}}"

	AuthNUserTmpl = "NAME\tROLES\n" +
		"{{ range $user := . }}" +
		"{{ $user.ID }}\t{{ JoinList $user.Roles }}\n" +
		"{{end}}"

	AuthNUserVerboseTmpl = "Name\t{{ .ID }}\n" +
		"Roles\t{{ JoinList .Roles }}\n" +
		"{{ if ne (len .ClusterACLs) 0 }}" +
		"CLUSTER ID\tALIAS\tPERMISSIONS\n" +
		"{{ range $clu := .ClusterACLs}}" +
		"{{ $clu.ID}}\t{{ $clu.Alias }}\t{{ FormatACL $clu.Access }}\n" +
		"{{end}}{{end}}" +
		"{{ if ne (len .BucketACLs) 0 }}" +
		"BUCKET\tPERMISSIONS\n" +
		"{{ range $bck := .BucketACLs}}" +
		"{{ $bck }}\t{{ FormatACL $bck.Access }}\n" +
		"{{end}}{{end}}"

	AuthNRoleVerboseTmpl = "Role\t{{ .ID }}\n" +
		"Description\t{{ .Desc }}\n" +
		"{{ if ne (len .Roles) 0 }}" +
		"Roles\t{{ JoinList .Roles }}\n" +
		"{{ end }}" +
		"{{ if ne (len .ClusterACLs) 0 }}" +
		"CLUSTER ID\tALIAS\tPERMISSIONS\n" +
		"{{ range $clu := .ClusterACLs}}" +
		"{{ $clu.ID}}\t{{ $clu.Alias }}\t{{ FormatACL $clu.Access }}\n" +
		"{{end}}{{end}}" +
		"{{ if ne (len .BucketACLs) 0 }}" +
		"BUCKET\tPERMISSIONS\n" +
		"{{ range $bck := .BucketACLs}}" +
		"{{ $bck }}\t{{ FormatACL $bck.Access }}\n" +
		"{{end}}{{end}}"

	// Command `search`
	SearchTmpl = "{{ JoinListNL . }}\n"

	// Command `transform`
	TransformListTmpl = "ID\n" +
		"{{range $transform := .}}" +
		"{{$transform.ID}}\n" +
		"{{end}}"

	// Command `show mountpath`
	TargetMpathListTmpl = "{{range $p := . }}" +
		"{{ $p.DaemonID }}\n" +
		"{{if and (eq (len $p.Mpl.Available) 0) (eq (len $p.Mpl.Disabled) 0)}}" +
		"\tNo mountpaths\n" +
		"{{else}}" +
		"{{if ne (len $p.Mpl.Available) 0}}" +
		"\tAvailable:\n" +
		"{{range $mp := $p.Mpl.Available }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{if ne (len $p.Mpl.Disabled) 0}}" +
		"\tDisabled:\n" +
		"{{range $mp := $p.Mpl.Disabled }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{if ne (len $p.Mpl.WaitingDD) 0}}" +
		"\tTransitioning to disabled or detached pending resilver:\n" +
		"{{range $mp := $p.Mpl.WaitingDD }}" +
		"\t\t{{ $mp }}\n" +
		"{{end}}{{end}}" +
		"{{end}}{{end}}"
)

var (
	// ObjectPropsMap matches ObjEntry field
	ObjectPropsMap = map[string]string{
		apc.GetPropsName:     "{{FormatNameArch $obj.Name $obj.Flags}}",
		apc.GetPropsSize:     "{{FormatBytesSig $obj.Size 2}}",
		apc.GetPropsChecksum: "{{$obj.Checksum}}",
		apc.GetPropsAtime:    "{{$obj.Atime}}",
		apc.GetPropsVersion:  "{{$obj.Version}}",
		apc.GetPropsLocation: "{{$obj.Location}}",
		apc.GetPropsCustom:   "{{FormatObjCustom $obj.Custom}}",
		apc.GetPropsStatus:   "{{FormatObjStatus $obj}}",
		apc.GetPropsCopies:   "{{$obj.Copies}}",
		apc.GetPropsCached:   "{{FormatObjIsCached $obj}}",
	}

	funcMap = template.FuncMap{
		"FormatBytesSig":    cos.B2S,
		"FormatBytesUns":    cos.UnsignedB2S,
		"FormatMAM":         func(u int64) string { return fmt.Sprintf("%-10s", cos.B2S(u, 2)) },
		"IsUnsetTime":       isUnsetTime,
		"IsFalse":           func(v bool) bool { return !v },
		"FormatTime":        fmtTime,
		"FormatUnixNano":    func(t int64) string { return cos.FormatUnixNano(t, "") },
		"FormatEC":          FmtEC,
		"FormatDur":         fmtDuration,
		"FormatObjStatus":   fmtObjStatus,
		"FormatObjCustom":   fmtObjCustom,
		"FormatObjIsCached": fmtObjIsCached,
		"FormatDaemonID":    fmtDaemonID,
		"FormatSmapVersion": fmtSmapVer,
		"FormatFloat":       func(f float64) string { return fmt.Sprintf("%.2f", f) },
		"FormatBool":        FmtBool,
		"FormatBckName":     func(bck cmn.Bck) string { return bck.DisplayName() },
		"FormatMilli":       fmtMilli,
		"JoinList":          fmtStringList,
		"JoinListNL":        func(lst []string) string { return fmtStringListGeneric(lst, "\n") },
		"FormatACL":         fmtACL,
		"ExtECGetStats":     extECGetStats,
		"ExtECPutStats":     extECPutStats,
		"FormatNameArch":    fmtNameArch,
		"FormatXactState":   fmtXactStatus,
		// for all stats.DaemonStatus structs in `h`: select specific field
		// and make a slice, and then a string out of it
		"OnlineStatus": func(h DaemonStatusTemplateHelper) string { return toString(h.onlineStatus()) },
		"Deployments":  func(h DaemonStatusTemplateHelper) string { return toString(h.deployments()) },
		"Versions":     func(h DaemonStatusTemplateHelper) string { return toString(h.versions()) },
		"BuildTimes":   func(h DaemonStatusTemplateHelper) string { return toString(h.buildTimes()) },
		"Rebalance":    func(h DaemonStatusTemplateHelper) string { return toString(h.rebalance()) },
	}

	AliasTemplate = "ALIAS\tCOMMAND\n{{range $alias := .}}" +
		"{{ $alias.Name }}\t{{ $alias.Value }}\n" +
		"{{end}}"

	HelpTemplateFuncMap = template.FuncMap{
		"FlagName": func(f cli.Flag) string { return strings.SplitN(f.GetName(), ",", 2)[0] },
		"Mod":      func(a, mod int) int { return a % mod },
	}
)

type (
	// Used to return specific fields/objects for marshaling (MarshalIdent).
	forMarshaler interface {
		forMarshal() any
	}
	DiskStatsTemplateHelper struct {
		TargetID string
		DiskName string
		Stat     ios.DiskStats
	}
	SmapTemplateHelper struct {
		Smap         *cluster.Smap
		ExtendedURLs bool
	}
	DaemonStatusTemplateHelper struct {
		Pmap stats.DaemonStatusMap `json:"pmap"`
		Tmap stats.DaemonStatusMap `json:"tmap"`
	}
	StatusTemplateHelper struct {
		Smap      *cluster.Smap              `json:"smap"`
		CluConfig *cmn.ClusterConfig         `json:"config"`
		Status    DaemonStatusTemplateHelper `json:"status"`
	}
	ListBucketsTemplateHelper struct {
		Bck   cmn.Bck
		Props *cmn.BucketProps
		Info  *cmn.BsummResult
	}
)

// interface guard
var _ forMarshaler = SmapTemplateHelper{}

func (sth SmapTemplateHelper) forMarshal() any {
	return sth.Smap
}

// Gets the associated value from CoreStats
func extractStat(daemon *stats.CoreStats, statName string) int64 {
	if daemon == nil {
		return 0
	}
	return daemon.Tracker[statName].Value
}

func calcCapPercentage(daemon *stats.DaemonStatus) (total float64) {
	for _, fs := range daemon.Capacity {
		total += float64(fs.PctUsed)
	}

	if len(daemon.Capacity) == 0 {
		return 0
	}
	return total / float64(len(daemon.Capacity))
}

func calcCap(daemon *stats.DaemonStatus) (total uint64) {
	for _, fs := range daemon.Capacity {
		total += fs.Avail
	}
	return total
}

func fmtObjStatus(obj *cmn.LsoEntry) string {
	switch obj.Status() {
	case apc.LocOK:
		return "ok"
	case apc.LocMisplacedNode:
		return "misplaced(cluster)"
	case apc.LocMisplacedMountpath:
		return "misplaced(mountpath)"
	case apc.LocIsCopy:
		return "replica"
	case apc.LocIsCopyMissingObj:
		return "replica(object-is-missing)"
	default:
		debug.Assertf(false, "%#v", obj)
		return "invalid"
	}
}

func fmtObjIsCached(obj *cmn.LsoEntry) string {
	return FmtBool(obj.CheckExists())
}

// FmtBool returns "yes" if true, else "no"
func FmtBool(t bool) string {
	if t {
		return "yes"
	}
	return "no"
}

// see also cli.isUnsetTime
func isUnsetTime(t time.Time) bool {
	return t.IsZero()
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return NotSetVal
	}
	return t.Format("01-02 15:04:05")
}

func fmtObjCustom(custom string) string {
	if custom != "" {
		return custom
	}
	return NotSetVal
}

// FmtCopies formats an int to a string, where 0 becomes "-"
func FmtCopies(copies int) string {
	if copies == 0 {
		return unknownVal
	}
	return fmt.Sprint(copies)
}

// FmtEC formats EC data (DataSlices, ParitySlices, IsECCopy) into a
// readable string for CLI, e.g. "1:2[encoded]"
func FmtEC(gen int64, data, parity int, isCopy bool) string {
	if data == 0 {
		return unknownVal
	}
	info := fmt.Sprintf("%d:%d (gen %d)", data, parity, gen)
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
	if smap.NonElectable(si) {
		return id + nonElectableSuffix
	}
	return id
}

func fmtSmapVer(v int64) string { return fmt.Sprintf("v%d", v) }

// Main function to print formatted output
// NOTE: if useJSON, outputTemplate is ignored
func Print(object any, writer io.Writer, outputTemplate string, altMap template.FuncMap, useJSON bool) error {
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

	fmap := funcMap
	if altMap != nil {
		fmap = make(template.FuncMap, len(funcMap))
		for k, v := range funcMap {
			if altv, ok := altMap[k]; ok {
				fmap[k] = altv
			} else {
				fmap[k] = v
			}
		}
	}
	tmpl, err := template.New("DisplayTemplate").Funcs(fmap).Parse(outputTemplate)
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
		return unknownVal
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

// for all stats.DaemonStatus structs: select specific field and append to the returned slice
// (using the corresponding jtags here for no particular reason)
func (h *DaemonStatusTemplateHelper) onlineStatus() []string { return h.toSlice("status") }
func (h *DaemonStatusTemplateHelper) deployments() []string  { return h.toSlice("deployment") }
func (h *DaemonStatusTemplateHelper) versions() []string     { return h.toSlice("ais_version") }
func (h *DaemonStatusTemplateHelper) buildTimes() []string   { return h.toSlice("build_time") }
func (h *DaemonStatusTemplateHelper) rebalance() []string    { return h.toSlice("rebalance_snap") }
func (h *DaemonStatusTemplateHelper) pods() []string         { return h.toSlice("k8s_pod_name") }

// internal helper for the methods above
func (h *DaemonStatusTemplateHelper) toSlice(jtag string) []string {
	set := cos.NewStrSet()
	for _, m := range []stats.DaemonStatusMap{h.Pmap, h.Tmap} {
		for _, s := range m {
			switch jtag {
			case "status":
				set.Add(s.Status)
			case "deployment":
				set.Add(s.DeploymentType)
			case "ais_version":
				set.Add(s.Version)
			case "build_time":
				set.Add(s.BuildTime)
			case "k8s_pod_name":
				set.Add(s.K8sPodName)
			case "rebalance_snap":
				if s.RebSnap != nil {
					set.Add(fmtRebStatus(s.RebSnap))
				}
			default:
				debug.Assert(false, jtag)
			}
		}
	}
	return set.ToSlice()
}

// internal helper for the methods above
func toString(lst []string) string {
	switch len(lst) {
	case 0:
		return NotSetVal
	case 1:
		return lst[0]
	default:
		return "[" + strings.Join(lst, ", ") + "]"
	}
}

func fmtACL(acl apc.AccessAttrs) string {
	if acl == 0 {
		return unknownVal
	}
	return acl.Describe()
}

func extECGetStats(base *xact.SnapExt) *ec.ExtECGetStats {
	ecGet := &ec.ExtECGetStats{}
	if err := cos.MorphMarshal(base.Ext, ecGet); err != nil {
		return &ec.ExtECGetStats{}
	}
	return ecGet
}

func extECPutStats(base *xact.SnapExt) *ec.ExtECPutStats {
	ecPut := &ec.ExtECPutStats{}
	if err := cos.MorphMarshal(base.Ext, ecPut); err != nil {
		return &ec.ExtECPutStats{}
	}
	return ecPut
}

func fmtMilli(val cos.Duration) string {
	return cos.FormatMilli(time.Duration(val))
}

func fmtNameArch(val string, flags uint16) string {
	if flags&apc.EntryInArch == 0 {
		return val
	}
	return "    " + val
}

func fmtRebStatus(rebSnap *stats.RebalanceSnap) string {
	if rebSnap == nil {
		return unknownVal
	}
	if rebSnap.IsAborted() {
		return fmt.Sprintf("aborted(%s)", rebSnap.ID)
	}
	if rebSnap.EndTime.IsZero() {
		return fmt.Sprintf("running(%s)", rebSnap.ID)
	}
	if time.Since(rebSnap.EndTime) < rebalanceExpirationTime {
		return fmt.Sprintf("finished(%s)", rebSnap.ID)
	}
	return unknownVal
}

func fmtXactStatus(xctn *xact.SnapExt) string {
	if xctn.AbortedX {
		return xactStateAborted
	}
	if !xctn.EndTime.IsZero() {
		return xactStateFinished
	}
	if xctn.Idle() {
		return xactStateIdle
	}
	return xactStateRunning
}

func AltFuncMapSizeBytes() (m template.FuncMap) {
	m = make(template.FuncMap, 2)
	m["FormatBytesSig"] = func(b int64, _ int) string { return strconv.FormatInt(b, 10) }
	m["FormatBytesUns"] = func(b uint64, _ int) string { return strconv.FormatInt(int64(b), 10) }
	m["FormatMAM"] = func(u int64) string { return fmt.Sprintf("%-10d", u) }
	return
}
