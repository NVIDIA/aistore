// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
)

const (
	unknownVal = "-"
	NotSetVal  = "-"
)

const rebalanceExpirationTime = 5 * time.Minute

const (
	primarySuffix       = "[P]"
	nonElectableSuffix  = "[n/e]"
	specialStatusSuffix = "[x]" // (daeStatus, via apc.WhatNodeStatsAndStatus)

	NodeOnline = "online"
)

const (
	xactStateFinished = "Finished"
	xactStateRunning  = "Running"
	xactStateIdle     = "Idle"
	xactStateAborted  = "Aborted"
)

// output templates
const (
	// Smap
	smapHdr = "NODE\t TYPE\t PUBLIC URL" +
		"{{ if (eq $.ExtendedURLs true) }}\t INTRA CONTROL URL\t INTRA DATA URL{{end}}" +
		"\n"
	smapBody = "{{FormatDaemonID $value.ID $.Smap}}\t {{$value.DaeType}}\t {{$value.PubNet.URL}}" +
		"{{ if (eq $.ExtendedURLs true) }}\t {{$value.ControlNet.URL}}\t {{$value.DataNet.URL}}{{end}}" +
		"\n"

	SmapTmpl = smapHdr +
		"{{ range $key, $value := .Smap.Pmap }}" + smapBody + "{{end}}\n" +
		smapHdr +
		"{{ range $key, $value := .Smap.Tmap }}" + smapBody + "{{end}}\n" +
		"Non-Electable:\n" +
		"{{ range $key, $si := .Smap.Pmap }} " +
		"{{ $nonElect := $.Smap.NonElectable $si }}" +
		"{{ if (eq $nonElect true) }} ProxyID: {{$key}}\n{{end}}{{end}}\n" +
		"Primary Proxy: {{.Smap.Primary.ID}}\nProxies: {{len .Smap.Pmap}}\t Targets: {{len .Smap.Tmap}}\t Smap Version: {{.Smap.Version}}\n"

	//
	// Cluster
	//

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

	//
	// special xactions & dsort
	//

	downloadListHdr  = "JOB ID\t XACTION\t STATUS\t ERRORS\t DESCRIPTION\n"
	downloadListBody = "{{$value.ID}}\t " +
		"{{$value.XactID}}\t " +
		"{{if $value.Aborted}}Aborted" +
		"{{else}}{{if $value.JobFinished}}Finished{{else}}{{$value.PendingCnt}} pending{{end}}" +
		"{{end}}\t {{$value.ErrorCnt}}\t {{$value.Description}}\n"
	DownloadListNoHdrTmpl = "{{ range $key, $value := . }}" + downloadListBody + "{{end}}"
	DownloadListTmpl      = downloadListHdr + DownloadListNoHdrTmpl

	dsortListHdr  = "JOB ID\t STATUS\t START\t FINISH\t DESCRIPTION\n"
	dsortListBody = "{{$value.ID}}\t " +
		"{{if $value.Aborted}}Aborted" +
		"{{else if $value.Archived}}Finished" +
		"{{else}}Running" +
		"{{end}}\t " +
		"{{FormatStart $value.StartedTime $value.FinishTime}}\t " +
		"{{FormatEnd $value.StartedTime $value.FinishTime}}\t " +
		"{{$value.Description}}\n"
	DSortListNoHdrTmpl = "{{ range $value := . }}" + dsortListBody + "{{end}}"
	DSortListTmpl      = dsortListHdr + DSortListNoHdrTmpl

	transformListHdr  = "ETL NAME\t XACTION\t OBJECTS\n"
	transformListBody = "{{$value.Name}}\t {{$value.XactID}}\t " +
		"{{if (eq $value.ObjCount 0) }}-{{else}}{{$value.ObjCount}}{{end}}\n"
	TransformListNoHdrTmpl = "{{ range $value := . }}" + transformListBody + "{{end}}"
	TransformListTmpl      = transformListHdr + TransformListNoHdrTmpl

	//
	// all other xactions
	//
	XactBucketTmpl      = xactBucketHdr + XactNoHdrBucketTmpl
	XactNoHdrBucketTmpl = "{{range $daemon := . }}" + xactBucketBodyAll + "{{end}}"

	xactBucketHdr     = "NODE\t ID\t KIND\t BUCKET\t OBJECTS\t BYTES\t START\t END\t STATE\n"
	xactBucketBodyAll = "{{range $key, $xctn := $daemon.XactSnaps}}" + xactBucketBodyOne + "{{end}}"
	xactBucketBodyOne = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{$xctn.Kind}}\t " +
		"{{FormatBckName $xctn.Bck}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +
		"{{FormatStart $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatEnd $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatXactState $xctn}}\n"

	// same as above except for: src-bck, dst-bck columns
	XactFromToTmpl      = xactFromToHdr + XactNoHdrFromToTmpl
	XactNoHdrFromToTmpl = "{{range $daemon := . }}" + xactFromToBodyAll + "{{end}}"

	xactFromToHdr     = "NODE\t ID\t KIND\t SRC BUCKET\t DST BUCKET\t OBJECTS\t BYTES\t START\t END\t STATE\n"
	xactFromToBodyAll = "{{range $key, $xctn := $daemon.XactSnaps}}" + xactFromToBodyOne + "{{end}}"
	xactFromToBodyOne = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{$xctn.Kind}}\t " +
		"{{FormatBckName $xctn.SrcBck}}\t " +
		"{{FormatBckName $xctn.DstBck}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +
		"{{FormatStart $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatEnd $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatXactState $xctn}}\n"

	// same as above for: no bucket column
	XactNoBucketTmpl      = xactNoBucketHdr + XactNoHdrNoBucketTmpl
	XactNoHdrNoBucketTmpl = "{{range $daemon := . }}" + xactNoBucketBodyAll + "{{end}}"

	xactNoBucketHdr     = "NODE\t ID\t KIND\t OBJECTS\t BYTES\t START\t END\t STATE\n"
	xactNoBucketBodyAll = "{{range $key, $xctn := $daemon.XactSnaps}}" + xactNoBucketBodyOne + "{{end}}"
	xactNoBucketBodyOne = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{$xctn.Kind}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +
		"{{FormatStart $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatEnd $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatXactState $xctn}}\n"

	XactECGetTmpl      = xactECGetStatsHdr + XactECGetNoHdrTmpl
	XactECGetNoHdrTmpl = "{{range $daemon := . }}" + xactECGetBody + "{{end}}"

	xactECGetStatsHdr  = "NODE\t ID\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t START\t END\t ABORTED\n"
	xactECGetBody      = "{{range $key, $xctn := $daemon.XactSnaps}}" + xactECGetStatsBody + "{{end}}"
	xactECGetStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{if $xctn.Bck.Name}}{{FormatBckName $xctn.Bck}}{{else}}-{{end}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +

		"{{ $ext := ExtECGetStats $xctn }}" +
		"{{if (eq $ext.ErrCount 0) }}-{{else}}{{$ext.ErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +

		"{{FormatStart $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatEnd $xctn.StartTime $xctn.EndTime}}\t " +
		"{{$xctn.AbortedX}}\n"

	XactECPutTmpl      = xactECPutStatsHdr + XactECPutNoHdrTmpl
	XactECPutNoHdrTmpl = "{{range $daemon := . }}" + xactECPutBody + "{{end}}"

	xactECPutStatsHdr  = "NODE\t ID\t BUCKET\t OBJECTS\t BYTES\t ERRORS\t QUEUE\t AVG TIME\t ENC TIME\t START\t END\t ABORTED\n"
	xactECPutBody      = "{{range $key, $xctn := $daemon.XactSnaps}}" + xactECPutStatsBody + "{{end}}"
	xactECPutStatsBody = "{{ $daemon.DaemonID }}\t " +
		"{{if $xctn.ID}}{{$xctn.ID}}{{else}}-{{end}}\t " +
		"{{if $xctn.Bck.Name}}{{FormatBckName $xctn.Bck}}{{else}}-{{end}}\t " +
		"{{if (eq $xctn.Stats.Objs 0) }}-{{else}}{{$xctn.Stats.Objs}}{{end}}\t " +
		"{{if (eq $xctn.Stats.Bytes 0) }}-{{else}}{{FormatBytesSig $xctn.Stats.Bytes 2}}{{end}}\t " +

		"{{ $ext := ExtECPutStats $xctn }}" +
		"{{if (eq $ext.EncodeErrCount 0) }}-{{else}}{{$ext.EncodeErrCount}}{{end}}\t " +
		"{{if (eq $ext.AvgQueueLen 0.0) }}-{{else}}{{ FormatFloat $ext.AvgQueueLen}}{{end}}\t " +
		"{{if (eq $ext.AvgObjTime 0) }}-{{else}}{{FormatMilli $ext.AvgObjTime}}{{end}}\t " +
		"{{if (eq $ext.AvgEncodeTime 0) }}-{{else}}{{FormatMilli $ext.AvgEncodeTime}}{{end}}\t " +

		"{{FormatStart $xctn.StartTime $xctn.EndTime}}\t " +
		"{{FormatEnd $xctn.StartTime $xctn.EndTime}}\t " +
		"{{$xctn.AbortedX}}\n"

	ListBucketsHdr  = "NAME\t PRESENT\t OBJECTS (cached, remote)\t TOTAL SIZE (apparent, objects)\t USAGE(%)\n"
	ListBucketsBody = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{FormatBool $v.Info.IsBckPresent}}\t " +
		"{{if (IsFalse $v.Info.IsBckPresent)}}-{{else}}{{$v.Info.ObjCount.Present}} {{$v.Info.ObjCount.Remote}}{{end}}\t " +
		"{{if (IsFalse $v.Info.IsBckPresent)}}-{{else}}{{FormatBytesUns $v.Info.TotalSize.OnDisk 2}} {{FormatBytesUns $v.Info.TotalSize.PresentObjs 2}}{{end}}\t " +
		"{{if (IsFalse $v.Info.IsBckPresent)}}-{{else}}{{$v.Info.UsedPct}}%{{end}}\n" +
		"{{end}}"
	ListBucketsTmpl = ListBucketsHdr + ListBucketsBody

	ListBucketsHdrNoSummary  = "NAME\t PRESENT\n"
	ListBucketsBodyNoSummary = "{{range $k, $v := . }}" +
		"{{FormatBckName $v.Bck}}\t {{FormatBool $v.Info.IsBckPresent}}\n" +
		"{{end}}"
	ListBucketsTmplNoSummary = ListBucketsHdrNoSummary + ListBucketsBodyNoSummary

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
	MultiPutTmpl = "Files to upload:\nEXTENSION\t COUNT\t SIZE\n" +
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

	// `search`
	SearchTmpl = "{{ JoinListNL . }}\n"

	// `show mountpath`
	MpathListTmpl = "{{range $p := . }}" +
		"{{ $p.DaemonID }}\n" +
		"{{if and (eq (len $p.Mpl.Available) 0) (eq (len $p.Mpl.Disabled) 0)}}" +
		"\tNo mountpaths\n" +
		"{{else}}" +
		"{{if ne (len $p.Mpl.Available) 0}}" +
		"\tUsed Capacity (all disks): avg {{$p.TargetCDF.PctAvg}}% max {{$p.TargetCDF.PctMax}}%\t " +
		"{{if (IsEqS $p.TargetCDF.CsErr \"\")}}{{else}}{{$p.TargetCDF.CsErr}}{{end}}\n" +
		"{{range $mp := $p.Mpl.Available }}" +
		"\t\t{{ $mp }} " +

		"{{range $k, $v := $p.TargetCDF.Mountpaths}}" +
		"{{if (IsEqS $k $mp)}}{{$v.FS}}{{end}}" +
		"{{end}}\n" +

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

type (
	// Used to return specific fields/objects for marshaling (MarshalIdent).
	forMarshaler interface {
		forMarshal() any
	}
	DiskStatsHelper struct {
		TargetID string
		DiskName string
		Stat     ios.DiskStats
	}
	SmapHelper struct {
		Smap         *cluster.Smap
		ExtendedURLs bool
	}
	StatsAndStatusHelper struct {
		Pmap StstMap `json:"pmap"`
		Tmap StstMap `json:"tmap"`
	}
	StatusHelper struct {
		Smap      *cluster.Smap        `json:"smap"`
		CluConfig *cmn.ClusterConfig   `json:"config"`
		Status    StatsAndStatusHelper `json:"status"`
	}
	ListBucketsHelper struct {
		Bck   cmn.Bck
		Props *cmn.BucketProps
		Info  *cmn.BsummResult
	}
)

var (
	// for extensions and override, see also:
	// - FuncMapUnits
	// - HelpTemplateFuncMap
	// - `altMap template.FuncMap` below
	funcMap = template.FuncMap{
		// formatting
		"FormatBytesSig":    func(size int64, digits int) string { return FmtSize(size, UnitsIEC, digits) },
		"FormatBytesUns":    func(size uint64, digits int) string { return FmtSize(int64(size), UnitsIEC, digits) },
		"FormatMAM":         func(u int64) string { return fmt.Sprintf("%-10s", FmtSize(u, UnitsIEC, 2)) },
		"FormatMilli":       func(dur cos.Duration) string { return fmtMilli(dur, UnitsIEC) },
		"FormatStart":       func(s, e time.Time) string { res, _ := FmtStartEnd(s, e); return res },
		"FormatEnd":         func(s, e time.Time) string { _, res := FmtStartEnd(s, e); return res },
		"FormatEC":          FmtEC,
		"FormatObjStatus":   fmtObjStatus,
		"FormatObjCustom":   fmtObjCustom,
		"FormatObjIsCached": fmtObjIsCached,
		"FormatDaemonID":    fmtDaemonID,
		"FormatSmapVersion": fmtSmapVer,
		"FormatFloat":       func(f float64) string { return fmt.Sprintf("%.2f", f) },
		"FormatBool":        FmtBool,
		"FormatBckName":     func(bck cmn.Bck) string { return bck.DisplayName() },
		"FormatACL":         fmtACL,
		"FormatNameArch":    fmtNameArch,
		"FormatXactState":   FmtXactStatus,
		//  misc. helpers
		"IsUnsetTime":   isUnsetTime,
		"IsEqS":         func(a, b string) bool { return a == b },
		"IsFalse":       func(v bool) bool { return !v },
		"JoinList":      fmtStringList,
		"JoinListNL":    func(lst []string) string { return fmtStringListGeneric(lst, "\n") },
		"ExtECGetStats": extECGetStats,
		"ExtECPutStats": extECPutStats,
		// StatsAndStatusHelper:
		// select specific field and make a slice, and then a string out of it
		"OnlineStatus": func(h StatsAndStatusHelper) string { return toString(h.onlineStatus()) },
		"Deployments":  func(h StatsAndStatusHelper) string { return toString(h.deployments()) },
		"Versions":     func(h StatsAndStatusHelper) string { return toString(h.versions()) },
		"BuildTimes":   func(h StatsAndStatusHelper) string { return toString(h.buildTimes()) },
		"Rebalance":    func(h StatsAndStatusHelper) string { return toString(h.rebalance()) },
	}

	AliasTemplate = "ALIAS\tCOMMAND\n{{range $alias := .}}" +
		"{{ $alias.Name }}\t{{ $alias.Value }}\n" +
		"{{end}}"

	HelpTemplateFuncMap = template.FuncMap{
		"FlagName": func(f cli.Flag) string { return strings.SplitN(f.GetName(), ",", 2)[0] },
		"Mod":      func(a, mod int) int { return a % mod },
	}
)

////////////////
// SmapHelper //
////////////////

var _ forMarshaler = SmapHelper{}

func (sth SmapHelper) forMarshal() any {
	return sth.Smap
}

//
// stats.NodeStatus
//

func calcCap(daemon *stats.NodeStatus) (total uint64) {
	for _, cdf := range daemon.TargetCDF.Mountpaths {
		total += cdf.Capacity.Avail
	}
	return total
}

////////////////////////
// StatsAndStatusHelper //
////////////////////////

// for all stats.NodeStatus structs: select specific field and append to the returned slice
// (using the corresponding jtags here for no particular reason)
func (h *StatsAndStatusHelper) onlineStatus() []string { return h.toSlice("status") }
func (h *StatsAndStatusHelper) deployments() []string  { return h.toSlice("deployment") }
func (h *StatsAndStatusHelper) versions() []string     { return h.toSlice("ais_version") }
func (h *StatsAndStatusHelper) buildTimes() []string   { return h.toSlice("build_time") }
func (h *StatsAndStatusHelper) rebalance() []string    { return h.toSlice("rebalance_snap") }
func (h *StatsAndStatusHelper) pods() []string         { return h.toSlice("k8s_pod_name") }

// internal helper for the methods above
func (h *StatsAndStatusHelper) toSlice(jtag string) []string {
	set := cos.NewStrSet()
	for _, m := range []StstMap{h.Pmap, h.Tmap} {
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
