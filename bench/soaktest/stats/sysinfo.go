package stats

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	TypeProxy  = "proxy"
	TypeTarget = "target"
)

// Tracks System Info Stats
type SysInfoStat struct {
	cmn.SysInfo
	cmn.FSInfo

	Type      string    `json:"type"` //type (proxy|target)
	DaemonID  string    `json:"daemonid"`
	Timestamp time.Time `json:"timestamp"`
}

func ParseClusterSysInfo(csi *cmn.ClusterSysInfo, timestamp time.Time) []*SysInfoStat {
	result := make([]*SysInfoStat, 0)
	for k, v := range csi.Proxy {
		result = append(result, &SysInfoStat{SysInfo: *v, Type: TypeProxy, DaemonID: k, Timestamp: timestamp})
	}
	for k, v := range csi.Target {
		result = append(result, &SysInfoStat{SysInfo: v.SysInfo, FSInfo: v.FSInfo, Type: TypeTarget, DaemonID: k, Timestamp: timestamp})
	}

	return result
}

func (sis SysInfoStat) writeHeadings(f *os.File) {
	f.WriteString(
		strings.Join(
			[]string{
				"Timestamp",
				"DaemonID",
				"Role",
				"Memory Used",
				"Memory Available",
				"% Memory Used",
				"% CPU Used",
				"Capacity Used (B)",
				"Total Capacity (B)",
				"% Capacity Used",
			},
			","))

	f.WriteString("\n")
}

func (sis SysInfoStat) writeStat(f *os.File) {
	var safeFSUsed, safeFSCapacity, safePctFSUsed string

	if sis.FSCapacity > 0 {
		safeFSUsed = fmt.Sprintf("%d", (sis.FSUsed))
		safeFSCapacity = fmt.Sprintf("%d", (sis.FSCapacity))
		safePctFSUsed = fmt.Sprintf("%f", (sis.PctFSUsed))
	}

	f.WriteString(
		strings.Join(
			[]string{
				sis.Timestamp.Format(csvTimeFormat),
				sis.DaemonID,
				sis.Type,
				fmt.Sprintf("%d", (sis.MemUsed)),
				fmt.Sprintf("%d", (sis.MemAvail)),
				fmt.Sprintf("%f", sis.PctMemUsed),
				fmt.Sprintf("%f", sis.PctCPUUsed),
				safeFSUsed,
				safeFSCapacity,
				safePctFSUsed,
			},
			","))

	f.WriteString("\n")
}
