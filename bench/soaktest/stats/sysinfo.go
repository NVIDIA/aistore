package stats

import (
	"fmt"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	TypeProxy  = "proxy"
	TypeTarget = "target"
)

// Tracks System Info Stats
type SysInfoStat struct {
	cmn.SysInfo
	Type     string `json:"type"` //type (proxy|target)
	DaemonID string `json:"daemonid"`
}

func ParseClusterSysInfo(csi *cmn.ClusterSysInfo) []*SysInfoStat {
	result := make([]*SysInfoStat, 0)
	for k, v := range csi.Proxy {
		result = append(result, &SysInfoStat{SysInfo: *v, Type: TypeProxy, DaemonID: k})
	}
	for k, v := range csi.Target {
		result = append(result, &SysInfoStat{SysInfo: *v, Type: TypeTarget, DaemonID: k})
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
			},
			","))

	f.WriteString("\n")
}

func (sis SysInfoStat) writeStat(f *os.File) {
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
			},
			","))

	f.WriteString("\n")
}
