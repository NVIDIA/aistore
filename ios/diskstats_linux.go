package ios

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	largeNumDisks = 3
)

type DiskStat struct {
	// based on https://www.kernel.org/doc/Documentation/iostats.txt
	ReadComplete  int64 // 1 - # of reads completed
	ReadMerged    int64 // 2 - # of reads merged
	ReadSectors   int64 // 3 - # of sectors read
	ReadMs        int64 // 4 - # ms spent reading
	WriteComplete int64 // 5 - # writes completed
	WriteMerged   int64 // 6 - # writes merged
	WriteSectors  int64 // 7 - # of sectors written
	WriteMs       int64 // 8 - # of milliseconds spent writing
	IOPending     int64 // 9 - # of I/Os currently in progress
	IOMs          int64 // 10 - # of milliseconds spent doing I/Os
	IOMsWeighted  int64 // 11 - weighted # of milliseconds spent doing I/Os
}

type DiskStats map[string]DiskStat

func (ds *DiskStat) ToString() string {
	return strings.Join([]string{
		spI64("ReadComplete", ds.ReadComplete),
		spI64("ReadMerged", ds.ReadMerged),
		spI64("ReadSectors", ds.ReadSectors),
		spI64("ReadMs", ds.ReadMs),
		spI64("WriteComplete", ds.WriteComplete),
		spI64("WriteMerged", ds.WriteMerged),
		spI64("WriteSectors", ds.WriteSectors),
		spI64("WriteMs", ds.WriteMs),
		spI64("IOPending", ds.IOPending),
		spI64("IOMs", ds.IOMs),
		spI64("IOMsWeighted", ds.IOMsWeighted),
	}, " ")
}

func GetDiskStats(disks cmn.StringSet) DiskStats {
	if len(disks) < largeNumDisks {
		output := make(DiskStats, len(disks))

		for disk := range disks {
			stat, ok := readSingleDiskStat(disk)
			if !ok {
				continue
			}
			output[disk] = stat
		}
		return output
	}

	return readMultipleDiskStats(disks)
}

func readSingleDiskStat(disk string) (DiskStat, bool) {
	file, err := os.Open(fmt.Sprintf("/sys/class/block/%v/stat", disk))
	if err != nil {
		glog.Error(err)
		return DiskStat{}, false
	}

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	fields := strings.Fields(scanner.Text())

	if len(fields) < 11 {
		return DiskStat{}, false
	}

	return extractDiskStat(fields, 0), true
}

func readMultipleDiskStats(disks cmn.StringSet) DiskStats {
	output := make(DiskStats, len(disks))

	file, err := os.Open("/proc/diskstats")
	if err != nil {
		glog.Error(err)
		return output
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}
		deviceName := fields[2]
		if _, ok := disks[deviceName]; !ok {
			continue
		}

		output[deviceName] = extractDiskStat(fields, 3)
	}

	return output
}

func extractDiskStat(fields []string, offset int) DiskStat {
	return DiskStat{
		extractI64(fields[offset]),
		extractI64(fields[offset+1]),
		extractI64(fields[offset+2]),
		extractI64(fields[offset+3]),
		extractI64(fields[offset+4]),
		extractI64(fields[offset+5]),
		extractI64(fields[offset+6]),
		extractI64(fields[offset+7]),
		extractI64(fields[offset+8]),
		extractI64(fields[offset+9]),
		extractI64(fields[offset+10]),
	}
}

func extractI64(field string) int64 {
	val, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		glog.Fatalf("Failed to convert field value '%s' to int: %v \n",
			field, err)
	}
	return val
}

func spI64(name string, field int64) string {
	return fmt.Sprintf("%s=%v", name, field)
}
