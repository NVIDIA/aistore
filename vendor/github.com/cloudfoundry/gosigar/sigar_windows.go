package sigar

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"github.com/cloudfoundry/gosigar/sys/windows"
	"github.com/pkg/errors"
)

var (
	kernel32DLL = syscall.MustLoadDLL("kernel32")

	procGetDiskFreeSpace     = kernel32DLL.MustFindProc("GetDiskFreeSpaceW")
	procGetSystemTimes       = kernel32DLL.MustFindProc("GetSystemTimes")
	procGetTickCount64       = kernel32DLL.MustFindProc("GetTickCount64")
	procGlobalMemoryStatusEx = kernel32DLL.MustFindProc("GlobalMemoryStatusEx")

	// processQueryLimitedInfoAccess is set to PROCESS_QUERY_INFORMATION for Windows
	// 2003 and XP where PROCESS_QUERY_LIMITED_INFORMATION is unknown. For all newer
	// OS versions it is set to PROCESS_QUERY_LIMITED_INFORMATION.
	processQueryLimitedInfoAccess = windows.PROCESS_QUERY_LIMITED_INFORMATION
)

func (self *LoadAverage) Get() error {
	return ErrNotImplemented
}

func (u *Uptime) Get() error {
	r1, _, e1 := syscall.Syscall(procGetTickCount64.Addr(), 0, 0, 0, 0)
	if e1 != 0 {
		return error(e1)
	}
	u.Length = (time.Duration(r1) * time.Millisecond).Seconds()
	return nil
}

type memorystatusex struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

func (m *Mem) Get() error {
	var x memorystatusex
	x.Length = uint32(unsafe.Sizeof(x))
	r1, _, e1 := syscall.Syscall(procGlobalMemoryStatusEx.Addr(), 1,
		uintptr(unsafe.Pointer(&x)),
		0,
		0,
	)
	if err := checkErrno(r1, e1); err != nil {
		return fmt.Errorf("GlobalMemoryStatusEx: %s", err)
	}
	m.Total = x.TotalPhys
	m.Free = x.AvailPhys
	m.ActualFree = m.Free
	m.Used = m.Total - m.Free
	m.ActualUsed = m.Used
	return nil
}

func (s *Swap) Get() error {
	const MB = 1024 * 1024
	out, err := exec.Command("wmic", "pagefile", "list", "full").Output()
	if err != nil {
		return err
	}
	total, err := parseWmicOutput(out, []byte("AllocatedBaseSize"))
	if err != nil {
		return err
	}
	used, err := parseWmicOutput(out, []byte("CurrentUsage"))
	if err != nil {
		return err
	}
	s.Total = total * MB
	s.Used = used * MB
	s.Free = s.Total - s.Used
	return nil
}

func parseWmicOutput(s, sep []byte) (uint64, error) {
	bb := bytes.Split(s, []byte("\n"))
	for i := 0; i < len(bb); i++ {
		b := bytes.TrimSpace(bb[i])
		n := bytes.IndexByte(b, '=')
		if n > 0 && bytes.Equal(sep, b[:n]) {
			return strconv.ParseUint(string(b[n+1:]), 10, 64)
		}
	}
	return 0, errors.New("parseWmicOutput: missing field: " + string(sep))
}

func (c *Cpu) Get() error {
	var (
		idleTime   syscall.Filetime
		kernelTime syscall.Filetime // Includes kernel and idle time.
		userTime   syscall.Filetime
	)
	r1, _, e1 := syscall.Syscall(procGetSystemTimes.Addr(), 3,
		uintptr(unsafe.Pointer(&idleTime)),
		uintptr(unsafe.Pointer(&kernelTime)),
		uintptr(unsafe.Pointer(&userTime)),
	)
	if err := checkErrno(r1, e1); err != nil {
		return fmt.Errorf("GetSystemTimes: %s", err)
	}

	c.Idle = uint64(idleTime.Nanoseconds())
	c.Sys = uint64(kernelTime.Nanoseconds()) - c.Idle
	c.User = uint64(userTime.Nanoseconds())
	return nil
}

func (self *CpuList) Get() error {
	return ErrNotImplemented
}

func (self *FileSystemList) Get() error {
	return ErrNotImplemented
}

func (self *ProcList) Get() error {
	return ErrNotImplemented
}

func (self *ProcState) Get(pid int) error {
	return ErrNotImplemented
}

func (self *ProcMem) Get(pid int) error {
	handle, err := syscall.OpenProcess(processQueryLimitedInfoAccess|windows.PROCESS_VM_READ, false, uint32(pid))
	if err != nil {
		return errors.Wrapf(err, "OpenProcess failed for pid=%v", pid)
	}
	defer syscall.CloseHandle(handle)

	counters, err := windows.GetProcessMemoryInfo(handle)
	if err != nil {
		return errors.Wrapf(err, "GetProcessMemoryInfo failed for pid=%v", pid)
	}

	self.Resident = uint64(counters.WorkingSetSize)
	self.Size = uint64(counters.PrivateUsage)
	return nil
}

func (self *ProcTime) Get(pid int) error {
	handle, err := syscall.OpenProcess(processQueryLimitedInfoAccess, false, uint32(pid))
	if err != nil {
		return errors.Wrapf(err, "OpenProcess failed for pid=%v", pid)
	}
	defer syscall.CloseHandle(handle)

	var CPU syscall.Rusage
	if err := syscall.GetProcessTimes(handle, &CPU.CreationTime, &CPU.ExitTime, &CPU.KernelTime, &CPU.UserTime); err != nil {
		return errors.Wrapf(err, "GetProcessTimes failed for pid=%v", pid)
	}

	// Windows epoch times are expressed as time elapsed since midnight on
	// January 1, 1601 at Greenwich, England. This converts the Filetime to
	// unix epoch in milliseconds.
	self.StartTime = uint64(CPU.CreationTime.Nanoseconds() / 1e6)

	// Convert to millis.
	self.User = uint64(windows.FiletimeToDuration(&CPU.UserTime).Nanoseconds() / 1e6)
	self.Sys = uint64(windows.FiletimeToDuration(&CPU.KernelTime).Nanoseconds() / 1e6)
	self.Total = self.User + self.Sys

	return nil
}

func (self *ProcArgs) Get(pid int) error {
	return ErrNotImplemented
}

func (self *ProcExe) Get(pid int) error {
	return ErrNotImplemented
}

func (fs *FileSystemUsage) Get(path string) error {
	root, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return fmt.Errorf("FileSystemUsage (%s): %s", path, err)
	}

	var (
		SectorsPerCluster uint32
		BytesPerSector    uint32

		// Free clusters available to the user
		// associated with the calling thread.
		NumberOfFreeClusters uint32

		// Total clusters available to the user
		// associated with the calling thread.
		TotalNumberOfClusters uint32
	)
	r1, _, e1 := syscall.Syscall6(procGetDiskFreeSpace.Addr(), 5,
		uintptr(unsafe.Pointer(root)),
		uintptr(unsafe.Pointer(&SectorsPerCluster)),
		uintptr(unsafe.Pointer(&BytesPerSector)),
		uintptr(unsafe.Pointer(&NumberOfFreeClusters)),
		uintptr(unsafe.Pointer(&TotalNumberOfClusters)),
		0,
	)
	if err := checkErrno(r1, e1); err != nil {
		return fmt.Errorf("FileSystemUsage (%s): %s", path, err)
	}

	m := uint64(SectorsPerCluster * BytesPerSector / 1024)
	fs.Total = uint64(TotalNumberOfClusters) * m
	fs.Free = uint64(NumberOfFreeClusters) * m
	fs.Avail = fs.Free
	fs.Used = fs.Total - fs.Free

	return nil
}

func checkErrno(r1 uintptr, e1 error) error {
	if r1 == 0 {
		if e, ok := e1.(syscall.Errno); ok && e != 0 {
			return e1
		}
		return syscall.EINVAL
	}
	return nil
}
