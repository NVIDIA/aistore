// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"

	jsoniter "github.com/json-iterator/go"
)

const (
	devPrefixReg = "/dev/"
	devPrefixLVM = "/dev/mapper/"
)

const (
	sysBlockPath    = "/sys/class/block"
	sysDevBlockPath = "/sys/dev/block"
)

const initialNumDevs = 16

type (
	DiskInfo struct {
		Size  uint32
		Flags uint32
	}
	blockDev struct {
		name     string
		info     DiskInfo
		children BlockDevs
	}

	BlockDevs []*blockDev
)

var (
	errEmptyPhysBlock = errors.New("empty physical block size")
)

func (bd *blockDev) MarshalJSON() ([]byte, error) {
	l := len(bd.children)
	if l == 0 {
		if bd.info.Size == 512 {
			return cos.UnsafeB(bd.name), nil
		}
		s := fmt.Sprintf("%s(block=%d)", bd.name, bd.info.Size)
		return cos.UnsafeB(s), nil
	}

	a := make([]string, l)
	for i, c := range bd.children {
		if bd.info.Size == 512 {
			a[i] = c.name
		} else {
			a[i] = fmt.Sprintf("%s(block=%d)", c.name, c.info.Size)
		}
	}
	s := fmt.Sprintf("%s %v", bd.name, a)
	return cos.UnsafeB(s), nil
}

func _dump(blockDevs BlockDevs) {
	dump, _ := jsoniter.MarshalIndent(blockDevs, "", " ") // (custom MarshalJSON above)
	s := strings.Repeat("=", 32)
	nlog.Infoln("Begin dump block devices", s)
	nlog.Infoln(string(dump))
	nlog.Infoln("End dump block devices  ", s)
}

// fs2disks retrieves the underlying disk or disks; it may return multiple disks
// but only if the filesystem is RAID; it is called upon adding/enabling mountpath.
// NOTE: blockDevs here are not nil only at startup - see fs.New()
func fs2disks(mpath, fsname string, label cos.MountpathLabel, blockDevs BlockDevs, num int, testingEnv bool) (disks FsDisks, err error) {
	if blockDevs == nil {
		blockDevs, err = _lsblk("", nil /*parent*/)
		if err != nil && !testingEnv {
			cos.Errorln("_lsblk:", err)
		}
	}

	trimmedFS, ok := strings.CutPrefix(fsname, devPrefixLVM)
	if !ok {
		trimmedFS = strings.TrimPrefix(fsname, devPrefixReg)
	}

	disks = make(FsDisks, num)
	findDevs(blockDevs, trimmedFS, label, disks) // map trimmed(fs) <= disk(s)

	if !flag.Parsed() {
		return disks, nil // unit tests
	}

	if cmn.Rom.V(4, cos.ModIOS) {
		_dump(blockDevs)
	}

	switch {
	case len(disks) > 0:
		s := disks._str()
		nlog.Infoln("["+fsname+label.ToLog()+"]:", s)
	case testingEnv:
		// anything goes
	case label.IsNil():
		// empty label implies _resolvable_ underlying disk or disks
		e := errors.New("empty label implies _resolvable_ underlying disk (" + trimmedFS + ")")
		err = cmn.NewErrMpathNoDisks(mpath, fsname, e)
		nlog.Errorln(err)
		_dump(blockDevs)
	default:
		nlog.Infoln("No disks for", fsname, "[", trimmedFS, label, "]")
	}
	return disks, err
}

//
// private
//

// (see recursion below)
func _lsblk(parentDir string, parent *blockDev) (BlockDevs, error) {
	if parentDir == "" {
		debug.Assert(parent == nil)
		parentDir = sysBlockPath
	}
	dirents, err := os.ReadDir(parentDir)
	if err != nil {
		return nil, fmt.Errorf("_lsblk: failed to read-dir %q: %w", parentDir, err)
	}

	blockDevs := make(BlockDevs, 0, initialNumDevs)
	for _, dirent := range dirents {
		var (
			devDirPath  = filepath.Join(parentDir, dirent.Name())
			devFilePath = filepath.Join(devDirPath, "dev")
		)

		if _, err := os.Stat(devFilePath); err != nil {
			// Ignore everything without `dev` file.
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
				continue
			}
			return nil, fmt.Errorf("_lsblk: unexpected fstat %q: %w", devFilePath, err)
		}

		// We also have to check if this device is present in `/sys/dev/block/{dev}`.
		{
			dev, err := _readAny[string](devFilePath)
			if err != nil {
				return nil, err
			}
			devPath := filepath.Join(sysDevBlockPath, dev)
			if _, err := os.Stat(devPath); err != nil {
				// Ignore everything without `dev` file.
				if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
					continue
				}
				return nil, fmt.Errorf("_lsblk: failed to fstat %q: %w", devPath, err)
			}
		}

		{
			// We also have to check if indeed this is a root device by looking at `slaves` directory.
			slaves, err := os.ReadDir(filepath.Join(devDirPath, "slaves"))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
			if parent == nil && len(slaves) > 0 {
				continue
			}

			// And also check if this is a root device by looking at the `partition` file.
			_, err = os.Stat(filepath.Join(devDirPath, "partition"))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
			if parent == nil && err == nil {
				continue
			}
		}

		var (
			kname   = dirent.Name()
			name    = kname
			blksize uint32
		)
		if strings.HasPrefix(name, "dm-") {
			// If this is "device mapper" we have to read the name from `{dir}/dm/name` file.
			data, err := os.ReadFile(filepath.Join(devDirPath, "dm", "name"))
			if err != nil {
				return nil, err
			}
			name = strings.TrimSpace(string(data))
		}

		// 1. block size of this device/partition
		size, err := _readAny[int64](filepath.Join(devDirPath, "queue", "physical_block_size"))
		if err != nil {
			switch {
			case err == errEmptyPhysBlock:
				// Empty physical_block_size file -- likely not a block device, proceed to next entry
				continue
			case errors.Is(err, fs.ErrNotExist) && parent != nil:
				// Inherit value from parent if physical_block_size file does not exist
				blksize = parent.info.Size
			default:
				// Real error when reading
				return nil, err
			}
		} else {
			debug.Assert(size >= 0 && size < math.MaxUint32, size)
			blksize = uint32(size)
		}

		// 2. flags
		var flags uint32
		if parent != nil {
			flags = parent.info.Flags
		} else {
			rot, err := _readAny[int64](filepath.Join(devDirPath, "queue", "rotational"))
			if err == nil && rot != 0 {
				flags = FlagRotational
			}
			// TODO: prefix match won't work for device-mapped NVMe (non-rotational implies "ssd")
			if strings.HasPrefix(kname, "nvme") && (flags&FlagRotational) == 0 {
				flags |= FlagNVMe
			}
		}

		bd := &blockDev{
			name: name,
			info: DiskInfo{Size: blksize, Flags: flags},
		}

		{
			// Process all partitions.
			bd.children, err = _lsblk(devDirPath, bd)
			if err != nil {
				return nil, err
			}

			// Also process `holders` directory.
			holders, err := _lsblk(filepath.Join(devDirPath, "holders"), bd)
			if err != nil {
				return nil, err
			}
			bd.children = append(bd.children, holders...)
		}

		blockDevs = append(blockDevs, bd)
	}
	return blockDevs, nil
}

func _readAny[T any](path string) (value T, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return value, fmt.Errorf("_readAny: failed to read %q, err: %w", path, err)
	}
	switch any(value).(type) {
	case string:
		return any(strings.TrimSpace(string(data))).(T), nil
	case int64:
		if len(data) == 0 {
			return value, errEmptyPhysBlock
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return value, fmt.Errorf("_readAny: failed to parse %q, err: %w", path, err)
		}
		return any(v).(T), nil
	default:
		return value, fmt.Errorf("_readAny: unexpected type: %T (path %q)", value, path)
	}
}

func findDevs(blockDevs BlockDevs, trimmedFS string, label cos.MountpathLabel, disks FsDisks) {
	for _, bd := range blockDevs {
		// by dev name
		if bd.name == trimmedFS {
			disks[bd.name] = bd.info
			continue
		}
		// NOTE: by label
		if label != "" && strings.Contains(bd.name, string(label)) {
			disks[bd.name] = bd.info
			continue
		}
		if len(bd.children) > 0 && _match(bd.children, trimmedFS) {
			disks[bd.name] = bd.info
		}
	}
}

func _match(blockDevs BlockDevs, device string) bool {
	for _, dev := range blockDevs {
		if dev.name == device {
			return true
		}
		// recurs
		if len(dev.children) != 0 && _match(dev.children, device) {
			return true
		}
	}
	return false
}
