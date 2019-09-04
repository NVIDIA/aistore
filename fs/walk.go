package fs

import (
	"os"

	"github.com/karrick/godirwalk"
)

type WalkFunc func(fqn string, de DirEntry) error

type (
	DirEntry interface {
		IsDir() bool
	}

	Options struct {
		Callback WalkFunc
		Sorted   bool
	}
)

// PathErrToAction is a default error callback for fast godirwalk.Walk.
// It silently skips deleted files and directories.
func PathErrToAction(_ string, err error) godirwalk.ErrorAction {
	if os.IsNotExist(err) {
		return godirwalk.SkipNode
	}
	return godirwalk.Halt
}

// godirwalk is used by default. If you want to switch to standard filepath.Walk do:
// 1. Rewrite `callback` to:
//   func (opts *Options) callback(fqn string, de os.FileInfo, err error) error {
//     if err != nil {
//        if err := cmn.PathWalkErr(err); err != nil {
//          return err
//        }
//        return nil
//     }
//     return opts.callback(fqn, de)
//   }
// 2. Replace `Walk` body with one-liner:
//   return filepath.Walk(fqn, opts.callback)
// No more changes required.
// NOTE: for standard filepath.Walk option 'Sorted' is ignored

var _ DirEntry = &godirwalk.Dirent{}

func (opts *Options) callback(fqn string, de *godirwalk.Dirent) error {
	return opts.Callback(fqn, de)
}

func Walk(fqn string, opts *Options) error {
	gOpts := &godirwalk.Options{
		ErrorCallback: PathErrToAction,
		Callback:      opts.callback,
		Unsorted:      !opts.Sorted,
	}
	return godirwalk.Walk(fqn, gOpts)
}
