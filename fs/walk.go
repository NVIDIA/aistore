package fs

import (
	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/karrick/godirwalk"
)

const (
	// Determines the threshold of error count which will result in halting
	// the walking operation.
	errThreshold = 1000
)

type (
	errFunc  func(string, error) godirwalk.ErrorAction
	WalkFunc func(fqn string, de DirEntry) error
)

type (
	DirEntry interface {
		IsDir() bool
	}

	Options struct {
		ErrCallback errFunc
		Callback    WalkFunc
		Sorted      bool
	}

	errCallbackWrapper struct {
		counter atomic.Int64
	}
)

// PathErrToAction is a default error callback for fast godirwalk.Walk.
// The idea is that on any error that was produced during the walk we dispatch
// this handler and act upon the error.
//
// By default it halts on bucket level errors because there is no option to
// continue walking if there is a problem with a bucket. Also we count "soft"
// errors and abort if we reach certain amount of them.
func (ew *errCallbackWrapper) PathErrToAction(_ string, err error) godirwalk.ErrorAction {
	if cmn.IsErrBucketLevel(err) {
		return godirwalk.Halt
	}
	if ew.counter.Load() > errThreshold {
		return godirwalk.Halt
	}
	if cmn.IsErrObjLevel(err) {
		ew.counter.Add(1)
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
	// For now `ErrCallback` is not used. Remove if something changes and ensure
	// that we have
	cmn.Assert(opts.ErrCallback == nil)

	ew := &errCallbackWrapper{}
	// Using default error callback which halts on bucket errors and halts
	// on `errThreshold` lom errors.
	opts.ErrCallback = ew.PathErrToAction

	gOpts := &godirwalk.Options{
		ErrorCallback: opts.ErrCallback,
		Callback:      opts.callback,
		Unsorted:      !opts.Sorted,
	}
	return godirwalk.Walk(fqn, gOpts)
}
