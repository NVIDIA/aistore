// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/dfcpub/fs"
)

// error types
type (
	ErrFqnMisplaced struct {
		errstr string
	}
)

func (e *ErrFqnMisplaced) Error() string { return e.errstr }

//
// resolve and validate fqn
//
func ResolveFQN(fqn string, bowner Bowner, islocal ...bool) (parsedFQN fs.FQNparsed, newfqn string, err error) {
	var errstr string
	parsedFQN, err = fs.Mountpaths.FQN2Info(fqn)
	if err != nil {
		return
	}
	newfqn, errstr = FQN(parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname, parsedFQN.IsLocal)
	if errstr != "" {
		err = errors.New(errstr)
		return
	}
	if newfqn != fqn {
		errstr = fmt.Sprintf("%s (%s/%s) appears to be locally misplaced: newfqn %s", fqn, parsedFQN.Bucket, parsedFQN.Objname, newfqn)
		err = &ErrFqnMisplaced{errstr}
		return
	}
	var bislocal bool
	if len(islocal) == 0 {
		bmd := bowner.Get()
		bislocal = bmd.IsLocal(parsedFQN.Bucket)
	} else {
		bislocal = islocal[0] // caller has already done the above
	}
	if bislocal != parsedFQN.IsLocal {
		err = fmt.Errorf("%s (%s/%s) - islocal mismatch(%t, %t)", fqn, parsedFQN.Bucket, parsedFQN.Objname, bislocal, parsedFQN.IsLocal)
	}
	return
}

func FQN(contentType, bucket, objname string, isLocal bool) (string, string) {
	mpath, errstr := hrwMpath(bucket, objname)
	if errstr != "" {
		return "", errstr
	}
	return fs.CSM.FQN(mpath, contentType, isLocal, bucket, objname)
}
