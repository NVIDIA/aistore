/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/dfcpub/fs"
)

// fqn => (contentType, bucket, objname, err)
func ResolveFQN(fqn string, bowner Bowner) (contentType, bucket, objname string, err error) {
	var (
		islocal   bool
		parsedFQN fs.FQNparsed
	)
	parsedFQN, err = fs.Mountpaths.FQN2Info(fqn)
	if err != nil {
		return
	}
	contentType, bucket, objname, islocal = parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname, parsedFQN.IsLocal
	resfqn, errstr := FQN(contentType, bucket, objname, islocal)
	if errstr != "" {
		err = errors.New(errstr)
		return
	}
	errstr = fmt.Sprintf("Cannot convert %s => %s/%s", fqn, bucket, objname)
	if resfqn != fqn {
		err = fmt.Errorf("%s - %q misplaced", errstr, resfqn)
		return
	}
	bmd := bowner.Get()
	if bmd.IsLocal(bucket) != islocal {
		err = fmt.Errorf("%s - islocal mismatch(%t, %t)", errstr, bmd.IsLocal(bucket), islocal)
	}
	return
}

func FQN(contentType, bucket, objname string, isLocal bool) (string, string) {
	mpath, errstr := hrwMpath(bucket, objname)
	if errstr != "" {
		return "", errstr
	}
	return fs.FQN(mpath, contentType, isLocal, bucket, objname)
}
