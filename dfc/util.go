/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"net"

	"github.com/golang/glog"
)

func assert(cond bool, args ...interface{}) {
	if cond {
		return
	}
	var message = "assertion failed"
	if len(args) > 0 {
		message += ": "
		for i := 0; i < len(args); i++ {
			message += fmt.Sprintf("%#v ", args[i])
		}
	}
	glog.Fatalln(message)
}

// Returns first IP address of host.
func getipaddr() (string, error) {
	var ipaddr string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		glog.Errorf("Failed to read Net interface %v \n", err)
		return ipaddr, err
	}
	// Returns first IP address
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				//os.Stdout.WriteString(ipnet.IP.String() + "\n")
				ipaddr = ipnet.IP.String()
				break
			}
		}
	}
	return ipaddr, err

}

// Check and Set MountPath error count and status.
func checksetmounterror(path string) {
	if getMountPathErrorCount(path) > ctx.config.Cache.ErrorThreshold {
		setMountPathStatus(path, false)
	} else {
		incrMountPathErrorCount(path)
	}

}
