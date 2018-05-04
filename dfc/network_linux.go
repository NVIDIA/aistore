// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

/*
#include <stdio.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <linux/sockios.h>
#include <linux/if.h>
#include <linux/ethtool.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

int get_interface_speed(char *ifname){
    int sock;
    struct ifreq ifr;
    struct ethtool_cmd edata;
    int rc;
    sock = socket(AF_INET, SOCK_STREAM, 0);
    // string copy first argument into struct
    strncpy(ifr.ifr_name, ifname, sizeof(ifr.ifr_name));
    ifr.ifr_data = &edata;
    // set some global options from ethtool API
    edata.cmd = ETHTOOL_GSET;
    // issue ioctl
    rc = ioctl(sock, SIOCETHTOOL, &ifr);

    close(sock);

    if (rc < 0) {
        // perror("ioctl");        // lets not error out here
        // make sure to zero out speed
        return -1;
    }

    return edata.speed + edata.speed_hi * 32;
}
*/

/*
// FIXME TODO: not every docker-building host may have GCC installed
//   so deploy_docker.sh fails to run proxy and target images if cgo is used
//   Comment out all the code until we enable GCC in the image or
//   we really need to know a network interface speed
import "C"

import (
 	"unsafe"
)

// netifSpeed takes interface name and returns the interface's bandwidth (Mbps)
func netifSpeed(netifName string) int {
	ifnm := []byte(netifName)
	return int(C.get_interface_speed((*C.char)(unsafe.Pointer(&ifnm[0]))))
}
*/

func netifSpeed(netifName string) int {
	return 0
}
