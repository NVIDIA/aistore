// Package devtools provides common low-level utilities for AIStore development tools.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package devtools

import (
	"net"
	"strings"

	"github.com/NVIDIA/aistore/api"
)

func BaseAPIParams(ctx *Ctx, url string) api.BaseParams {
	return api.BaseParams{
		Client: ctx.Client,
		URL:    url,
	}
}

func PingURL(url string) (err error) {
	var (
		conn net.Conn
		addr = strings.TrimPrefix(url, "http://")
	)

	if addr == url {
		addr = strings.TrimPrefix(url, "https://")
	}
	conn, err = net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
	}
	return
}
