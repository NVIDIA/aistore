// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"context"
)

var contextVlabs = &struct{}{}

func NewCtxVlabs(vlabs map[string]string) context.Context {
	return context.WithValue(context.Background(), contextVlabs, vlabs)
}

func GetCtxVlabs(ctx context.Context) map[string]string {
	v := ctx.Value(contextVlabs)
	vlabs, ok := v.(map[string]string)
	if !ok {
		return nil
	}
	return vlabs
}
