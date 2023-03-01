// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles download jobs in the cluster.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

const barWidth = 64

type (
	barArgs struct {
		barType string
		barText string
		total   int64
		options []mpb.BarOption
	}

	// TODO: is obsolete (reimpl. via simpleBar)
	progIndicator struct {
		objName         string
		sizeTransferred *atomic.Int64
	}
)

func simpleBar(args ...barArgs) (progress *mpb.Progress, bars []*mpb.Bar) {
	progress = mpb.New(mpb.WithWidth(barWidth))
	bars = make([]*mpb.Bar, 0, len(args))

	for _, a := range args {
		var argDecorators []decor.Decorator
		switch a.barType {
		case unitsArg:
			argDecorators = []decor.Decorator{
				decor.Name(a.barText, decor.WC{W: len(a.barText) + 1, C: decor.DidentRight}),
				decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
			}
		case sizeArg:
			argDecorators = []decor.Decorator{
				decor.Name(a.barText, decor.WC{W: len(a.barText) + 1, C: decor.DidentRight}),
				decor.CountersKibiByte("% .2f / % .2f", decor.WCSyncWidth),
			}
		default:
			debug.Assertf(false, "invalid argument: %s", a.barType)
		}
		options := make([]mpb.BarOption, 0, len(a.options)+2)
		options = append(options, a.options...)
		options = append(
			options,
			mpb.PrependDecorators(argDecorators...),
			mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
		)
		bars = append(bars, progress.AddBar(a.total, options...))
	}
	return
}

///////////////////
// progIndicator  -- TODO: reimplement via simpleBar()
///////////////////

func (*progIndicator) start() { fmt.Print("\033[s") }
func (*progIndicator) stop()  { fmt.Println("") }

func (pi *progIndicator) printProgress(incr int64) {
	fmt.Print("\033[u\033[K")
	fmt.Printf("Uploaded %s: %s", pi.objName, cos.ToSizeIEC(pi.sizeTransferred.Add(incr), 2))
}

func newProgIndicator(objName string) *progIndicator {
	return &progIndicator{objName, atomic.NewInt64(0)}
}
