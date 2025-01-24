package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
)

const (
	lenNodeID = 13
	lenUname  = 80
)

type (
	Scenario struct {
		Smap   meta.Smap         `json:"smap"`
		HrwMap map[string]string `json:"hrw_map"`
	}
	TestCase struct {
		numNodes int
		numObjs  int
	}
)

// generateScenario creates a scenario JSON file for the given test case
func (t *TestCase) generateScenario() (err error) {
	var outfile *os.File

	filename := fmt.Sprintf("./scenario-%d-nodes-%d-objs.json", t.numNodes, t.numObjs)
	outfile, err = os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}

	defer outfile.Close()

	// 1. Initialize targets in smap
	scenario := Scenario{
		Smap: meta.Smap{
			Tmap: make(meta.NodeMap, t.numNodes),
			Pmap: make(meta.NodeMap, 1),
		},
		HrwMap: make(map[string]string, t.numNodes),
	}
	for range t.numNodes {
		tname := cos.CryptoRandS(lenNodeID)
		target := &meta.Snode{}
		scenario.Smap.Tmap[tname] = target
		target.Init(tname, apc.Target)
	}

	// 2. Initialize proxy in smap
	pname := cos.CryptoRandS(lenNodeID)
	proxy := &meta.Snode{}
	scenario.Smap.Pmap[pname] = proxy
	proxy.Init(pname, apc.Proxy)

	scenario.Smap.Primary = proxy

	// 3. Initialize objects
	for range t.numObjs {
		uname := cos.CryptoRandS(lenUname)
		snode, err := scenario.Smap.HrwName2T(cos.UnsafeB(uname))
		if err != nil {
			return fmt.Errorf("failed to find target Snode %s: %w", uname, err)
		}
		scenario.HrwMap[uname] = snode.DaeID
	}

	jsonData, err := json.Marshal(scenario)
	if err != nil {
		return fmt.Errorf("failed to Marshal to JSON format: %w", err)
	}

	if _, err = outfile.Write(jsonData); err != nil {
		return fmt.Errorf("failed to write: %s: %w", filename, err)
	}

	return nil
}

func main() {
	tests := []TestCase{
		{5, 10},
		{2, 100},
		{3, 1000},
		{10, 10},
	}

	for _, test := range tests {
		if err := test.generateScenario(); err != nil {
			cos.AssertNoErr(err)
		}
	}
}
