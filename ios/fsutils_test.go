/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import "testing"

func TestGetFSUsedPercentage(t *testing.T) {
	percentage, ok := GetFSUsedPercentage("/")
	if !ok {
		t.Error("Unable to retrieve FS used percentage!")
	}
	if percentage > 100 {
		t.Errorf("Invalid FS used percentage [%d].", percentage)
	}
}
