// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"testing"
)

func TestMatchRESTItemsSmoke(t *testing.T) {
	apiItems, err := MatchRESTItems("/some/path/to/url", 2, true, "some", "path")
	if err != nil {
		t.Fatal(err)
	}
	if len(apiItems) != 2 || apiItems[0] != "to" || apiItems[1] != "url" {
		t.Errorf("invalid API items: %v", apiItems)
	}
}

func TestMatchRESTItemsDontSplitAfter(t *testing.T) {
	apiItems, err := MatchRESTItems("/some/path/to/url", 1, false, "some", "path")
	if err != nil {
		t.Fatal(err)
	}
	if len(apiItems) != 1 || apiItems[0] != "to/url" {
		t.Errorf("invalid API items: %v", apiItems)
	}
}

func TestMatchRESTItemsMoreItemsAfter(t *testing.T) {
	apiItems, err := MatchRESTItems("/some/path/to/url/more", 2, true, "some", "path")
	if err != nil {
		t.Fatal(err)
	}
	if len(apiItems) != 3 || apiItems[0] != "to" || apiItems[1] != "url" || apiItems[2] != "more" {
		t.Errorf("invalid API items: %v", apiItems)
	}
}

func TestMatchRESTItemsMoreItemsAfterWithoutSplit(t *testing.T) {
	apiItems, err := MatchRESTItems("/some/path/to/url/more", 2, false, "some", "path")
	if err != nil {
		t.Fatal(err)
	}
	if len(apiItems) != 2 || apiItems[0] != "to" || apiItems[1] != "url/more" {
		t.Errorf("invalid API items: %v", apiItems)
	}
}

func TestMatchRESTItemsInvalidPath(t *testing.T) {
	apiItems, err := MatchRESTItems("/some/to/url/path", 2, true, "some", "path")
	if err == nil {
		t.Errorf("expected error, apiItems returned: %v", apiItems)
	}
}

func TestMatchRESTItemsTooShort(t *testing.T) {
	apiItems, err := MatchRESTItems("/some/path/to/url", 3, true, "some", "path")
	if err == nil {
		t.Errorf("expected error, apiItems returned: %v", apiItems)
	}
}
