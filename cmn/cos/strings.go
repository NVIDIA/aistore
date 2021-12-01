// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "strings"

// based on wikipedia article: https://en.wikipedia.org/wiki/Damerau–Levenshtein_distance
func DamerauLevenstheinDistance(s, t string) int {
	distances := make([][]int, len(s)+1)
	for i := range distances {
		distances[i] = make([]int, len(t)+1)
	}

	// cost of dropping all characters is equal to a length
	for i := range distances {
		distances[i][0] = i
	}
	for j := range distances[0] {
		distances[0][j] = j
	}

	for j := 1; j <= len(t); j++ {
		for i := 1; i <= len(s); i++ {
			if s[i-1] == t[j-1] {
				distances[i][j] = distances[i-1][j-1]
				continue
			}

			// characters are different. Take character from s or from t or drop character at all
			min := Min(distances[i-1][j], distances[i][j-1], distances[i-1][j-1])

			// check if error might be swap of subsequent characters
			if i >= 2 && j >= 2 && s[i-2] == t[j-1] && s[i-1] == t[j-2] {
				min = Min(min, distances[i-2][j-2])
			}
			distances[i][j] = min + 1
		}
	}
	return distances[len(s)][len(t)]
}

func CapitalizeString(s string) string {
	if s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func NounEnding(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

// Either returns either lhs or rhs depending on which one is non-empty
func Either(lhs, rhs string) string {
	if lhs != "" {
		return lhs
	}
	return rhs
}
