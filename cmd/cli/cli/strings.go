// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// based on https://en.wikipedia.org/wiki/Damerau–Levenshtein_distance
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
			distance := min(distances[i-1][j], distances[i][j-1], distances[i-1][j-1])

			// check if error might be swap of subsequent characters
			if i >= 2 && j >= 2 && s[i-2] == t[j-1] && s[i-1] == t[j-2] {
				distance = min(distance, distances[i-2][j-2])
			}
			distances[i][j] = distance + 1
		}
	}
	return distances[len(s)][len(t)]
}

func strToSentence(str string) string {
	if str == "" {
		return ""
	}
	capitalized := capitalizeFirst(str)
	if !cos.IsLastB(capitalized, '.') {
		capitalized += "."
	}
	return capitalized
}

func capitalizeFirst(s string) string {
	return strings.ToUpper(s[:1]) + s[1:]
}
