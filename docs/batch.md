---
layout: post
title: BATCH
permalink: docs/batch
redirect_from:
- docs/batch.md/
---

## Table of Contents
- [List/Range Operations](#listrange-operations)
        - [List](#list)
        - [Range](#range)
        - [Examples](#examples)

## List/Range Operations

AIStore provides two APIs to operate on groups of objects: List, and Range. Both of these share two optional parameters:

| Parameter | Description | Default |
|--- | --- | --- |
| deadline | The amount of time before the request expires formatted as a [golang duration string](https://golang.org/pkg/time/#ParseDuration). A timeout of 0 means no timeout.| 0 |
| wait | If true, a response will be sent only when the operation completes or the deadline passes. When false, a response will be sent once the operation is initiated. When setting wait=true, ensure your request has a timeout at least as long as the deadline. | false |

#### List

List APIs take a JSON array of object names, and initiate the operation on those objects.

| Parameter | Description |
| --- | --- |
| objnames | JSON array of object names |

#### Range

Range APIs take an optional prefix, a regular expression, and a numeric range. A matching object name will begin with the prefix and contain a number that satisfies both the regex and the range as illustrated below.


| Parameter | Description |
| --- | --- |
| prefix | The prefix that all matching object names will begin with. Empty prefix ("") will match all names. |
| regex | The regular expression, represented as an escaped string, to match the number embedded in the object name. Note that the regular expression applies to the entire name - the prefix (if provided) is not excluded. |
| range | Represented as "min:max", corresponding to the inclusive range from min to max. Either or both of min and max may be empty strings (""), in which case they will be ignored. If regex is an empty string, range will be ignored. |

#### Examples

| Prefix | Regex |  Escaped Regex | Range | Matches<br>(the match is highlighted) | Doesn't Match |
| --- | --- | --- | --- | --- | --- |
| "__tst/test-" | `"\d22\d"` | `"\\d22\\d"` | "1000:2000" | "__tst/test-`1223`"<br>"__tst/test-`1229`-4000.dat"<br>"__tst/test-1111-`1229`.dat"<br>"__tst/test-`1222`2-40000.dat" | "__prod/test-1223"<br>"__tst/test-1333"<br>"__tst/test-2222-4000.dat" |
| "a/b/c" | `"^\d+1\d"` | `"^\\d+1\\d"` | ":100000" | "a/b/c/`110`"<br>"a/b/c/`99919`-200000.dat"<br>"a/b/c/`2314`video-big" | "a/b/110"<br>"a/b/c/d/110"<br>"a/b/c/video-99919-20000.dat"<br>"a/b/c/100012"<br>"a/b/c/30331" |

