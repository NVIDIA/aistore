---
layout: post
title: SEARCH
permalink: /docs/cli/search
redirect_from:
 - /cli/search.md/
 - /docs/cli/search.md/
---

# CLI Command Search

AIS CLI is designed for easy use, without a need to spend time reading CLI docs - you should be able to start by running ais <TAB-TAB>, selecting one of the available (completion) options, and repeating until the command is ready to be entered.
On the other hand, the CLI supports an ever-growing set of commands. You may find yourself wondering whether the command you are looking sounds, for instance, as list, or ls, or maybe even dir?
If this is the case, the search command is exactly the tool that can help search through all supported commands by:
1. an exact match
2. regular expression, and
3. synonym

## Keyword Search

Return commands containing the search word or synonym.

```command
$ ais search object
ais object mv
ais object rm
ais show object

$ ais search create user
ais auth add user
```

## Regex pattern search

Search commands using `--regex` flag

```command
$ ais search --regex "mv|cp"
ais bucket cp
ais bucket mv
ais object mv
```
