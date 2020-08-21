---
layout: post
title: SEARCH
permalink: cmd/cli/resources/search
redirect_from:
 - cmd/cli/resources/search.md/
---

# Command Search

AIS CLI is designed for an immediate usage when there is no need to spend time reading CLI docs - you should be able to start by running ais <TAB-TAB>, selecting one of the available (completion) options, and repeating until the command is ready to be entered.
On the other hand, CLI supports an ever-growing set of commands. And sometimes you may find yourself wondering whether the command you are looking sounds, for instance, as list, or ls, or maybe even dir?
If this is the case, the search command is exactly the tool that can help search through all supported commands by:
1. an exact match
2. regular expression, and
3. synonym

### Keyword Search

Return commands containing the search word or synonym. 

```command
$ ais search object
ais rename object
ais rm object
ais show object

$ ais search create user
ais auth add user
```

### Regex pattern search

Search commands using `--regex` flag

```command 
$ ais search --regex "set\s"
ais set config
ais set props
ais set primary
```
