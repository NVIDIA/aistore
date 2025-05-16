# CLI Command Search

AIS CLI is designed for ease of usage. In most cases, there must be no need to spend time reading CLI docs.

However, when starting to type a command, you may find yourself wondering whether the next keyword you are looking for is, for instance, `ais list`, or `ls`, or maybe `dir`?

In those cases, the `search` command would be exactly the tool to use.

You can search all supported commands via:
1. keyword or part of thereof,
2. regular expression, or
3. synonym

## Keyword Search

Return commands containing the search word or synonym.

```command
$ ais search object
ais object mv
ais object rm
ais show object

$ ais search mountpath
ais show storage mountpath
ais storage mountpath attach
ais storage mountpath detach
ais storage mountpath disable
ais storage mountpath enable
ais storage mountpath show
ais storage show mountpath

$ ais search create user
ais auth add user
```

As you can see in the case of `ais search mountpath`, the search tool will list all possible commands containing the `mountpath` keyword, and even the aliased versions of those commands. This is a great way to learn as you go and remind yourself of the commands at your disposal while developing.

## Regex pattern search

Search commands using `--regex` flag

```command
$ ais search --regex "mv|cp"
ais cp
ais bucket mv
ais object mv
```
