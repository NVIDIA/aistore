## AIS CLI

CLI provides easy-to-use intuitive interface to AIStore.

It is a separate package, module (as in: `go.mod`), and development.

For implementation, we have used [urfave/cli](https://github.com/urfave/cli/blob/main/docs/v1/getting-started.md) open-source.

To build CLI from source, run the following two steps:

```console
$ make cli			# 1. build CLI binary and install it into your `$GOPATH/bin` directory
$ make cli-autocompletions	# 2. install CLI autocompletions (Bash and/or Zsh)
```

Alternatively, install directly from GitHub:

* [Install CLI from release binaries](https://github.com/NVIDIA/aistore/blob/main/scripts/install_from_binaries.sh)

For example, the following command extracts CLI binary to the specified destination and, secondly, installs Bash autocompletions:

```console
$ ./scripts/install_from_binaries.sh --dstdir /tmp/www --completions
```

For more usage options, run: `./scripts/install_from_binaries.sh --help`

You can also install Bash and/or Zsh autocompletions separately at any (later) time:

* [Install CLI autocompletions](https://github.com/NVIDIA/aistore/blob/main/cmd/cli/install_autocompletions.sh)

Once installed, you should be able to start by running ais `<TAB-TAB>`, selecting one of the available (completion) options, and repeating until the command is ready to be entered.

**Please note**: we strongly recommend using CLI _with_ autocompletions enabled.

## Further references

* [Main CLI readme](/docs/cli.md)
* [CLI user documentation](/docs/cli)
