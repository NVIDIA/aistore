Main documentation can be found [here](/docs/cli.md).

## Info For Developers

AIS CLI utilizes [urfave/cli](https://github.com/urfave/cli/blob/master/docs/v1/manual.md) open-source framework.

### Adding New Commands

Currently, the CLI has the format of `ais <resource> <command>`.

To add a new command to an existing resource:

1. Create a subcommand entry for the command in the resource object
2. Create an entry in the command's flag map for the new command
3. Register flags in the subcommand object
4. Register the handler function (named `XXXHandler`) in the subcommand object

To add a new resource:

1. Create a new Go file (named `xxx_hdlr.go`) with the name of the new resource and follow the format of existing files
2. Once the new resource and its commands are implemented, make sure to register the new resource with the CLI (see `setupCommands()` in `app.go`)
