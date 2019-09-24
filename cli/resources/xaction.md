## Extended Actions (Xactions)

The CLI allows users to interact with AIStore [Xactions](../../docs/xaction.md).

### Start

`ais start xaction XACTION_NAME [BUCKET_NAME]`

Starts xaction(s). Some xactions require a bucket name to execute.
The second argument is used to determine the bucket name if it is required.

### Stop

`ais stop xaction XACTION_NAME|all [BUCKET_NAME]`

Stops xaction(s). If the first argument is `all`, all xactions are stopped.
The second argument is used to determine the bucket name if it is required.

### Show stats

`ais show xaction [XACTION_NAME] [BUCKET_NAME]`

Displays details about `XACTION_NAME` xaction. If no arguments are given, displays details about all xactions.
The second argument is used to determine the bucket name if it is required.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | `bool` | Output details in JSON format | `false` |
| `--all-items` | `bool` | If set, additionally displays old, finished xactions | `false` |
| `--active` | `bool` | If set, displays only running xactions | `false` |
