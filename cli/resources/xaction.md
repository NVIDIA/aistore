## Extended Actions (Xaction)

The CLI allows users to interact with AIStore [Xactions](../../docs/xaction.md).

### start

`ais xaction start XACTION_NAME`

Starts xaction(s).

> Some xactions require a bucket to execute.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | Name of the bucket to start the xaction | `""` |

### stop

`ais xaction stop [XACTION_NAME]`

Stops xaction(s). If `XACTION_NAME` is not provided, all xactions are stopped.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | Name of the bucket to stop the xaction | `""` |

### stats

`ais xaction stats [XACTION_NAME]`

Returns the stats of `XACTION_NAME` xaction. If `XACTION_NAME` is not provided, displays stats of all xactions.


| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | bool | Output the stats in JSON format | `false` |
| `--bucket` | string | Name of the bucket to start the xaction | `""` |
| `--all` | bool | If set, additionally displays old, finished xactions | `false` |
