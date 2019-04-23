## Extended Actions (Xaction)

The CLI allows users to interact with AIStore [Xactions](../../docs/xaction.md).

### start

`ais xaction start <value>`

Starts xaction(s). `<value>` is the name of the xaction to start.

> Some xactions require a bucket to execute.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | Name of the bucket to start the xaction | `""` |

### stats

`ais xaction stats <value>`

Returns the stats of `<value>` xaction. If the value is `empty`, return all xaction stats.


| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | bool | Output the stats in JSON format | `false` |

### stop

`ais xaction stop <value>`

Stops xaction(s). `<value>` is the xaction to start. If the value is `empty`, stop all xactions.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | Name of the bucket to stop the xaction | `""` |
