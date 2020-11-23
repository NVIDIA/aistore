cat > $AIS_CONF_FILE <<EOL
{
	"confdir": "${AIS_CONF_DIR}",
  "cloud": {
    $(for i in ${AIS_CLD_PROVIDERS};do echo -n "\"${i}\":{}", ;done | sed 's/,$//')
  },
	"mirror": {
		"copies":       2,
		"burst_buffer": 512,
		"util_thresh":  ${MIRROR_UTIL_THRESH:-20},
		"optimize_put": false,
		"enabled":      ${MIRROR_ENABLED:-false}
	},
	"ec": {
		"objsize_limit": ${OBJ_SIZE_LIMIT:-262144},
		"data_slices":   ${DATA_SLICES:-1},
		"parity_slices": ${PARITY_SLICES:-1},
		"compression":   "${COMPRESSION:-never}",
		"enabled":       ${EC_ENABLED:-false},
		"batch_size":    ${EC_BATCH_SIZE:-64}
	},
	"log": {
		"dir":       "${AIS_LOG_DIR:-/tmp/ais$NEXT_TIER/log}",
		"level":     "${AIS_LOG_LEVEL:-3}",
		"max_size":  4194304,
		"max_total": 67108864
	},
	"periodic": {
		"stats_time":        "10s",
		"notif_time":        "30s",
		"retry_sync_time":   "2s"
	},
	"timeout": {
		"cplane_operation":     "2s",
		"max_keepalive":        "4s",
		"max_host_busy":        "20s",
		"startup_time":         "1m",
		"send_file_time":       "5m"
	},
	"client": {
		"client_timeout":      "10s",
		"client_long_timeout": "30m",
		"list_timeout":        "3m"
	},
	"proxy": {
		"primary_url":   "${AIS_PRIMARY_URL}",
		"original_url":  "${AIS_PRIMARY_URL}",
		"discovery_url": "${AIS_DISCOVERY_URL}",
		"non_electable": ${NON_ELECTABLE:-false}
	},
	"lru": {
		"lowwm":             75,
		"highwm":            90,
		"out_of_space":      95,
		"dont_evict_time":   "120m",
		"capacity_upd_time": "10m",
		"enabled":           true
	},
	"disk":{
	    "iostat_time_long":  "${IOSTAT_TIME_LONG:-2s}",
	    "iostat_time_short": "${IOSTAT_TIME_SHORT:-100ms}",
	    "disk_util_low_wm":  20,
	    "disk_util_high_wm": 80,
	    "disk_util_max_wm":  95
	},
	"rebalance": {
		"enabled":         true,
		"dont_run_time":   "0m",
		"dest_retry_time": "2m",
		"quiescent":       "10s",
		"compression":     "${COMPRESSION:-never}",
		"multiplier":      ${REBALANCE_MULTIPLIER:-4}
	},
	"checksum": {
		"type":			"xxhash",
		"validate_cold_get":	true,
		"validate_warm_get":	false,
		"validate_obj_move":	false,
		"enable_read_range":	false
	},
	"compression": {
		"block_size": ${BLOCK_SIZE:-262144},
		"checksum":   ${CHECKSUM:-false}
	},
	"versioning": {
		"enabled":           true,
		"validate_warm_get": false
	},
	"fspaths": {
		$AIS_FS_PATHS
	},
	"test_fspaths": {
		"root":     "${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}",
		"count":    ${TEST_FSPATH_COUNT:-0},
		"instance": ${INSTANCE:-0}
	},
	"net": {
		"ipv4":                 "${IPV4LIST}",
		"ipv4_intra_control":   "${IPV4LIST_INTRA_CONTROL}",
		"ipv4_intra_data":      "${IPV4LIST_INTRA_DATA}",
		"l4": {
			"proto":              "tcp",
			"port":               "${PORT:-8080}",
			"port_intra_control": "${PORT_INTRA_CONTROL:-9080}",
			"port_intra_data":    "${PORT_INTRA_DATA:-10080}",
			"sndrcv_buf_size":    ${SNDRCV_BUF_SIZE:-131072}
		},
		"http": {
			"use_https":         ${AIS_USE_HTTPS:-false},
			"server_crt":        "${AIS_SERVER_CRT:-server.crt}",
			"server_key":        "${AIS_SERVER_KEY:-server.key}",
			"write_buffer_size": ${HTTP_WRITE_BUFFER_SIZE:-0},
			"read_buffer_size":  ${HTTP_READ_BUFFER_SIZE:-0},
			"chunked_transfer":  ${CHUNKED_TRANSFER:-true},
			"skip_verify":       ${AIS_SKIP_VERIFY_CRT:-false}
		}
	},
	"fshc": {
		"enabled":     true,
		"test_files":  4,
		"error_limit": 2
	},
	"auth": {
		"secret":      "$AIS_SECRET_KEY",
		"enabled":     ${AUTH_ENABLED:-false}
	},
	"keepalivetracker": {
		"proxy": {
			"interval": "10s",
			"name":     "heartbeat",
			"factor":   3
		},
		"target": {
			"interval": "10s",
			"name":     "heartbeat",
			"factor":   3
		},
		"retry_factor":   5,
		"timeout_factor": 3
	},
	"downloader": {
		"timeout": "1h"
	},
	"distributed_sort": {
		"duplicated_records":    "ignore",
		"missing_shards":        "ignore",
		"ekm_malformed_line":    "abort",
		"ekm_missing_key":       "abort",
		"default_max_mem_usage": "80%",
		"dsorter_mem_threshold": "100GB",
		"compression":           "${COMPRESSION:-never}",
		"call_timeout":          "10m"
	}
}
EOL

cat > $STATSD_CONF_FILE <<EOL
{
	graphitePort: ${GRAPHITE_PORT:-2003},
	graphiteHost: "${GRAPHITE_SERVER:-localhost}"
}
EOL

cat > $COLLECTD_CONF_FILE <<EOL
LoadPlugin df
LoadPlugin cpu
LoadPlugin disk
LoadPlugin interface
LoadPlugin load
LoadPlugin memory
LoadPlugin processes
LoadPlugin write_graphite

<Plugin syslog>
	LogLevel info
</Plugin>

<Plugin df>
	FSType rootfs
	FSType sysfs
	FSType proc
	FSType devtmpfs
	FSType devpts
	FSType tmpfs
	FSType fusectl
	FSType cgroup
	IgnoreSelected true
	ValuesPercentage True
</Plugin>

<Plugin write_graphite>
	<Node "graphiting">
		Host "${GRAPHITE_SERVER:-localhost}"
		Port "${GRAPHITE_PORT:-2003}"
		Protocol "tcp"
		LogSendErrors true
		StoreRates true
		AlwaysAppendDS false
		EscapeCharacter "_"
	</Node>
</Plugin>

<Include "/etc/collectd/collectd.conf.d">
	Filter "*.conf"
</Include>
EOL
