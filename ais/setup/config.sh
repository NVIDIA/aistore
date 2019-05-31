cat > $CONFFILE <<EOL
{
	"confdir":       "${CONFDIR}",
	"cloudprovider": "${CLDPROVIDER}",
	"mirror": {
		"copies":       2,
		"burst_buffer": 512,
		"util_thresh":  ${MIRROR_UTIL_THRESH:-20},
		"optimize_put": false,
		"enabled":      ${MIRROR_ENABLED:-false}
	},
	"readahead": {
		"object_mem": 1048576,
		"total_mem":  1073741824,
		"by_proxy":   true,
		"discard":    false,
		"enabled":    false
	},
	"log": {
		"dir":       "${LOGDIR:-/tmp/ais$NEXT_TIER/log}",
		"level":     "${LOGLEVEL:-3}",
		"max_size":  4194304,
		"max_total": 67108864
	},
	"periodic": {
		"stats_time":        "10s",
		"retry_sync_time":   "2s"
	},
	"timeout": {
		"default_timeout":	"10s",
		"default_long_timeout":	"30m",
		"list_timeout":		"2m",
		"max_keepalive":	"4s",
		"proxy_ping":		"100ms",
		"cplane_operation":	"2s",
		"send_file_time":	"5m",
		"startup_time":		"1m"
	},
	"proxy": {
		"non_electable": ${NON_ELECTABLE:-false},
		"primary_url":   "${PROXYURL}",
		"original_url":  "${PROXYURL}",
		"discovery_url": "${DISCOVERYURL}"
	},
	"lru": {
		"lowwm":		75,
		"highwm":		90,
		"out_of_space":		95,
		"dont_evict_time":	"120m",
		"capacity_upd_time":	"10m",
		"local_buckets":	false,
		"enabled":		true
	},
	"disk":{
	    "iostat_time_long":  "${IOSTAT_TIME_LONG:-2s}",
	    "iostat_time_short": "${IOSTAT_TIME_SHORT:-100ms}",
	    "disk_util_low_wm":  20,
	    "disk_util_high_wm": 80
	},
	"rebalance": {
		"dest_retry_time":	"2m",
		"multiplier":		${REBALANCE_MULTIPLIER:-4},
		"enabled":		true
	},
	"cksum": {
		"type":			"xxhash",
		"validate_cold_get":	true,
		"validate_warm_get":	false,
		"validate_obj_move":	false,
		"enable_read_range":	false
	},
	"versioning": {
		"enabled":		"true",
		"validate_warm_get":	false
	},
	"fspaths": {
		$FSPATHS
	},
	"test_fspaths": {
		"root":     "${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}",
		"count":    ${TEST_FSPATH_COUNT:-0},
		"instance": ${INSTANCE:-0}
	},
	"net": {
		"ipv4":               "${IPV4LIST}",
		"ipv4_intra_control": "${IPV4LIST_INTRA_CONTROL}",
		"ipv4_intra_data":    "${IPV4LIST_INTRA_DATA}",
		"l4": {
			"proto":              "tcp",
			"port":	              "${PORT:-8080}",
			"port_intra_control": "${PORT_INTRA_CONTROL:-9080}",
			"port_intra_data":    "${PORT_INTRA_DATA:-10080}"
		},
		"http": {
			"proto":		"http",
			"rproxy":		"",
			"rproxy_cache":		true,
			"server_certificate":	"server.crt",
			"server_key":		"server.key",
			"use_https":		${USE_HTTPS:-false}
		}
	},
	"fshc": {
		"enabled":     true,
		"test_files":  4,
		"error_limit": 2
	},
	"auth": {
		"secret":  "$SECRETKEY",
		"enabled": ${AUTHENABLED:-false},
		"creddir": "$CREDDIR"
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
		"missing_shards":        "abort",
		"default_max_mem_usage": "80%",
		"call_timeout":          "10m"
	}
}
EOL

cat > $CONFFILE_STATSD <<EOL
{
	graphitePort: ${GRAPHITE_PORT:-2003},
	graphiteHost: "${GRAPHITE_SERVER:-localhost}"
}
EOL

cat > $CONFFILE_COLLECTD <<EOL
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
