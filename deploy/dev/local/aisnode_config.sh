# NOTE: system environment variables are listed in the `env` package,
# see https://github.com/NVIDIA/aistore/blob/main/api/env/README.md

# NOTE: aws.extra settings:
#      "extra.aws.cloud_region"
#      "extra.aws.endpoint"
#      "extra.aws.profile"
#      "extra.aws.max_pagesize"
#      "extra.aws.multipart_size"

cat > $AIS_CONF_FILE <<EOL
{
	"backend": $(make_backend_conf),
	"mirror": {
		"copies":       2,
		"burst_buffer": 128,
		"enabled":      ${AIS_MIRROR_ENABLED:-false}
	},
	$(make_tracing_conf)
	"ec": {
		"objsize_limit":	${AIS_OBJ_SIZE_LIMIT:-262144},
		"compression":		"${AIS_EC_COMPRESSION:-never}",
		"bundle_multiplier":	${AIS_EC_BUNDLE_MULTIPLIER:-2},
		"data_slices":		${AIS_DATA_SLICES:-1},
		"parity_slices":	${AIS_PARITY_SLICES:-1},
		"enabled":		${AIS_EC_ENABLED:-false},
		"disk_only":		false
	},
	"log": {
		"level":     "${AIS_LOG_LEVEL:-3}",
		"max_size":  "4mb",
		"max_total": "128mb",
		"flush_time": "60s",
		"stats_time": "60s"
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
		"join_startup_time":    "3m",
		"send_file_time":       "5m",
		"ec_streams_time":	"10m",
		"object_md":            "2h"
	},
	"client": {
		"client_timeout":      "10s",
		"client_long_timeout": "10m",
		"list_timeout":        "1m"
	},
	"proxy": {
		"primary_url":   "${AIS_PRIMARY_URL}",
		"original_url":  "${AIS_PRIMARY_URL}",
		"discovery_url": "${AIS_DISCOVERY_URL}",
		"non_electable": ${AIS_NON_ELECTABLE:-false}
	},
	"space": {
		"cleanupwm":         65,
		"lowwm":             ${AIS_SPACE_LOWWM:-75},
		"highwm":            ${AIS_SPACE_HIGHWM:-90},
		"out_of_space":      ${AIS_SPACE_OOS:-95}
	},
	"lru": {
		"dont_evict_time":   "120m",
		"capacity_upd_time": "10m",
		"enabled":           true
	},
	"disk":{
	    "iostat_time_long":  "${AIS_IOSTAT_TIME_LONG:-2s}",
	    "iostat_time_short": "${AIS_IOSTAT_TIME_SHORT:-100ms}",
	    "disk_util_low_wm":  20,
	    "disk_util_high_wm": 80,
	    "disk_util_max_wm":  95
	},
	"rebalance": {
		"dest_retry_time":	"2m",
		"compression":     	"${AIS_REBALANCE_COMPRESSION:-never}",
		"bundle_multiplier":	${AIS_REBALANCE_BUNDLE_MULTIPLIER:-2},
		"enabled":         	true
	},
	"resilver": {
		"enabled": true
	},
	"checksum": {
		"type":			"xxhash2",
		"validate_cold_get":	false,
		"validate_warm_get":	false,
		"validate_obj_move":	false,
		"enable_read_range":	false
	},
	"transport": {
		"max_header":		4096,
		"burst_buffer":		512,
		"idle_teardown":	"${AIS_TRANSPORT_IDLE_TEARDOWN:-4s}",
		"quiescent":		"${AIS_TRANSPORT_QUIESCENT:-10s}",
		"lz4_block":		"${AIS_TRANSPORT_LZ4_BLOCK:-256kb}",
		"lz4_frame_checksum":	${AIS_TRANSPORT_LZ4_FRAME_CHECKSUM:-false}
	},
	"memsys": {
		"min_free":		"2gb",
		"default_buf":		"32kb",
		"to_gc":		"4gb",
		"hk_time":		"3m",
		"min_pct_total":	0,
		"min_pct_free":		0
	},
	"versioning": {
		"enabled":           true,
		"validate_warm_get": false
	},
	"net": {
		"l4": {
			"proto":              "tcp",
			"sndrcv_buf_size":    ${SNDRCV_BUF_SIZE:-131072}
		},
		"http": {
			"use_https":          ${AIS_USE_HTTPS:-false},
			"server_crt":         "${AIS_SERVER_CRT:-server.crt}",
			"server_key":         "${AIS_SERVER_KEY:-server.key}",
			"domain_tls":         "",
			"client_ca_tls":      "${AIS_CLIENT_CA_TLS}",
			"client_auth_tls":    ${AIS_CLIENT_AUTH_TLS:-0},
			"idle_conn_time":     "6s",
			"idle_conns_per_host":32,
			"idle_conns":         0,
			"write_buffer_size":  ${HTTP_WRITE_BUFFER_SIZE:-0},
			"read_buffer_size":   ${HTTP_READ_BUFFER_SIZE:-0},
			"chunked_transfer":   ${AIS_HTTP_CHUNKED_TRANSFER:-true},
			"skip_verify":        ${AIS_SKIP_VERIFY_CRT:-false}
		}
	},
	"fshc": {
		"test_files":     4,
		"error_limit":    2,
		"io_err_limit":   10,
		"io_err_time":    "10s",
		"enabled":        true
	},
	"auth": {
		"secret":      "$AIS_AUTHN_SECRET_KEY",
		"enabled":     ${AIS_AUTHN_ENABLED:-false}
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
		"num_retries":    3,
		"retry_factor":   4
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
		"call_timeout":          "10m",
		"dsorter_mem_threshold": "100GB",
		"compression":           "${AIS_DSORT_COMPRESSION:-never}",
		"bundle_multiplier":	 ${AIS_DSORT_BUNDLE_MULTIPLIER:-4}
	},
	"tcb": {
		"compression":		"never",
		"bundle_multiplier":	2
	},
	"write_policy": {
		"data": "${WRITE_POLICY_DATA:-}",
		"md": "${WRITE_POLICY_MD:-}"
	},
	"rate_limit": {
		"backend": {
			"num_retries":       3,
			"interval":          "1m",
			"per_op_max_tokens": "",
			"max_tokens":        1000,
			"enabled":           false
		},
		"frontend": {
			"burst_size":        375,
			"interval":          "1m",
			"per_op_max_tokens": "",
			"max_tokens":        1000,
			"enabled":           false
		}
	},
	"features": "0"
}
EOL

cat > $AIS_LOCAL_CONF_FILE <<EOL
{
	"confdir": "${AIS_CONF_DIR:-/etc/ais/}",
	"log_dir": "${AIS_LOG_DIR:-/tmp/ais$NEXT_TIER/log}",
	"host_net": {
		"hostname":                 "${HOSTNAME_LIST}",
		"hostname_intra_control":   "${HOSTNAME_LIST_INTRA_CONTROL}",
		"hostname_intra_data":      "${HOSTNAME_LIST_INTRA_DATA}",
		"port":               "${PORT:-8080}",
		"port_intra_control": "${PORT_INTRA_CONTROL:-9080}",
		"port_intra_data":    "${PORT_INTRA_DATA:-10080}"
	},
	"fspaths": {
		$AIS_FS_PATHS
	},
	"test_fspaths": {
		"root":     "${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}",
		"count":    ${TEST_FSPATH_COUNT:-0},
		"instance": ${INSTANCE:-0}
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
