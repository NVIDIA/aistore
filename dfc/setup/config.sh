cat > $CONFFILE <<EOL
{
	"confdir":                	"$CONFDIR",
	"cloudprovider":		"${CLDPROVIDER}",
	"readahead": {
		"rahobjectmem":		1048576,
		"rahtotalmem":		1073741824,
		"rahbyproxy":		true,
		"rahdiscard":		false,
		"rahenabled":		false
	},
	"log": {
		"logdir":		"$LOGDIR",
		"loglevel": 		"${LOGLEVEL}",
		"logmaxsize": 		4194304,
		"logmaxtotal":		67108864
	},
	"periodic": {
		"stats_time":		"10s",
		"iostat_time":		"2s",
		"retry_sync_time":	"2s"
	},
	"timeout": {
		"default_timeout":	"30s",
		"default_long_timeout":	"30m",
		"max_keepalive":	"4s",
		"proxy_ping":		"100ms",
		"cplane_operation":	"1s",
		"send_file_time":	"5m",
		"startup_time":		"1m"
	},
	"proxyconfig": {
		"non_electable":	${NON_ELECTABLE},
		"primary_url":		"${PROXYURL}",
		"original_url": 	"${PROXYURL}",
		"discovery_url": 	"${DISCOVERYURL}"
	},
	"lru_config": {
		"lowwm":		75,
		"highwm":		90,
		"atime_cache_max":	65536,
		"dont_evict_time":	"120m",
		"capacity_upd_time":	"10m",
		"lru_enabled":  	true
	},
	"xaction_config":{
	    "disk_util_low_wm":      20,
	    "disk_util_high_wm":     80
	},
	"rebalance_conf": {
		"dest_retry_time":	"2m",
		"rebalancing_enabled": 	true
	},
	"cksum_config": {
		"checksum":                    "xxhash",
		"validate_checksum_cold_get":  true,
		"validate_checksum_warm_get":  false,
		"enable_read_range_checksum":  false
	},
	"version_config": {
		"validate_version_warm_get":    false,
		"versioning":                   "all"
	},
	"fspaths": {
		$FSPATHS
	},
	"test_fspaths": {
		"root":			"/tmp/dfc$NEXT_TIER/",
		"count":		$TESTFSPATHCOUNT,
		"instance":		$c
	},
	"netconfig": {
		"ipv4":               "${IPV4LIST}",
		"ipv4_intra_control": "${IPV4LIST_INTRA_CONTROL}",
		"ipv4_intra_data":     "${IPV4LIST_INTRA_DATA}",
		"l4": {
			"proto":              "tcp",
			"port":	              "${PORT}",
			"port_intra_control": "${PORT_INTRA_CONTROL}",
			"port_intra_data":    "${PORT_INTRA_DATA}"
		},
		"http": {
			"proto":		"http",
			"rproxy":		"",
			"server_certificate":	"server.crt",
			"server_key":		"server.key",
			"max_num_targets":	16,
			"use_https":		${USE_HTTPS}
		}
	},
	"fshc": {
		"fshc_enabled":		true,
		"fshc_test_files":	4,
		"fshc_error_limit":	2
	},
	"auth": {
		"secret": "$SECRETKEY",
		"enabled": $AUTHENABLED,
		"creddir": "$CREDDIR"
	},
	"keepalivetracker": {
		"proxy": {
			"interval": "10s",
			"name": "heartbeat",
			"factor": 3
		},
		"target": {
			"interval": "10s",
			"name": "heartbeat",
			"factor": 3
		}
	}
}
EOL

cat > $CONFFILE_STATSD <<EOL
{
	graphitePort: ${GRAPHITE_PORT},
	graphiteHost: "${GRAPHITE_SERVER}"
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
		Host "${GRAPHITE_SERVER}"
		Port "${GRAPHITE_PORT}"
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
