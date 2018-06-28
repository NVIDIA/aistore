	cat > $CONFFILE <<EOL
{
	"confdir":                	"$CONFDIR",
	"cloudprovider":		"${CLDPROVIDER}",
	"cloud_buckets":		"cloud",
	"local_buckets":		"local",
	"log": {
		"logdir":		"$LOGDIR",
		"loglevel": 		"${LOGLEVEL}",
		"logmaxsize": 		4194304,
		"logmaxtotal":		67108864
	},
	"periodic": {
		"stats_time":		"10s",
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
		"primary": {
			"id":		"${PROXYID}",
			"url": 		"${PROXYURL}",
			"passthru": 	true
		},
		"original": {
			"id":		"${PROXYID}",
			"url": 		"${PROXYURL}",
			"passthru": 	true
		}
	},
	"lru_config": {
		"lowwm":		75,
		"highwm":		90,
		"atime_cache_max":	65536,
		"dont_evict_time":	"120m",
		"capacity_upd_time":	"10m",
		"lru_enabled":  	true
	},
	"rebalance_conf": {
		"startup_delay_time":	"3m",
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
		"root":			"/tmp/dfc/",
		"count":		$TESTFSPATHCOUNT,
		"instance":		$c
	},
	"netconfig": {
		"ipv4": "$IPV4LIST",
		"l4": {
			"proto": 	"tcp",
			"port":		"${PORT}"
		},
		"http": {
			"max_num_targets":    16,
			"use_https":          ${USE_HTTPS},
			"use_http2":          false,
			"use_as_proxy":       false,
			"server_certificate": "server.crt",
			"server_key":         "server.key"
		}
	},
	"fskeeper": {
		"fs_check_time":         "0",
		"offline_fs_check_time": "0",
		"fskeeper_enabled":      false
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
			"max": "20s",
			"factor": 3
		},
		"target": {
			"interval": "10s",
			"name": "heartbeat",
			"max": "20s",
			"factor": 3
		}
	},
	"callstats": {
		"request_included": [ "keepalive", "metasync" ],
		"factor": 2.5
	}
}
EOL

	cat > $CONFFILE_STATSD <<EOL
{
	graphitePort: 2003,
	graphiteHost: "${GRAPHITE_SERVER}",
	port: 8125
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
                Port "2003"
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
