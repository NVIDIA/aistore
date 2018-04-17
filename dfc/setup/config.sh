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
		"keep_alive_time":	"20s"
	},
	"timeout": {
		"default":		"30s",
		"default_long":		"30m",
		"max_keepalive":	"4s",
		"proxy_ping":		"100ms",
		"vote_request":		"2s"
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
		"startup_delay_time":	"10m",
		"rebalancing_enabled": 	true
	},
	"cksum_config": {
                 "checksum":		"xxhash",
                 "validate_cold_get":	true
	},
	"version_config": {
		"validate_warm_get":	false,
		"versioning":		"all"
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
			"use_https":          false,
			"server_certificate": "server.pem",
			"server_key":         "server.key"
		}
	},
	"fskeeper": {
		"fs_check_time":         "0",
		"offline_fs_check_time": "0",
		"fskeeper_enabled":      false
	},
	"experimental": {
		"ack_put":		"disk",
		"max_mem_mb":		16
	},
	"h2c": 				false
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
