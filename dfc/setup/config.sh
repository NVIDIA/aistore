	cat > $CONFFILE <<EOL
{
	"logdir":			"$LOGDIR",
	"confdir":                	"$CONFDIR",
	"loglevel": 			"${LOGLEVEL}",
	"cloudprovider":		"${CLDPROVIDER}",
	"cloud_buckets":		"cloud",
	"local_buckets":		"local",
	"stats_time":			"10s",
	"http": {
		"timeout":		"30s",
		"long_timeout":		"30m"
	},
	"keep_alive_time":		"20s",
	"h2c": 				false,
	"listen": {
		"proto": 		"tcp",
		"port":			"${PORT}"
	},
	"proxy": {
		"url": 			"${PROXYURL}",
		"passthru": 		true
	},
	"s3": {
		"maxconcurrdownld":	64,
		"maxconcurrupld":	64,
		"maxpartsize":		4294967296
	},
	"cksum_config": {
                 "validate_cold_get":	true,
                 "checksum":		"xxhash"
	},
	"version_config": {
		"validate_warm_get":	false,
		"versioning":		"all"
	},
	"lru_config": {
		"lowwm":		75,
		"highwm":		90,
		"dont_evict_time":	"120m",
		"capacity_upd_time":	"10m",
		"lru_enabled":  	true
	},
	"rebalance_conf": {
		"startup_delay_time":	"10m",
		"rebalancing_enabled": 	true
	},
	"test_fspaths": {
		"root":			"/tmp/dfc/",
		"count":		$TESTFSPATHCOUNT,
		"instance":		$c
	},
	"fspaths": {
		$FSPATHS
	},
	"network": {
		"ipv4": "$IPV4LIST"
	},
	"ack_policy": {
		"put":			"disk",
		"max_mem_mb":		16
	},
	"diskkeeper": {
		"fs_check_time":         "30s",
		"offline_fs_check_time": "2m"
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
