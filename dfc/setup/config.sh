	cat > $CONFFILE <<EOL
{
	"logdir":			"$LOGDIR",
	"loglevel": 			"${LOGLEVEL}",
	"cloudprovider":		"${CLDPROVIDER}",
	"cloud_buckets":		"cloud",
	"local_buckets":		"local",
	"lb_conf":                	"localbuckets",
	"stats_time":			"10s",
	"http": {
		"timeout":		"30s",
		"long_timeout":		"30m"
	},
	"keep_alive_time":		"30s",
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
		"validate_warm_get":	false
	},
	"lru_config": {
		"lowwm":		75,
		"highwm":		90,
		"dont_evict_time":	"120m",
		"lru_enabled":  	true
	},
	"test_fspaths": {
		"root":			"/tmp/dfc/",
		"count":		$TESTFSPATHCOUNT,
		"instance":		$c
	},
	"fspaths": {
		"/tmp/dfc":		"",
		"/disk2/dfc":		""
	},
	"ack_policy": {
		"put":			"disk",
		"max_mem_mb":		16
	}
}
EOL
