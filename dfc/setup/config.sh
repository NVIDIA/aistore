	cat > $CONFFILE <<EOL
{
	"logdir":			"$LOGDIR",
	"loglevel": 			"${LOGLEVEL}",
	"cloudprovider":		"${CLDPROVIDER}",
	"cloud_buckets":		"cloud",
	"local_buckets":		"local",
	"lb_conf":                	"localbuckets",
	"stats_time":			"10s",
	"http_timeout":			"60s",
	"keep_alive_time":		"30s",
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
                 "validate_cold_get":    true,
                 "checksum":             "xxhash"
	},
	"lru_config": {
		"lowwm":		75,
		"highwm":		90,
		"dont_evict_time":      "120m",
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
	"h2c": 				false
}
EOL
