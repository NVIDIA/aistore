cat > $CONFFILE <<EOL
{
	"confdir": "$CONFDIR",
	"log": {
		"logdir": "$LOGDIR",
		"loglevel": "$LOGLEVEL"
	},
	"proxy": {
		"url": "$PROXYURL"
	},
	"net": {
		"http": {
			"port": 8203,
			"use_https": false,
			"server_cert": "server.pem",
			"server_key": "server.key"
		}
	},
	"auth": {
		"secret": "$SECRETKEY",
		"username": "$AUTH_SU_NAME",
		"password": "$AUTH_SU_PASS",
		"expiration_time": "30m"
	},
	"timeout": {
		"default_timeout": "30s"
	}
}
EOL

