cat > $CONFFILE <<EOL
{
	"confdir": "$CONFDIR",
	"log": {
		"dir":   "$LOGDIR",
		"level": "${LOGLEVEL:-3}"
	},
	"proxy": {
		"url": "$PROXYURL"
	},
	"net": {
		"http": {
			"port": ${AUTHN_PORT:-52001},
			"use_https": false,
			"server_cert": "server.pem",
			"server_key": "server.key"
		}
	},
	"auth": {
		"secret": "$SECRETKEY",
		"username": "$AUTH_SU_NAME",
		"password": "$AUTH_SU_PASS",
		"expiration_time": "${AUTHN_TTL:-24h}"
	},
	"timeout": {
		"default_timeout": "30s"
	}
}
EOL

