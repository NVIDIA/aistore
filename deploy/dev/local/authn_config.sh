cat > $AUTHN_CONF_FILE <<EOL
{
	"confdir": "$AUTHN_CONF_DIR",
	"log": {
		"dir":   "$AUTHN_LOG_DIR",
		"level": "${AUTHN_LOG_LEVEL:-3}"
	},
	"net": {
		"http": {
			"port":	${AIS_AUTHN_PORT:-52001},
			"use_https": ${AIS_USE_HTTPS:-false},
			"server_crt": "${AIS_SERVER_CRT:-server.crt}",
			"server_key": "${AIS_SERVER_KEY:-server.key}"
		}
	},
	"auth": {
		"secret": "$AIS_SECRET_KEY",
		"expiration_time": "${AIS_AUTHN_TTL:-24h}"
	},
	"timeout": {
		"default_timeout": "30s"
	}
}
EOL

