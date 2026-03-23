authn_auth_block=$(printf '\t\t"expiration_time": "%s"' "${AIS_AUTHN_TTL:-24h}")
if [[ -n "$AIS_AUTHN_SECRET_KEY" ]]; then
  authn_auth_block=$(printf '\t\t"secret": "%s",\n%s' "$AIS_AUTHN_SECRET_KEY" "$authn_auth_block")
fi

cat > "$AIS_AUTHN_CONF_DIR/authn.json" <<EOL
{
	"log": {
		"dir":   "$AIS_AUTHN_LOG_DIR",
		"level": "${AIS_AUTHN_LOG_LEVEL:-3}"
	},
	"net": {
		"http": {
			"port":	${AIS_AUTHN_PORT:-52001},
			"use_https": ${AIS_AUTHN_USE_HTTPS:-false},
			"server_crt": "${AIS_SERVER_CRT:-server.crt}",
			"server_key": "${AIS_SERVER_KEY:-server.key}"
		}
	},
	"auth": {
${authn_auth_block}
	},
	"timeout": {
		"default_timeout": "30s"
	}
}
EOL

