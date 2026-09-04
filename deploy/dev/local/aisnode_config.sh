# NOTE: system environment variables are listed in the `env` package,
# see https://github.com/NVIDIA/aistore/blob/main/api/env/README.md

# NOTE: aws.extra settings:
#      "extra.aws.cloud_region"
#      "extra.aws.endpoint"
#      "extra.aws.profile"
#      "extra.aws.max_pagesize"
#      "extra.aws.multipart_size"

# AIS auth configuration:
# - JWT validation -- see authn_config.sh for AuthN service config
# - intra-cluster request protection
#
# NOTE: auth.intra_cluster is **independent of** auth.client_auth_required.
# The two examples below separately enable one or the other:
# Example #1: AIS_AUTHN_ENABLED=true AIS_AUTHN_ALLOWED_ISS='https://issuer-one.example, https://issuer-two.example' make deploy
# Example #2: AIS_AUTH_INTRA_CLUSTER_REQUEST_AUTH=true make deploy

make_auth_intra_cluster_conf() {
	local conf=""

	if [[ "${AIS_AUTH_INTRA_CLUSTER_REQUEST_AUTH:-false}" == "true" ]]; then
		conf='"request_auth": true'
	fi
	if [[ -n "$AIS_AUTH_INTRA_CLUSTER_TTL" ]]; then
		conf="${conf:+${conf}, }\"ttl\": \"${AIS_AUTH_INTRA_CLUSTER_TTL}\""
	fi
	if [[ -n "$AIS_AUTH_INTRA_CLUSTER_NONCE_WINDOW" ]]; then
		conf="${conf:+${conf}, }\"nonce_window\": \"${AIS_AUTH_INTRA_CLUSTER_NONCE_WINDOW}\""
	fi
	if [[ -n "$AIS_AUTH_INTRA_CLUSTER_ROTATION_GRACE" ]]; then
		conf="${conf:+${conf}, }\"rotation_grace\": \"${AIS_AUTH_INTRA_CLUSTER_ROTATION_GRACE}\""
	fi
	if [[ -n "$AIS_AUTH_INTRA_CLUSTER_NODE_JOIN_SECRET_PATH" ]]; then
		conf="${conf:+${conf}, }\"node_join_secret_path\": \"${AIS_AUTH_INTRA_CLUSTER_NODE_JOIN_SECRET_PATH}\""
	fi

	echo "{${conf}}"
}

make_auth_conf() {
	local conf=""

	if [[ -n "$AIS_AUTHN_SECRET_KEY" ]]; then
		conf='"signature": {"method": "HMAC"}'
	elif [[ -n "$AIS_AUTHN_PUBLIC_KEY" ]]; then
		conf='"signature": {"method": "RSA"}'
	elif [[ -n "$AIS_AUTHN_ALLOWED_ISS" ]]; then
		local json_arr
		json_arr=$(printf '%s\n' "$AIS_AUTHN_ALLOWED_ISS" | awk -F',' '{
			printf "["
			n = 0
			for (i=1; i<=NF; i++) {
				gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
				if ($i == "") continue
				gsub(/\\/, "\\\\", $i)
				gsub(/"/, "\\\"", $i)
				if (n++ > 0) printf ","
				printf "\"%s\"", $i
			}
			printf "]"
		}')
		if [[ "$json_arr" != "[]" ]]; then
			conf="\"oidc\": {\"allowed_iss\": ${json_arr}}"
		fi
	fi

	if [[ "${AIS_AUTHN_ENABLED:-false}" == "true" && \
	      -z "$AIS_AUTHN_SECRET_KEY" && \
	      -z "$AIS_AUTHN_PUBLIC_KEY" && \
	      -z "$AIS_AUTHN_ALLOWED_ISS" ]]; then
		echo "Warning: AIS_AUTHN_ENABLED=true requires AIS_AUTHN_SECRET_KEY, AIS_AUTHN_PUBLIC_KEY, or AIS_AUTHN_ALLOWED_ISS" >&2
	fi

	if [[ -n "$AIS_AUTH_INTRA_CLUSTER_REQUEST_AUTH" || \
	      -n "$AIS_AUTH_INTRA_CLUSTER_TTL" || \
	      -n "$AIS_AUTH_INTRA_CLUSTER_NONCE_WINDOW" || \
	      -n "$AIS_AUTH_INTRA_CLUSTER_ROTATION_GRACE" || \
	      -n "$AIS_AUTH_INTRA_CLUSTER_NODE_JOIN_SECRET_PATH" ]]; then
		conf="${conf:+${conf}, }\"intra_cluster\": $(make_auth_intra_cluster_conf)"
	fi
	if [[ "${AIS_AUTHN_ENABLED:-false}" == "true" ]]; then
		conf="${conf:+${conf}, }\"client_auth_required\": true"
	fi

	echo "{${conf}}"
}

##
## NOTE: AIS_SPACE_* environment variables are used by constrained GitHub/GitLab CI environments
##
cat > "$AIS_CONF_FILE" <<EOL
{
	"backend": $(make_backend_conf),
	$(make_tracing_conf)
	"timeout": {
		"cplane_operation":     "2s",
		"max_keepalive":        "5s",
		"cold_get_conflict":    "5s",
		"max_host_busy":        "20s",
		"startup_time":         "${AIS_STARTUP_TIME:-1m}",
		"join_startup_time":    "${AIS_JOIN_STARTUP_TIME:-3m}",
		"send_file_time":       "5m",
		"ec_streams_time":	"10m",
		"object_md":            "2h"
	},
	"proxy": {
		"primary_url":   "${AIS_PRIMARY_URL}",
		"original_url":  "${AIS_PRIMARY_URL}",
		"discovery_url": "${AIS_DISCOVERY_URL}",
		"non_electable": ${AIS_NON_ELECTABLE:-false}
	},
	"space": {
		"cleanupwm":         65,
		"lowwm":             ${AIS_SPACE_LOWWM:-75},
		"highwm":            ${AIS_SPACE_HIGHWM:-90},
		"out_of_space":      ${AIS_SPACE_OOS:-95},
		"batch_size":        32768,
		"dont_cleanup_time": "120m"
	},
	"resilver": {
		"enabled": true
	},
	"memsys": {
		"min_free":		"2gb",
		"default_buf":		"32kb",
		"to_gc":		"4gb",
		"hk_time":		"3m",
		"min_pct_total":	0,
		"min_pct_free":		0
	},
	"versioning": {
		"enabled":           true,
		"validate_warm_get": false
	},
	"net": {
		"l4": {
			"proto":              "tcp",
			"sndrcv_buf_size":    ${SNDRCV_BUF_SIZE:-131072}
		},
		"http": {
			"use_https":          ${AIS_USE_HTTPS:-false},
			"server_crt":         "${AIS_SERVER_CRT:-server.crt}",
			"server_key":         "${AIS_SERVER_KEY:-server.key}",
			"domain_tls":         "",
			"client_ca_tls":      "${AIS_CLIENT_CA_TLS}",
			"client_auth_tls":    ${AIS_CLIENT_AUTH_TLS:-0},
			"idle_conn_time":     "6s",
			"idle_conns_per_host":32,
			"idle_conns":         256,
			"write_buffer_size":  ${HTTP_WRITE_BUFFER_SIZE:-0},
			"read_buffer_size":   ${HTTP_READ_BUFFER_SIZE:-0},
			"chunked_transfer":   ${AIS_HTTP_CHUNKED_TRANSFER:-true},
			"skip_verify":        ${AIS_SKIP_VERIFY_CRT:-false}
		},
		"use_ipv6":          ${AIS_USE_IPv6:-false}
	},
	"auth": $(make_auth_conf),
	"distributed_sort": {
		"duplicated_records":    "ignore",
		"missing_shards":        "ignore",
		"ekm_malformed_line":    "abort",
		"ekm_missing_key":       "abort",
		"default_max_mem_usage": "80%",
		"call_timeout":          "10m",
		"dsorter_mem_threshold": "100GB",
		"compression":           "${AIS_DSORT_COMPRESSION:-never}",
		"bundle_multiplier":	 ${AIS_DSORT_BUNDLE_MULTIPLIER:-4}
	}
}
EOL

cat > $AIS_LOCAL_CONF_FILE <<EOL
{
	"confdir": "${AIS_CONF_DIR:-/etc/ais/}",
	"log_dir": "${AIS_LOG_DIR:-/tmp/ais$NEXT_TIER/log}",
	"host_net": {
		"hostname":                 "${HOSTNAME_LIST}",
		"hostname_intra_control":   "${HOSTNAME_LIST_INTRA_CONTROL}",
		"hostname_intra_data":      "${HOSTNAME_LIST_INTRA_DATA}",
		"port":               "${PORT:-8080}",
		"port_intra_control": "${PORT_INTRA_CONTROL:-9080}",
		"port_intra_data":    "${PORT_INTRA_DATA:-10080}"
	},
	"fspaths": {
		$AIS_FS_PATHS
	},
	"test_fspaths": {
		"root":     "${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}",
		"count":    ${TEST_FSPATH_COUNT:-0},
		"instance": ${INSTANCE:-0}
	}
}
EOL
