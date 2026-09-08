#!/usr/bin/with-contenv bashio
# Spouštěč add-onu: přístupy k Mosquittu vezme od Supervisoru (services: mqtt:need),
# zbytek konfigurace si Python načte sám z /data/options.json.
set -e

if bashio::services.available mqtt; then
    export MQTT_HOST="$(bashio::services mqtt 'host')"
    export MQTT_PORT="$(bashio::services mqtt 'port')"
    export MQTT_USERNAME="$(bashio::services mqtt 'username')"
    export MQTT_PASSWORD="$(bashio::services mqtt 'password')"
    bashio::log.info "Mosquitto: ${MQTT_HOST}:${MQTT_PORT}"
else
    bashio::log.warning "Služba MQTT není k dispozici – statistiky se do HA nedostanou, dokud nepoběží Mosquitto add-on."
fi

export BAMBU_OPTIONS=/data/options.json
export BAMBU_DATA_DIR=/data
export LOG_LEVEL="$(bashio::config 'log_level' 'info')"

cd /usr/src
exec python3 -m bambu_stats
