PROJECT = emq_sn
PROJECT_DESCRIPTION = EMQ MQTT-SN Gateway
PROJECT_VERSION = 0.3

DEPS = esockd
dep_esockd = git https://github.com/emqtt/esockd emq20

BUILD_DEPS = emqttd
dep_emqttd = git https://github.com/emqtt/emqttd master

TEST_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

ERLC_OPTS += +'{parse_transform, lager_transform}'

include erlang.mk

app.config::
	cuttlefish -l info -e etc/ -c etc/emq_sn.conf -i priv/emq_sn.schema -d data
