PROJECT = emqx_sn
PROJECT_DESCRIPTION = EMQ X MQTT-SN Gateway
PROJECT_VERSION = 2.3.0

DEPS = esockd clique
dep_esockd = git https://github.com/emqtt/esockd master
dep_clique = git https://github.com/emqtt/clique

BUILD_DEPS = emqx cuttlefish
dep_emqx = git git@github.com:emqx/emqx.git X
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emqx_sn.conf -i priv/emqx_sn.schema -d data
