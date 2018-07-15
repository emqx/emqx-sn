PROJECT = emqx_sn
PROJECT_DESCRIPTION = EMQ X MQTT-SN Gateway
PROJECT_VERSION = 3.0

DEPS = esockd clique
dep_esockd = git https://github.com/emqtt/esockd emqx30
dep_clique = git https://github.com/emqtt/clique

BUILD_DEPS = emqx cuttlefish
dep_emqx = git git@github.com:emqtt/emqttd emqx30
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

ERLC_OPTS += +debug_info

TEST_DEPS = meck emqx_ct_helpers
dep_meck = https://github.com/eproxus/meck.git
dep_emqx_ct_helpers = git https://github.com/emqx/emqx-ct-helpers

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, emqx_ct_transform}'

COVER = true

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emqx_sn.conf -i priv/emqx_sn.schema -d data
