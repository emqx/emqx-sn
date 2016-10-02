PROJECT = emqttd_sn
PROJECT_DESCRIPTION = MQTT-SN Gateway for The EMQTT Broker
PROJECT_VERSION = 0.2.0

DEPS = esockd
dep_esockd = git https://github.com/emqtt/esockd emq20

BUILD_DEPS = emqttd
dep_emqttd = git https://github.com/emqtt/emqttd master

ERLC_OPTS += +'{parse_transform, lager_transform}'

include erlang.mk
