PROJECT = emqttd_sn
PROJECT_DESCRIPTION = MQTT-SN Gateway for The EMQTT Broker
PROJECT_VERSION = 0.2

DEPS = esockd emqttd

dep_esockd = git https://github.com/emqtt/esockd.git udp
dep_emqttd = git https://github.com/emqtt/emqttd.git emq20

ERLC_OPTS += +'{parse_transform, lager_transform}'

include erlang.mk
