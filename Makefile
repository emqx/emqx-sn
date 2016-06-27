PROJECT = emqtt_sn
PROJECT_DESCRIPTION = Erlang MQTT-SN Gateway
PROJECT_VERSION = 0.0.1

DEPS = lager esockd emqttd

dep_esockd = git https://github.com/emqtt/esockd.git udp
dep_emqttd = git https://github.com/emqtt/emqttd.git plus

include erlang.mk
