PROJECT = emqttd_sn
PROJECT_DESCRIPTION = Erlang MQTT-SN Gateway
PROJECT_VERSION = 0.2

DEPS = gen_conf esockd emqttd

dep_gen_conf = git https://github.com/emqtt/gen_conf.git master
dep_esockd   = git https://github.com/emqtt/esockd.git udp
dep_emqttd   = git https://github.com/emqtt/emqttd.git emq20

ERLC_OPTS += +'{parse_transform, lager_transform}'

include erlang.mk
