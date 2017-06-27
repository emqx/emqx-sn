
.PHONY: clean, clean_result, start_broker  stop_broker  case1

RELX_CONF = emq-relx/relx.config
PAHO_GIT  = paho.mqtt-sn.embedded-c/README.md

PAHO_SRC  = paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNConnectClient.c        \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNConnectServer.c        \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNDeserializePublish.c   \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNPacket.c               \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSearchClient.c         \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSearchServer.c         \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSerializePublish.c     \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSubscribeClient.c      \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSubscribeServer.c      \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNUnsubscribeClient.c    \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNUnsubscribeServer.c    \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/samples/transport.c              \
			client/int_test_result.c

INCLUDE_PATH = -Ipaho.mqtt-sn.embedded-c/MQTTSNPacket/src -Ipaho.mqtt-sn.embedded-c/MQTTSNPacket/samples -Iclient
			

all:  clean_result $(RELX_CONF) $(PAHO_GIT) start_broker case1 case2 case3 case4 case5 stop_broker
	@echo "  "
	@echo "  test complete"
	@echo "  "
	
clean_result:
	-rm -f client/*_FAIL.txt
	-rm -f client/*_PASS.txt
	
start_broker:
	-emq-relx/_rel/emqttd/bin/emqttd stop
	sleep 3
	emq-relx/_rel/emqttd/bin/emqttd start
	sleep 5
	emq-relx/_rel/emqttd/bin/emqttd_ctl plugins load emq_sn
	
stop_broker:
	emq-relx/_rel/emqttd/bin/emqttd stop
	

case1:
	-ps aux|grep case1_qos0pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case1_qos0sub|awk '{print $$2}'|xargs kill -9
	gcc client/case1_qos0pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case1_qos0pub.exe
	gcc client/case1_qos0sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case1_qos0sub.exe
	client/case1_qos0sub.exe &
	sleep 1
	client/case1_qos0pub.exe

case2:
	-ps aux|grep case2_qos0pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case2_qos0sub|awk '{print $$2}'|xargs kill -9
	gcc client/case2_qos0pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case2_qos0pub.exe
	gcc client/case2_qos0sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case2_qos0sub.exe
	client/case2_qos0sub.exe &
	sleep 1
	client/case2_qos0pub.exe

case3:
	-ps aux|grep case3_qos0pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case3_qos0sub|awk '{print $$2}'|xargs kill -9
	gcc client/case3_qos0pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case3_qos0pub.exe
	gcc client/case3_qos0sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case3_qos0sub.exe
	client/case3_qos0sub.exe &
	sleep 1
	client/case3_qos0pub.exe

case4:
	-ps aux|grep case4_qos3pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case4_qos3sub|awk '{print $$2}'|xargs kill -9
	gcc client/case4_qos3pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case4_qos3pub.exe
	gcc client/case4_qos3sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case4_qos3sub.exe
	client/case4_qos3sub.exe &
	sleep 1
	client/case4_qos3pub.exe

case5:
	-ps aux|grep case5_qos3pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case5_qos3sub|awk '{print $$2}'|xargs kill -9
	gcc client/case5_qos3pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case5_qos3pub.exe
	gcc client/case5_qos3sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case5_qos3sub.exe
	client/case5_qos3sub.exe &
	sleep 1
	client/case5_qos3pub.exe
	
	
	
	
$(RELX_CONF):
	git clone https://github.com/emqtt/emq-relx.git
	git clone https://github.com/emqtt/emq-sn.git
	@echo "update emq-sn with this development code"
	mv emq-sn  emq_sn
	-rm -rf emq_sn/etc
	-rm -rf emq_sn/include
	-rm -rf emq_sn/priv
	-rm -rf emq_sn/src
	-rm -rf emq_sn/Makefile
	cp -rf ../etc      emq_sn/
	cp -rf ../include  emq_sn/
	cp -rf ../priv     emq_sn/
	cp -rf ../src      emq_sn/
	cp -rf ../Makefile emq_sn/Makefile
	-mkdir emq-relx/deps
	mv emq_sn  emq-relx/deps/
	@echo "start building ..."
	make -C emq-relx -f Makefile
	

$(PAHO_GIT):
	git clone https://github.com/eclipse/paho.mqtt-sn.embedded-c/
	
r: rebuild_emq
	# short for rebuild_emq
	@echo "rebuild complete"
	
rebuild_emq:
	-rm -rf emq-relx/deps/emq_sn/etc
	-rm -rf emq-relx/deps/emq_sn/include
	-rm -rf emq-relx/deps/emq_sn/priv
	-rm -rf emq-relx/deps/emq_sn/src
	-rm -rf emq-relx/deps/emq_sn/Makefile
	cp -rf ../etc      emq-relx/deps/emq_sn/
	cp -rf ../include  emq-relx/deps/emq_sn/
	cp -rf ../priv     emq-relx/deps/emq_sn/
	cp -rf ../src      emq-relx/deps/emq_sn/
	cp -rf ../Makefile emq-relx/deps/emq_sn/Makefile
	make -C emq-relx -f Makefile
	
	
clean: clean_result
	-rm -f client/*.exe
	-rm -f client/*.o
	-rm -rf emq-relx
	-rm -rf paho.mqtt-sn.embedded-c
	
	
