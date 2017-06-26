emq_sn
======

MQTT-SN Gateway for The EMQ Broker

Configure Plugin
----------------

File: etc/emq_sn.conf

```
mqtt.sn.port = 1884
mqtt.sn.advertise_duration = 900
mqtt.sn.gateway_id = 1
mqtt.sn.enable_stats = off
mqtt.sn.predefined.topic.0 = reserved
mqtt.sn.predefined.topic.1 = /predefined/topic/name/hello
mqtt.sn.predefined.topic.2 = /predefined/topic/name/nice
mqtt.sn.username = mqtt_sn_user
mqtt.sn.password = abc
```

- mqtt.sn.port
  * The UDP port which emq-sn is listening on.
- mqtt.sn.advertise_duration
  * The duration(seconds) that emq-sn broadcast ADVERTISE message through.
- mqtt.sn.gateway_id
  * Gateway id in ADVERTISE message.
- mqtt.sn.enable_stats
  * To control whether write statistics data into ETS table for dashbord to read.
- mqtt.sn.predefined.topic.N
  * The pre-defined topic name corresponding to the pre-defined topic id of N. Note that the pre-defined topic id of 0 is reserved.
- mqtt.sn.username
  * This parameter is optional. If specified, emq-sn will connect EMQ core with this username. It is useful if any auth plug-in is enabled.
- mqtt.sn.password
  * This parameter is optional. Pair with username above.


Load Plugin
-----------

```
./bin/emq_ctl plugins load emq_sn
```

## Client

### NOTE
- Topic ID is per-client, and will be cleared if client disconnected with broker or keep-alive failure is detected in broker.
- Please register your topics again each time connected with broker.



### Library

- https://github.com/eclipse/paho.mqtt-sn.embedded-c/
- https://github.com/ty4tw/MQTT-SN
- https://github.com/njh/mqtt-sn-tools
- https://github.com/arobenko/mqtt-sn


sleeping device
-----------

PINGREQ must have a ClientId which is identical to the one in CONNECT message. Without ClientId, emq-sn will ignore such PINGREQ.


pre-defined topics
-----------

The mapping of a pre-defined topic id and topic name should be known inadvance by both client's application and gateway. We define this mapping info in emq_sn.conf file, and which shall be kept equivalent in all client's side.




License
-------

Apache License Version 2.0

Author
------

Feng Lee <feng@emqtt.io>

