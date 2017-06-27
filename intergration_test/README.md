Integration test for emq-sn
======

## execute following command
```
make
```

## note

The case4 and case5 are about processing publish message with qos=-1, which needs the mqtt.sn.enable_qos3 in the emq_sn.conf to set to on, otherwise these two cases may not get correct return value.

