-module (emq_sn_protocol_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emq_sn.hrl").
-include_lib("emqttd/include/emqttd_protocol.hrl").
-compile(export_all).
-define(HOST, "localhost").
-define(PORT, 1884).

-define(FLAG_DUP(X),X).
-define(FLAG_QOS(X),X).
-define(FLAG_RETAIN(X),X).
-define(FLAG_SESSION(X),X).

% FLAG NOT USED
-define(FNU, 0).


all() -> [subscribe_test, subscribe_test1, subscribe_test2,
    subscribe_test10, subscribe_test11, subscribe_test12, subscribe_test13,
    publish_qos0_test1, publish_qos0_test2, publish_qos0_test3,
    publish_qos1_test1, publish_qos1_test2, publish_qos1_test3, publish_qos1_test4, publish_qos1_test5,
    publish_qos2_test1, publish_qos2_test2,
    will_test1, will_test2, will_test3, will_test4, will_test5,
    broadcast_test1].


init_per_suite(Config) ->
    prepare_config(),
    application:set_env(emq_sn, advertise_duration, 2),
    ?assertMatch({ok, _}, application:ensure_all_started(emqttd)),
    ?assertMatch({ok, _}, application:ensure_all_started(emq_sn)),
    ?assertEqual(ok, application:ensure_started(lager)),
    lager_common_test_backend:bounce(debug),
    Config.

end_per_suite(_Config) ->
    application:stop(lager),
    application:stop(emq_sn),
    application:stop(emqttd).



subscribe_test(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = 1,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_register_msg(Socket, <<"abcD">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_predefined_topic(Socket, Qos, TopicId, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_unsubscribe_msg_predefined_topic(Socket, TopicId, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


subscribe_test1(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = 1,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_register_msg(Socket, <<"abc">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_predefined_topic(Socket, Qos, TopicId, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_unsubscribe_msg_normal_topic(Socket, <<"abc">>, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


subscribe_test2(_Config) ->
    Dup = 0,
    Qos = 2,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId = 0,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"ClientA">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_short_topic(Socket, Qos, <<"te">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_unsubscribe_msg_short_topic(Socket, <<"te">>, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).




subscribe_test10(_Config) ->
    Dup = 0,
    Qos = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 25,
    TopicId0 = 0,
    TopicId1 = 1,
    TopicId2 = 2,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"testu">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_register_msg(Socket, <<"abcD">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId1:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"abcD">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId1:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"/sport/#">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId0:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"/a/+/water">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId0:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"/Tom/Home">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId2:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_unsubscribe_msg_normal_topic(Socket, <<"abcD">>, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


subscribe_test11(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId0 = 0,
    TopicId1 = 1,
    TopicId2 = 2,
    ReturnCode = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_register_msg(Socket, <<"abc">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId1:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_register_msg(Socket, <<"/blue/#">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId0:16, MsgId:16, ?SN_RC_NOT_SUPPORTED:8>>, receive_response(Socket)),
    send_register_msg(Socket, <<"/blue/+/white">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId0:16, MsgId:16, ?SN_RC_NOT_SUPPORTED:8>>, receive_response(Socket)),
    send_register_msg(Socket, <<"/$sys/rain">>, MsgId),
    ?assertEqual(<<7, ?SN_REGACK, TopicId2:16, MsgId:16, 0:8>>, receive_response(Socket)),
    send_subscribe_msg_short_topic(Socket, Qos, <<"Q2">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId0:16, MsgId:16, ReturnCode>>,
        receive_response(Socket)),
    send_unsubscribe_msg_normal_topic(Socket, <<"Q2">>, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


subscribe_test12(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = 1,
    TopicId2 = 2,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_predefined_topic(Socket, Qos, TopicId1, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId1:16, MsgId:16, ?SN_RC_INVALID_TOPIC_ID>>,
        receive_response(Socket)),
    send_unsubscribe_msg_predefined_topic(Socket, TopicId2, MsgId),
    ?assertEqual(<<4, ?SN_UNSUBACK, MsgId:16>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).




subscribe_test13(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId2 = 2,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_reserved_topic(Socket, Qos, TopicId2, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, ?SN_INVALID_TOPIC_ID:16, MsgId:16, ?SN_RC_INVALID_TOPIC_ID>>,
        receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


publish_qos0_test1(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"abc">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId1:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_predefined_topic(Socket, Qos, MsgId, TopicId1, <<20, 21, 22, 23>>),
    Eexp = <<11, ?SN_PUBLISH, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_PREDEFINED_TOPIC:2, TopicId1:16, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    io:format("What=~p~n", [What]),
    ?assertEqual(Eexp, What),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


publish_qos0_test2(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId0 = 0,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"#">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId0:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_short_topic(Socket, Qos, MsgId, <<"TR">>, <<20, 21, 22, 23>>),
    Eexp = <<11, ?SN_PUBLISH, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_SHORT_TOPIC:2, (<<"TR">>)/binary, (mid(0)):16, <<20, 21, 22, 23>>/binary>>,
    What = receive_response(Socket),
    io:format("What=~p~n", [What]),
    ?assertEqual(Eexp, What),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


publish_qos0_test3(_Config) ->
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId0 = 0,
    MsgId = 1,
    TopicId0 = 0,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_short_topic(Socket, Qos, <<"/#">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId0:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),

    {ok, C} = emqttc:start_link([{host, "localhost"}, {client_id, <<"simpleClient">>}]),
    emqttc:publish(C, <<"/abcd">>, <<"12345">>),

    Data20 = receive_response(Socket),
    io:format("Data20=~p~n", [Data20]),
    <<11, ?SN_REGISTER, TopicId1:16, Rest20/binary>> = Data20,
    <<MId:16, Rest21/binary>> = Rest20,
    <<"/abcd">> = Rest21,

    send_regack_msg(Socket, TopicId1, MsgId0),
    ?assertEqual(<<12, ?SN_PUBLISH, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_PREDEFINED_TOPIC:2, TopicId1:16, MsgId0:16, <<"12345">>/binary>>,
        receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),

    emqttc:disconnect(C),
    gen_udp:close(Socket).



publish_qos1_test1(_Config) ->
    Dup = 0,
    Qos = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"abc">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId1:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_predefined_topic(Socket, Qos, MsgId, TopicId1, <<20, 21, 22, 23>>),
    ?assertEqual(<<7, ?SN_PUBACK, TopicId1:16, MsgId:16, ?SN_RC_ACCECPTED>>, receive_response(Socket)),
    check_dispatched_message(Dup, Qos, Retain, ?SN_PREDEFINED_TOPIC, TopicId1, <<20, 21, 22, 23>>, Socket),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


publish_qos1_test2(_Config) ->
    Qos = 1,
    MsgId = 1,
    TopicId5 = 5,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),

    send_publish_msg_predefined_topic(Socket, Qos, MsgId, tid(5), <<20, 21, 22, 23>>),
    ?assertEqual(<<7, ?SN_PUBACK, TopicId5:16, MsgId:16, ?SN_RC_INVALID_TOPIC_ID>>, receive_response(Socket)),

    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

publish_qos1_test3(_Config) ->
    Dup = 0,
    Qos = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId0 = 0,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_short_topic(Socket, Qos, <<"ab">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId0:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_short_topic(Socket, Qos, MsgId, <<"ab">>, <<20, 21, 22, 23>>),
    <<TopicIdShort:16>> = <<"ab">>,
    ?assertEqual(<<7, ?SN_PUBACK, TopicIdShort:16, MsgId:16, ?SN_RC_ACCECPTED>>, receive_response(Socket)),
    check_dispatched_message(Dup, Qos, Retain, ?SN_SHORT_TOPIC, TopicIdShort, <<20, 21, 22, 23>>, Socket),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

publish_qos1_test4(_Config) ->
    Dup = 0,
    Qos = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"ab">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId1:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_short_topic(Socket, Qos, MsgId, <<"/#">>, <<20, 21, 22, 23>>),
    <<TopicIdShort:16>> = <<"/#">>,
    ?assertEqual(<<7, ?SN_PUBACK, TopicIdShort:16, MsgId:16, ?SN_RC_NOT_SUPPORTED>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).

publish_qos1_test5(_Config) ->
    Dup = 0,
    Qos = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 7,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"ab">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, ?SN_NORMAL_TOPIC:2, TopicId1:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_short_topic(Socket, Qos, MsgId, <<"/+">>, <<20, 21, 22, 23>>),
    <<TopicIdShort:16>> = <<"/+">>,
    ?assertEqual(<<7, ?SN_PUBACK, TopicIdShort:16, MsgId:16, ?SN_RC_NOT_SUPPORTED>>, receive_response(Socket)),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


publish_qos2_test1(_Config) ->
    Qos = 2,
    MsgId = 7,
    TopicId1 = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"/abc">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, ?FNU:1, Qos:2, ?FNU:5, TopicId1:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_predefined_topic(Socket, Qos, MsgId, TopicId1, <<20, 21, 22, 23>>),
    ?assertEqual(<<4, ?SN_PUBREC, MsgId:16>>, receive_response(Socket)),
    send_pubrel_msg(Socket, MsgId),
    ?assertEqual(<<4, ?SN_PUBCOMP, MsgId:16>>, receive_response(Socket)),
    check_dispatched_message(0, Qos, 0, ?SN_PREDEFINED_TOPIC, TopicId1, <<20, 21, 22, 23>>, Socket),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


publish_qos2_test2(_Config) ->
    Qos = 2,
    MsgId = 7,
    TopicId0 = 0,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg(Socket, <<"test">>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_subscribe_msg_normal_topic(Socket, Qos, <<"/#">>, MsgId),
    ?assertEqual(<<8, ?SN_SUBACK, ?FNU:1, Qos:2, ?FNU:5, TopicId0:16, MsgId:16, ?SN_RC_ACCECPTED>>,
        receive_response(Socket)),
    send_publish_msg_short_topic(Socket, Qos, MsgId, <<"/a">>, <<20, 21, 22, 23>>),
    ?assertEqual(<<4, ?SN_PUBREC, MsgId:16>>, receive_response(Socket)),
    send_pubrel_msg(Socket, MsgId),
    ?assertEqual(<<4, ?SN_PUBCOMP, MsgId:16>>, receive_response(Socket)),
    <<TopicIdShort:16>> = <<"/a">>,
    check_dispatched_message(0, Qos, 0, ?SN_SHORT_TOPIC, TopicIdShort, <<20, 21, 22, 23>>, Socket),
    send_disconnect_msg(Socket),
    ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket)),
    gen_udp:close(Socket).


will_test1(_Config) ->
    Qos = 1,
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg_with_will(Socket, Duration),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, <<"abc">>, Qos),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, <<10, 11, 12, 13, 14>>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    {ok, C} = emqttc:start_link([{host, "localhost"}, {client_id, <<"simpleClient">>}]),
    ?assertEqual(connected, receive_emqttc_response()),
    emqttc:subscribe(C, <<"abc">>, ?QOS1),
    timer:sleep(10000),

    receive_response(Socket), % ignore PUBACK
    ?assertEqual({publish, <<"abc">>, <<10, 11, 12, 13, 14>>}, receive_emqttc_response()),

    send_disconnect_msg(Socket),
    ?assertEqual(udp_receive_timeout, receive_response(Socket)),
    emqttc:disconnect(C),
    gen_udp:close(Socket).



will_test2(_Config) ->
    Qos = 2,
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg_with_will(Socket, Duration),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, <<"goodbye">>, Qos),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, <<10, 11, 12, 13, 14>>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    {ok, C} = emqttc:start_link([{host, "localhost"}, {client_id, <<"simpleClient">>}]),
    ?assertEqual(connected, receive_emqttc_response()),
    emqttc:subscribe(C, <<"goodbye">>, ?QOS1),
    timer:sleep(10000),

    receive_response(Socket), % ignore PUBACK
    receive_response(Socket), % ignore PUBCOMP
    ?assertEqual({publish, <<"goodbye">>, <<10, 11, 12, 13, 14>>}, receive_emqttc_response()),

    send_disconnect_msg(Socket),
    ?assertEqual(udp_receive_timeout, receive_response(Socket)),
    emqttc:disconnect(C),
    gen_udp:close(Socket).



will_test3(_Config) ->
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg_with_will(Socket, Duration),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_empty_msg(Socket),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),

    {ok, C} = emqttc:start_link([{host, "localhost"}, {client_id, <<"simpleClient">>}]),
    ?assertEqual(connected, receive_emqttc_response()),
    emqttc:subscribe(C, <<"goodbye">>, ?QOS1),
    timer:sleep(10000),

    ?assertEqual(udp_receive_timeout, receive_response(Socket)),
    ?assertEqual(emqttc_receive_timeout, receive_emqttc_response()),

    send_disconnect_msg(Socket),
    ?assertEqual(udp_receive_timeout, receive_response(Socket)),
    emqttc:disconnect(C),
    gen_udp:close(Socket).



will_test4(_Config) ->
    Qos = 1,
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg_with_will(Socket, Duration),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, <<"abc">>, Qos),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, <<10, 11, 12, 13, 14>>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),
    send_willtopicupd_msg(Socket, <<"/XYZ">>, ?QOS1),
    ?assertEqual(<<3, ?SN_WILLTOPICRESP, ?SN_RC_ACCECPTED>>, receive_response(Socket)),
    send_willmsgupd_msg(Socket, <<"1A2B3C">>),
    ?assertEqual(<<3, ?SN_WILLMSGRESP, ?SN_RC_ACCECPTED>>, receive_response(Socket)),

    {ok, C} = emqttc:start_link([{host, "localhost"}, {client_id, <<"simpleClient">>}]),
    ?assertEqual(connected, receive_emqttc_response()),
    emqttc:subscribe(C, <<"abc">>, ?QOS1),
    emqttc:subscribe(C, <<"/XYZ">>, ?QOS1),
    timer:sleep(10000),

    receive_response(Socket), % ignore PUBACK
    ?assertEqual({publish, <<"/XYZ">>, <<"1A2B3C">>}, receive_emqttc_response()),

    send_disconnect_msg(Socket),
    ?assertEqual(udp_receive_timeout, receive_response(Socket)),
    emqttc:disconnect(C),
    gen_udp:close(Socket).



will_test5(_Config) ->
    Qos = 1,
    Duration = 1,
    {ok, Socket} = gen_udp:open(0, [binary]),
    send_connect_msg_with_will(Socket, Duration),
    ?assertEqual(<<2, ?SN_WILLTOPICREQ>>, receive_response(Socket)),
    send_willtopic_msg(Socket, <<"abc">>, Qos),
    ?assertEqual(<<2, ?SN_WILLMSGREQ>>, receive_response(Socket)),
    send_willmsg_msg(Socket, <<10, 11, 12, 13, 14>>),
    ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket)),
    send_pingreq_msg(Socket),
    ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket)),
    send_willtopicupd_empty_msg(Socket),
    ?assertEqual(<<3, ?SN_WILLTOPICRESP, ?SN_RC_ACCECPTED>>, receive_response(Socket)),


    {ok, C} = emqttc:start_link([{host, "localhost"}, {client_id, <<"simpleClient">>}]),
    ?assertEqual(connected, receive_emqttc_response()),
    emqttc:subscribe(C, <<"abc">>, ?QOS1),
    emqttc:subscribe(C, <<"/XYZ">>, ?QOS1),
    timer:sleep(10000),

    ?assertEqual(udp_receive_timeout, receive_response(Socket)),
    ?assertEqual(emqttc_receive_timeout, receive_emqttc_response()),

    send_disconnect_msg(Socket),
    ?assertEqual(udp_receive_timeout, receive_response(Socket)),
    emqttc:disconnect(C),
    gen_udp:close(Socket).

broadcast_test2(_Config) ->
    timer:sleep(15000).

broadcast_test1(_Config) ->
    {ok, Socket} = gen_udp:open( 0, [binary]),
    send_searchgw_msg(Socket),
    ?assertEqual(<<3, ?SN_GWINFO, 1>>, receive_response(Socket)),
    timer:sleep(600),
    gen_udp:close(Socket).



send_searchgw_msg(Socket) ->
    Length = 3,
    MsgType = ?SN_SEARCHGW,
    Radius = 0,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, <<Length:8, MsgType:8, Radius:8>>).


send_connect_msg(Socket, ClientId) ->
    Length = 6 + byte_size(ClientId),
    MsgType = ?SN_CONNECT,
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 1,
    TopicIdType = 0,
    ProtocolId = 1,
    Duration = 10,
    Packet = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2, ProtocolId:8, Duration:16, ClientId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Packet).

send_connect_msg_with_will(Socket, Duration) ->
    Length = 10,
    Will = 1,
    CleanSession = 1,
    ProtocolId = 1,
    ClientId = <<"test">>,
    ConnectPacket = <<Length:8, ?SN_CONNECT:8, ?FNU:4, Will:1,
            CleanSession:1, ?FNU:2, ProtocolId:8, Duration:16, ClientId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, ConnectPacket).

send_willtopic_msg(Socket, Topic, Qos) ->
    Length = 3+byte_size(Topic),
    MsgType = ?SN_WILLTOPIC,
    Retain = 0,
    WillTopicPacket = <<Length:8, MsgType:8, ?FNU:1, Qos:2, Retain:1, ?FNU:4, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willtopic_empty_msg(Socket) ->
    Length = 2,
    MsgType = ?SN_WILLTOPIC,
    WillTopicPacket = <<Length:8, MsgType:8>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willmsg_msg(Socket, Msg) ->
    Length = 2+byte_size(Msg),
    WillMsgPacket = <<Length:8, ?SN_WILLMSG:8, Msg/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillMsgPacket).

send_willtopicupd_msg(Socket, Topic, Qos) ->
    Length = 3+byte_size(Topic),
    MsgType = ?SN_WILLTOPICUPD,
    Retain = 0,
    WillTopicPacket = <<Length:8, MsgType:8, ?FNU:1, Qos:2, Retain:1, ?FNU:4, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willtopicupd_empty_msg(Socket) ->
    Length = 2,
    MsgType = ?SN_WILLTOPICUPD,
    WillTopicPacket = <<Length:8, MsgType:8>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_willmsgupd_msg(Socket, Msg) ->
    Length = 2+byte_size(Msg),
    MsgType = ?SN_WILLMSGUPD,
    WillTopicPacket = <<Length:8, MsgType:8, Msg/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).

send_register_msg(Socket, TopicName, MsgId) ->
    Length = 6 + byte_size(TopicName),
    MsgType = ?SN_REGISTER,
    TopicId = 0,
    RegisterPacket = <<Length:8, MsgType:8, TopicId:16, MsgId:16, TopicName/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, RegisterPacket).

send_regack_msg(Socket, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_REGACK,
    Packet = <<Length:8, MsgType:8, TopicId:16, MsgId:16, ?SN_RC_ACCECPTED>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Packet).


send_publish_msg_predefined_topic(Socket, Qos, MsgId, TopicId, Data) ->
    Length = 7 + byte_size(Data),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 1,
    PublishPacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2, TopicId:16, MsgId:16, Data/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPacket).

send_publish_msg_short_topic(Socket, Qos, MsgId, TopicName, Data) ->
    Length = 7 + byte_size(Data),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 2,
    PublishPacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1,
        CleanSession:1, TopicIdType:2, TopicName/binary, MsgId:16, Data/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPacket).


send_puback_msg(Socket, TopicId, MsgId) ->
    Length = 4,
    MsgType = ?SN_PUBACK,
    PubAckPacket = <<Length:8, MsgType:8, TopicId:16, MsgId:16, 0:8>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubAckPacket).

send_pubrec_msg(Socket, MsgId) ->
    Length = 4,
    MsgType = ?SN_PUBREC,
    PubRecPacket = <<Length:8, MsgType:8, MsgId:16>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubRecPacket).

send_pubrel_msg(Socket, MsgId) ->
    Length = 4,
    MsgType = ?SN_PUBREL,
    PubRelPacket = <<Length:8, MsgType:8, MsgId:16>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubRelPacket).

send_pubcomp_msg(Socket, MsgId) ->
    Length = 4,
    MsgType = ?SN_PUBCOMP,
    PubCompPacket = <<Length:8, MsgType:8, MsgId:16>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PubCompPacket).

send_subscribe_msg_normal_topic(Socket, Qos, Topic, MsgId) ->
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 0,
    Length = byte_size(Topic) + 5,
    SubscribePacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2, MsgId:16, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).


send_subscribe_msg_predefined_topic(Socket, Qos, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 1,
    SubscribePacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1,
        CleanSession:1, TopicIdType:2, MsgId:16, TopicId:16>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).


send_subscribe_msg_short_topic(Socket, Qos, Topic, MsgId) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 2,
    SubscribePacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1,
        CleanSession:1, TopicIdType:2, MsgId:16, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).

send_subscribe_msg_reserved_topic(Socket, Qos, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 3,
    SubscribePacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1,
        CleanSession:1, TopicIdType:2, MsgId:16, TopicId:16>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).


send_unsubscribe_msg_predefined_topic(Socket, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_UNSUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 1,
    UnSubscribePacket = <<Length:8, MsgType:8, Dup:1, 0:2, Retain:1, Will:1,
            CleanSession:1, TopicIdType:2, MsgId:16, TopicId:16>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, UnSubscribePacket).

send_unsubscribe_msg_normal_topic(Socket, TopicName, MsgId) ->
    MsgType = ?SN_UNSUBSCRIBE,
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 0,
    Length = 5 + byte_size(TopicName),
    UnSubscribePacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1,
        CleanSession:1, TopicIdType:2, MsgId:16, TopicName/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, UnSubscribePacket).

send_unsubscribe_msg_short_topic(Socket, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_UNSUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 1,
    UnSubscribePacket = <<Length:8, MsgType:8, Dup:1, ?QOS0:2, Retain:1, Will:1,
        CleanSession:1, TopicIdType:2, MsgId:16, TopicId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, UnSubscribePacket).

send_pingreq_msg(Socket)->
    Length = 2,
    MsgType = ?SN_PINGREQ,
    PingReqPacket = <<Length:8, MsgType:8>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PingReqPacket).

send_disconnect_msg(Socket) ->
    Length = 2,
    MsgType = ?SN_DISCONNECT,
    DisConnectPacket = <<Length:8, MsgType:8>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, DisConnectPacket).

mid(Id) -> Id.
tid(Id) -> Id.


receive_response(Socket) ->
    receive
        {udp, Socket, _, _, Bin} ->
            io:format("receive_response Bin=~p~n", [Bin]),
            Bin;
        {mqttc, From, Data2} ->
            io:format("receive_response() ignore mqttc From=~p, Data2=~p~n", [From, Data2]),
            receive_response(Socket);
        Other -> {unexpected_udp_data, Other}
    after 2000 ->
        udp_receive_timeout
    end.


receive_emqttc_response() ->
    receive
        {mqttc, _From, Data2} ->
            Data2;
        {publish, Topic, Payload} ->
            {publish, Topic, Payload};
        Other -> {unexpected_emqttc_data, Other}
    after 2000 ->
        emqttc_receive_timeout
    end.


check_dispatched_message(Dup, Qos, Retain, TopicIdType, TopicId, Payload, Socket) ->
    PubMsg = receive_response(Socket),
    Length = 7 + byte_size(Payload),
    io:format("check_dispatched_message ~p~n", [PubMsg]),
    io:format("expected ~p xx ~p~n", [<<Length, ?SN_PUBLISH, Dup:1, Qos:2, Retain:1, ?FNU:2, TopicIdType:2, TopicId:16>>, Payload]),
    <<Length, ?SN_PUBLISH, Dup:1, Qos:2, Retain:1, ?FNU:2, TopicIdType:2, TopicId:16, MsgId:16, Payload/binary>> = PubMsg,
    case Qos of
        0 -> ok;
        1 -> send_puback_msg(Socket, TopicId, MsgId);
        2 -> send_pubrel_msg(Socket, MsgId),
            ?assertEqual(<<4, ?SN_PUBCOMP, MsgId:16>>, receive_response(Socket))
    end,
    ok.


get_udp_broadcast_address() ->
    %{ok, IfList} = inet:getifaddrs(),
    %{_, LoOpts} = proplists:lookup("lo", IfList),
    %{_, BroadAddress} = proplists:lookup(broadaddr, LoOpts),
    %BroadAddress.
    "255.255.255.255".


prepare_config() ->
    Configs = [{plugins_loaded_file,"{{ platform_data_dir }}/loaded_plugins"},
        {plugins_etc_dir,"{{ platform_etc_dir }}/plugins/"},
        {broker_sys_interval,60},
        {cache_acl,true},
        {acl_file,"{{ platform_etc_dir }}/acl.conf"},
        {allow_anonymous,true},
        {protocol,
            [{max_clientid_len,1024},
                {max_packet_size,65536},
                {client_idle_timeout,30}]},
        {session,
            [{max_inflight,100},
                {retry_interval,60},
                {await_rel_timeout,20},
                {max_awaiting_rel,0},
                {collect_interval,0},
                {expired_after,86400}]},
        {queue,
            [{priority,[]},
                {type,simple},
                {max_length,infinity},
                {low_watermark,0.2},
                {high_watermark,0.6},
                {queue_qos0,true}]},
        {pubsub,[{pool_size,8},{by_clientid,true},{async,true}]},
        {bridge,[{max_queue_len,10000},{ping_down_interval,1}]},
        {listeners,
            [{tcp,1883,
                [{connopts,[]},
                    {sockopts,[{backlog,1024},{nodelay,true}]},
                    {acceptors,8},
                    {max_clients,1024}]},
                {ssl,8883,
                    [{ssl,
                        [{handshake_timeout,15000},
                            {keyfile,"{{ platform_etc_dir }}/certs/key.pem"},
                            {certfile,"{{ platform_etc_dir }}/certs/cert.pem"}]},
                        {connopts,[]},
                        {sockopts,[{nodelay,true}]},
                        {acceptors,4},
                        {max_clients,512}]},
                {http,8083,
                    [{connopts,[]},
                        {sockopts,[{nodelay,true}]},
                        {acceptors,4},
                        {max_clients,64}]},
                {https,8084,
                    [{ssl,
                        [{handshake_timeout,15000},
                            {keyfile,"{{ platform_etc_dir }}/certs/key.pem"},
                            {certfile,"{{ platform_etc_dir }}/certs/cert.pem"}]},
                        {connopts,[]},
                        {sockopts,[{nodelay,true}]},
                        {acceptors,4},
                        {max_clients,64}]}]}],
    [application:set_env(emqttd, Key, Val) || {Key, Val} <- Configs].


