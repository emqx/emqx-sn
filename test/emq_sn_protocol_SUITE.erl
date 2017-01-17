-module (emq_sn_protocol_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emq_sn.hrl").
-include_lib("emqttd/include/emqttd_protocol.hrl").
-compile(export_all).
-define(HOST, "localhost").
-define(PORT, 1884).

all() -> [].

subscribe() ->
    subscribe(0).
subscribe(Qos) ->
    Fun = fun(Socket) ->
        send_register_msg(Socket),
        send_subscribe_msg(Socket, Qos),
        send_disconnect_msg(Socket)
    end,
    send_connect_msg(Fun).


publish()->
    publish(0).

publish(Qos) when Qos =:= 0->
    publish(0, 0);

publish(Qos) ->
    publish(Qos, erlang:round(random:uniform(50000))+1).

publish(Qos, MsgId) ->
    Fun = fun(Socket) ->
        send_register_msg(Socket),
        send_subscribe_msg(Socket, Qos),
        send_publish_msg(Socket, Qos, MsgId),
        send_unsubscribe_msg(Socket),
        send_pingreq_msg(Socket),
        send_disconnect_msg(Socket)
    end,
    send_connect_msg(Fun).

publish_for_wait_will() ->
    Fun = fun(Socket) ->
        send_register_msg(Socket),
        send_subscribe_msg(Socket, 0),
        send_publish_msg(Socket, 0),
        send_unsubscribe_msg(Socket),
        send_pingreq_msg(Socket)
    end,
    send_connect_msg_for_wait_will(Fun).

send_searchgw_msg() ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    Length = 3,
    MsgType = ?SN_CONNECT,
    Radius = 0,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, <<Length:8, MsgType:8, Radius:8>>),
    receive
        {udp, Socket, _, _, Bin} ->
            io:format("client received:~p~n", [Bin])
        after 2000 ->
            gen_udp:close(Socket)
        end.    

send_connect_msg(Fun) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    Length = 10,
    MsgType = ?SN_CONNECT,
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 1,
    TopicIdType = 0,
    ProtocolId = 1,
    Duration = 10,
    ClientId = <<"test">>,
    Packet = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2, ProtocolId:8, Duration:16, ClientId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Packet),
    lookup(Socket, Fun).    

send_connect_msg_for_wait_will(Fun) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    Length = 10,
    MsgType = ?SN_CONNECT,
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 1,
    CleanSession = 1,
    TopicIdType = 0,
    ProtocolId = 1,
    Duration = 10,
    ClientId = <<"test">>,
    ConnectPacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2, ProtocolId:8, Duration:16, ClientId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, ConnectPacket),
    lookup(Socket, Fun). 

send_willtopic_msg(Socket) ->
    Length = 7,
    MsgType = ?SN_WILLTOPIC,
    Dup = 0,
    Qos = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 0,
    WillTopic = <<"will">>,
    WillTopicPacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2, WillTopic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillTopicPacket).    

send_willmsg_msg(Socket) ->
    Length = 9,
    MsgType = ?SN_WILLMSG,
    WillMsg = <<"willmsg">>,
    WillMsgPacket = <<Length:8, MsgType:8, WillMsg/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, WillMsgPacket).

send_register_msg(Socket) ->
    Length = 15,
    MsgType = ?SN_REGISTER,
    TopicId = 1,
    MsgId = 1,
    Topic = <<"testtopic">>,
    RegisterPacket = <<Length:8, MsgType:8, TopicId:16, MsgId:16, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, RegisterPacket).

send_publish_msg(Socket, Qos) ->
    send_publish_msg(Socket, Qos, 0).

send_publish_msg(Socket, Qos, MsgId) ->
    Length = 16,
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 0,
    TopicId = 1,
    Data = <<"testtopic">>,
    PublishPacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2,TopicId:16, MsgId:16, Data/binary>>,
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

send_subscribe_msg(Socket, Qos) ->
    Length = 14,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 0,
    MsgId = 1,
    TopicId = <<"testtopic">>,
    SubscribePacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
            CleanSession:1, TopicIdType:2, MsgId:16, TopicId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket).    

send_unsubscribe_msg(Socket) ->
    Length = 13,
    MsgType = ?SN_UNSUBSCRIBE,
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 0,
    MsgId = 1,
    TopicId = <<"subtopic">>,
    UnSubscribePacket = <<Length:8, MsgType:8, Dup:1, Qos:2, Retain:1, Will:1, 
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

lookup(Socket, Fun) ->
    receive
        {udp, Socket, _, _, Bin} ->
            %%io:format("client received:~p~n", [Bin]),
            case Bin of
                <<3, ?SN_GWINFO, GwId:8, GwAdd/binary>> ->
                    io:format("recv gwinfo GwId: ~p, GwAdd: ~p~n", [GwId, GwAdd]);
                <<3, ?SN_CONNACK, 0>> ->
                    io:format("recv connack~n"),
                    Fun(Socket);
                <<2, ?SN_WILLTOPICREQ>> ->
                    io:format("wait for will topic~n"),
                    send_willtopic_msg(Socket);
                <<2, ?SN_WILLMSGREQ>> ->
                    io:format("wait for will msg~n"),
                    send_willmsg_msg(Socket);
                <<7, ?SN_REGACK, TopicId:16, MsgId:16, ReturnCode:8>> ->
                    io:format("recv regack TopicId: ~p, MsgId: ~p, ReturnCode: ~p~n", [TopicId, MsgId, ReturnCode]);
                <<_Len:8, ?SN_PUBLISH, _:1, Qos:2, _:1, _:1, _:1, _:2, TopicId:16, MsgId:16, Data/binary>> ->
                    case Qos of
                        0 -> ok;
                        1 -> send_puback_msg(Socket, TopicId, MsgId);
                        2 -> send_pubrec_msg(Socket, MsgId)
                    end,
                    io:format("recv publish Qos: ~p, TopicId: ~p, MsgId: ~p, Data: ~p~n", [Qos, TopicId, MsgId, Data]);
                <<7, ?SN_PUBACK, TopicId:16, MsgId:16, ReturnCode:8>> ->
                    io:format("recv puback TopicId: ~p, MsgId: ~p, ReturnCode: ~p~n", [TopicId, MsgId, ReturnCode]);
                <<4, ?SN_PUBCOMP, MsgId:16>> ->
                    io:format("recv pubcomp MsgId: ~p~n", [MsgId]);
                <<4, ?SN_PUBREC, MsgId:16>> ->
                    io:format("recv pubrec MsgId: ~p~n", [MsgId]),
                    send_pubrel_msg(Socket, MsgId);
                <<4, ?SN_PUBREL, MsgId:16>> ->
                    io:format("recv pubrel MsgId: ~p~n", [MsgId]),
                    send_pubcomp_msg(Socket, MsgId);
                <<8, ?SN_SUBACK, Flags:8, TopicId:16, MsgId:16, ReturnCode:8>> ->
                    io:format("recv suback Flags: ~p, TopicId: ~p, MsgId: ~p, ReturnCode: ~p~n", [Flags, TopicId, MsgId, ReturnCode]);
                <<4, ?SN_UNSUBACK, MsgId:16>> ->
                    io:format("recv unsuback MsgId: ~p~n", [MsgId]);
                <<2, ?SN_PINGRESP>> ->
                    io:format("recv pingresp ~n");
                _ ->
                    ok
            end,
            lookup(Socket, Fun)
        after 15000 ->
            io:format("Socket closed~n"),
            gen_udp:close(Socket)
        end.
