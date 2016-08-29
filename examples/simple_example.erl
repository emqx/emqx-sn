-module(simple_example).

-include("emqttd_sn.hrl").

-define(HOST, "localhost").
-define(PORT, 1884).

-export([start/0]).

start() ->
    %% create udp socket
    {ok, Socket} = gen_udp:open(0, [binary]),

    %% connect to emqttd_sn broker
    Package = gen_connect_package(ClientId),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Package),

    %% register topic_id
    RegisterPackage = gen_register_package(<<"TopicA">>, 1),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, RegisterPackage),

    %% subscribe
    SubscribePackage = gen_subscribe_package(1),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePackage),

    %% publish
    PublishPackage = gen_publish_package(1, <<"Payload...">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPackage),

    %% receive message
    receive
        {udp, Socket, _, _, Bin} ->
            case Bin of
                <<_Len:8, ?SN_PUBLISH, _:1, _Flag:8, TopicId:16, MsgId:16, Data/binary>> ->
                    io:format("recv publish Qos: ~p, TopicId: ~p, MsgId: ~p, Data: ~p~n", [Qos, TopicId, MsgId, Data]);
                _ ->
                    ok
            end
    after
        1000 ->
            io:format("Error: receive timeout!~n")
    end,
    
    %% disconnect from emqttd_sn broker
    DisConnectPackage = gen_disconnect_package(),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, DisConnectPackage).



gen_connect_package(ClientId) ->
    Length = 10,
    MsgType = ?SN_CONNECT,
    Dup = 0,
    Qos = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 1,
    TopicIdType = 0,
    Flag = <<Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    ProtocolId = 1,
    Duration = 10,
    <<Length:8, MsgType:8, , ProtocolId:8, Duration:16, ClientId/binary>>.

gen_subscribe_package(TopicId) ->
    Length = 14,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 1,
    Flag = <<Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    MsgId = 1,
     = <<Length:8, MsgType:8, MsgId:16, TopicId:16>>,
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePackage). 

gen_register_package(Topic, TopicId) ->
    Length = 15,
    MsgType = ?SN_REGISTER,
    MsgId = 1,
    <<Length:8, MsgType:8, TopicId:16, MsgId:16, Topic/binary>>.

gen_publish_package(TopicId, Payload) ->
    Length = 16,
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = 0,
    Flag = <<Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    <<Length:8, MsgType:8,TopicId:16, MsgId:16, Payload/binary>>.

gen_disconnect_package()->
    Length = 2,
    MsgType = ?SN_DISCONNECT,
    <<Length:8, MsgType:8>>.