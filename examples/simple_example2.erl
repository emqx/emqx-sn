-module(simple_example2).

-include("emq_sn.hrl").

-define(HOST, "localhost").
-define(PORT, 1884).

-export([start/0]).

start() ->
    io:format("start to connect ~p:~p~n", [?HOST, ?PORT]),

    %% create udp socket
    {ok, Socket} = gen_udp:open(0, [binary]),

    %% connect to emqttd_sn broker
    Package = gen_connect_package(<<"client1">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Package),
    io:format("send connect package=~p~n", [Package]),
    %% receive message
    wait_response(),

    %% subscribe
    SubscribePackage = gen_subscribe_package(<<"T1">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePackage),
    io:format("send subscribe package=~p~n", [SubscribePackage]),
    wait_response(),

    %% publish
    PublishPackage = gen_publish_package(<<"T1">>, <<"Payload...">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPackage),
    io:format("send publish package=~p~n", [PublishPackage]),
    wait_response(),

    % wait for subscribed message from broker
    wait_response(),

    %% disconnect from emqttd_sn broker
    DisConnectPackage = gen_disconnect_package(),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, DisConnectPackage),
    io:format("send disconnect package=~p~n", [DisConnectPackage]).



gen_connect_package(ClientId) ->
    Length = 6+byte_size(ClientId),
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
    <<Length:8, MsgType:8, Flag/binary, ProtocolId:8, Duration:16, ClientId/binary>>.

gen_subscribe_package(ShortTopic) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    Qos = 1,
    CleanSession = 0,
    TopicIdType = 2,
    Flag = <<Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    MsgId = 1,
    <<Length:8, MsgType:8, Flag/binary, MsgId:16, ShortTopic/binary>>.

gen_register_package(Topic, TopicId) ->
    Length = 6+byte_size(Topic),
    MsgType = ?SN_REGISTER,
    MsgId = 1,
    <<Length:8, MsgType:8, TopicId:16, MsgId:16, Topic/binary>>.

gen_publish_package(ShortTopic, Payload) ->
    Length = 7+byte_size(Payload),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    Qos = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicIdType = 2,
    Flag = <<Dup:1, Qos:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    <<Length:8, MsgType:8, Flag/binary, ShortTopic/binary, MsgId:16, Payload/binary>>.

gen_disconnect_package()->
    Length = 2,
    MsgType = ?SN_DISCONNECT,
    <<Length:8, MsgType:8>>.

wait_response() ->
    receive
        {udp, _Socket, _, _, Bin} ->
            case Bin of
                <<_Len:8, ?SN_PUBLISH, _Flag:8, TopicId:16, MsgId:16, Data/binary>> ->
                    io:format("recv publish TopicId: ~p, MsgId: ~p, Data: ~p~n", [TopicId, MsgId, Data]);
                <<_Len:8, ?SN_CONNACK, 0:8>> ->
                    io:format("recv connect ack~n");
                <<_Len:8, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv regack TopicId=~p, MsgId=~p~n", [TopicId, MsgId]),
                    TopicId;
                <<_Len:8, ?SN_SUBACK, Flags:8, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv suback Flags=~p TopicId=~p, MsgId=~p~n", [Flags, TopicId, MsgId]);
                <<_Len:8, ?SN_PUBACK, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv puback TopicId=~p, MsgId=~p~n", [TopicId, MsgId]);
                _ ->
                    io:format("ignore bin=~p~n", [Bin])
            end;
        Any ->
            io:format("recv something else from udp socket ~p~n", [Any])
    after
        2000 ->
            io:format("Error: receive timeout!~n"),
            wait_response()
    end.
