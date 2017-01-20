%%--------------------------------------------------------------------
%% Copyright (c) 2016-2017 Feng Lee <feng@emqtt.io>. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emq_sn_gateway).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_fsm).

-include("emq_sn.hrl").

-include_lib("emqttd/include/emqttd_protocol.hrl").

%% API.
-export([start_link/2]).

%% SUB/UNSUB Asynchronously. Called by plugins.
-export([subscribe/2, unsubscribe/2]).

%% gen_fsm.

-export([idle/2, idle/3, wait_for_will_topic/2, wait_for_will_topic/3,
         wait_for_will_msg/2, wait_for_will_msg/3, connected/2, connected/3]).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(state, {gwid, gwinfo = <<>>, sock, peer, protocol, client_id, keepalive, connpkt, queue_pid, awaiting_suback = []}).

-define(LOG(Level, Format, Args, State),
            lager:Level("MQTT-SN(~s): " ++ Format,
                        [esockd_net:format(State#state.peer) | Args])).

-spec(start_link(inet:socket(), {inet:ip_address(), inet:port()}) -> {ok, pid()}).
start_link(Sock, Peer) ->
    gen_fsm:start_link(?MODULE, [Sock, Peer], []).

%% TODO:

subscribe(GwPid, TopicTable) ->
    gen_fsm:send_event(GwPid, {subscribe, TopicTable}).

unsubscribe(GwPid, Topics) ->
    gen_fsm:send_event(GwPid, {unsubscribe, Topics}).

%% TODO:

%% gen_fsm.
init([Sock, Peer]) ->
    put(sn_gw, Peer), %%TODO:
    {ok, QueuePid} = emq_sn_gateway_queue:start_link(),
    State = #state{gwid = 1, sock = Sock, peer = Peer, queue_pid = QueuePid},
    SendFun = fun(Packet) -> send_message(transform(Packet, QueuePid), State) end,
    PktOpts = [{max_clientid_len, 24}, {max_packet_size, 256}],
    ProtoState = emqttd_protocol:init(Peer, SendFun, PktOpts),
    {ok, idle, State#state{protocol = ProtoState}, 3000}.

idle(timeout, StateData) ->
    {stop, idle_timeout, StateData};

idle(?SN_SEARCHGW_MSG(_Radius), StateData = #state{gwid = GwId, gwinfo = GwInfo}) ->
    send_message(?SN_GWINFO_MSG(GwId, GwInfo), StateData),
    {next_state, idle, StateData};

idle(?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId), StateData = #state{protocol = Proto}) ->
    %%TODO:
    #mqtt_sn_flags{will = Will, clean_session = CleanSession} = Flags,
    ConnPkt = #mqtt_packet_connect{client_id  = ClientId,
                                   clean_sess = CleanSession,
                                   keep_alive = Duration},
    case Will of
        true  ->
            send_message(?SN_WILLTOPICREQ_MSG(), StateData#state{connpkt = ConnPkt}),
            {next_state, wait_for_will_topic, StateData#state{connpkt = ConnPkt, client_id = ClientId}};
        false ->
            case emqttd_protocol:received(?CONNECT_PACKET(ConnPkt), Proto) of
                {ok, Proto1}           -> next_state(connected, StateData#state{client_id = ClientId, protocol = Proto1});
                {error, Error}         -> shutdown(Error, StateData);
                {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
                {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
            end
    end;

idle(Event, StateData) ->
    %%TODO:...
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {next_state, idle, StateData}.

wait_for_will_topic(?SN_WILLTOPIC_MSG(Flags, Topic), StateData = #state{connpkt = ConnPkt}) ->
    #mqtt_sn_flags{qos = Qos, retain = Retain} = Flags,
    ConnPkt1 = ConnPkt#mqtt_packet_connect{will_retain = Retain,
                                           will_qos    = Qos,
                                           will_topic  = Topic,
                                           will_flag   = true},
    send_message(?SN_WILLMSGREQ_MSG(), StateData),
    {next_state, wait_for_will_msg, StateData#state{connpkt = ConnPkt1}};

wait_for_will_topic(_Event, StateData) ->
    %%TODO: LOG error
    {next_state, wait_for_will_topic, StateData}.

wait_for_will_msg(?SN_WILLMSG_MSG(Msg), StateData = #state{protocol = Proto, connpkt = ConnPkt}) ->
    %%TODO: protocol connect
    ConnPkt1 = ConnPkt#mqtt_packet_connect{will_msg = Msg},
    case emqttd_protocol:received(?CONNECT_PACKET(ConnPkt1), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

wait_for_will_msg(Event, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {next_state, wait_for_will_msg, StateData}.

connected(?SN_REGISTER_MSG(_TopicId, MsgId, TopicName), StateData = #state{client_id = ClientId}) ->
    case emq_sn_registry:register_topic(ClientId, TopicName) of
        undefined ->
            ?LOG(error, "TopicId is full! ClientId=~p, TopicName=~p", [ClientId, TopicName], StateData),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData);
        wildcard_topic ->
            ?LOG(error, "wildcard topic can not be registered! ClientId=~p, TopicName=~p", [ClientId, TopicName], StateData),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData);
        NewTopicId ->
            ?LOG(debug, "register ClientId=~p, TopicName=~p, NewTopicId=~p", [ClientId, TopicName, NewTopicId], StateData),
            send_message(?SN_REGACK_MSG(NewTopicId, MsgId, ?SN_RC_ACCECPTED), StateData)
    end,
    {next_state, connected, StateData};

connected(?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data), StateData) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    do_publish(TopicIdType, TopicId, Data, Flags, MsgId, StateData);

connected(?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode), StateData = #state{client_id = ClientId, protocol = Proto}) ->
    case ReturnCode of
        ?SN_RC_ACCECPTED ->
            case emqttd_protocol:received(?PUBACK_PACKET(mqttsn_to_mqtt(?PUBACK), MsgId), Proto) of
                {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
                {error, Error}         -> shutdown(Error, StateData);
                {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
                {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
            end;
        ?SN_RC_INVALID_TOPIC_ID ->
            case emq_sn_registry:lookup_topic(ClientId, TopicId) of
                undefined -> ok;
                TopicName ->
                    send_register(TopicName, TopicId, MsgId, StateData),
                    next_state(connected, StateData)
            end;
        _ ->
            ?LOG(error, "CAN NOT handle PUBACK ReturnCode=~p", [ReturnCode], StateData),
            next_state(connected, StateData)
    end;


connected(?SN_PUBREC_MSG(PubRec, MsgId), StateData = #state{protocol = Proto})
    when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    case emqttd_protocol:received(?PUBACK_PACKET(mqttsn_to_mqtt(PubRec), MsgId), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

connected(?SN_SUBSCRIBE_MSG(Flags, MsgId, TopicId), StateData) ->
    #mqtt_sn_flags{qos = Qos, topic_id_type = TopicIdType} = Flags,
    do_subscribe(TopicIdType, TopicId, Qos, MsgId, StateData);

connected(?SN_UNSUBSCRIBE_MSG(Flags, MsgId, TopicId), StateData) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    do_unsubscribe(TopicIdType, TopicId, MsgId, StateData);

connected(?SN_PINGREQ_MSG(_ClientId), StateData) ->
    send_message(?SN_PINGRESP_MSG(), StateData),
    next_state(connected, StateData);

connected(?SN_REGACK_MSG(TopicId, MsgId, ?SN_RC_ACCECPTED), StateData) ->
    next_state(connected, StateData);
connected(?SN_REGACK_MSG(TopicId, MsgId, ReturnCode), StateData) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p", [TopicId, MsgId, ReturnCode], StateData),
    next_state(connected, StateData);

connected(?SN_DISCONNECT_MSG(_Duration), StateData = #state{protocol = Proto}) ->
    {stop, Reason, Proto1} = emqttd_protocol:received(?PACKET(?DISCONNECT), Proto),
    %% TODO: handle duration
    send_message(?SN_DISCONNECT_MSG(undefined), StateData),
    stop(Reason, StateData#state{protocol = Proto1});

% connected(?SN_WILLTOPICUPD_MSG(Flags, Topic), StateData = #state{connpkt = ConnPkt, protocol = Proto}) ->
%     #mqtt_sn_flags{qos = Qos, retain = Retain} = Flags,
%     ConnPkt1 = ConnPkt#mqtt_packet_connect{will_retain = Retain,
%                                            will_qos    = Qos,
%                                            will_topic  = Topic},
%     send_message(?SN_WILLTOPICRESP_MSG(0), StateData),
%     % Proto1 = will_topic_update(ConnPkt1, Proto),
%     {next_state, connected, StateData#state{protocol = Proto}};

% connected(?SN_WILLMSGUPD_MSG(Msg), StateData = #state{connpkt = ConnPkt, protocol = Proto}) ->
%     ConnPkt1 = ConnPkt#mqtt_packet_connect{will_msg = Msg},
%     send_message(?SN_WILLMSGRESP_MSG(0), StateData),
%     % Proto1 = will_msg_update(ConnPkt1, Proto),
%     {next_state, connected, StateData#state{protocol = Proto}};

connected(Event, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {next_state, connected, StateData}.

handle_event(Event, StateName, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {next_state, StateName, StateData}.

idle(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {reply, ignored, idle, StateData}.

wait_for_will_topic(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {reply, ignored, wait_for_will_topic, StateData}.

wait_for_will_msg(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {reply, ignored, wait_for_will_msg, StateData}.

connected(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {reply, ignored, state_name, StateData}.

handle_sync_event(Event, _From, StateName, StateData) ->
    ?LOG(error, "UNEXPECTED SYNC Event: ~p", [Event], StateData),
    {reply, ignored, StateName, StateData}.

handle_info({datagram, _From, Data}, StateName, StateData) ->
     case emq_sn_message:parse(Data) of
        {ok, Msg} ->
            ?LOG(info, "RECV ~p", [format(Msg)], StateData),
            ?MODULE:StateName(Msg, StateData); %% cool?
        format_error ->
            next_state(StateName, StateData)
     end;

%% Asynchronous SUBACK
handle_info({suback, MsgId, [GrantedQos]}, StateName, StateData=#state{awaiting_suback = Awaiting}) ->
    Flags = #mqtt_sn_flags{qos = GrantedQos},
    {MsgId, TopicId} = find_suback_topicid(MsgId, Awaiting),
    ?LOG(debug, "suback Awaiting=~p, MsgId=~p, TopicId=~p", [Awaiting, MsgId, TopicId], StateData),
    send_message(?SN_SUBACK_MSG(Flags, TopicId, MsgId, ?SN_RC_ACCECPTED), StateData),
    next_state(StateName, StateData#state{awaiting_suback = lists:delete({MsgId, TopicId}, Awaiting)});

handle_info({deliver, Msg}, StateName, StateData = #state{client_id = ClientId}) ->
    #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = Qos, retain = Retain},
                  variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId0},
                  payload  = Payload} = emqttd_message:to_packet(Msg),
    MsgId = message_id(MsgId0),
    case emq_sn_registry:lookup_topic_id(ClientId, TopicName) of
        undefined -> 
            case byte_size(TopicName) of
                2 ->
                    <<TransTopicId:16>> = TopicName,
                    send_publish(Dup, Qos, Retain, ?SN_SHORT_TOPIC, TransTopicId, MsgId, Payload, StateData);  % use short topic name
                _ ->
                    register_and_notify_client(TopicName, Payload, Dup, Qos, Retain, MsgId, StateData)
            end;
        TopicId -> 
            send_publish(Dup, Qos, Retain, ?SN_PREDEFINED_TOPIC, TopicId, MsgId, Payload, StateData)   % use pre-defined topic id
    end,
    next_state(StateName, StateData);

handle_info({redeliver, {?PUBREL, MsgId}}, StateName, StateData) ->
    send_message(?SN_PUBREC_MSG(?SN_PUBREL, MsgId), StateData),
    next_state(StateName, StateData);

handle_info({keepalive, start, Interval}, StateName, StateData = #state{sock = Sock}) ->
    ?LOG(debug, "Keepalive at the interval of ~p", [Interval], StateData),
    StatFun = fun() ->
                case inet:getstat(Sock, [recv_oct]) of
                    {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                    {error, Error}              -> {error, Error}
                end
             end,
    KeepAlive = emqttd_keepalive:start(StatFun, Interval, {keepalive, check}),
    next_state(StateName, StateData#state{keepalive = KeepAlive});

handle_info({keepalive, check}, StateName, StateData = #state{keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            next_state(StateName, StateData#state{keepalive = KeepAlive1});
        {error, timeout} ->
            ?LOG(debug, "Keepalive timeout", [], StateData),
            shutdown(keepalive_timeout, StateData);
        {error, Error} ->
            ?LOG(warning, "Keepalive error - ~p", [Error], StateData),
            shutdown(Error, StateData)
    end;

handle_info({'$gen_cast', {subscribe, Topics}}, StateName, StateData) ->
    ?LOG(debug, "ignore subscribe Topics=~p", [Topics], StateData),
    {next_state, StateName, StateData};

handle_info(Info, StateName, StateData) ->
    ?LOG(error, "UNEXPECTED INFO: ~p", [Info], StateData),
    {next_state, StateName, StateData}.

terminate(Reason, _StateName, _StateData = #state{client_id = ClientId, keepalive = KeepAlive, protocol = Proto}) ->
    emq_sn_registry:unregister_topic(ClientId),
    emqttd_keepalive:cancel(KeepAlive),
    case {Proto, Reason} of
        {undefined, _} ->
            ok;
        {_, {shutdown, Error}} ->
            emqttd_protocol:shutdown(Error, Proto);
        {_, Reason} ->
            emqttd_protocol:shutdown(Reason, Proto)
    end.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

transform(?CONNACK_PACKET(0), _QueuePid) ->
    ?SN_CONNACK_MSG(0);

transform(?CONNACK_PACKET(_ReturnCode), _QueuePid) ->
    ?SN_CONNACK_MSG(?SN_RC_CONGESTION);

transform(?PUBACK_PACKET(?PUBACK, MsgId), QueuePid) ->
    {TopicId, MsgId} = emq_sn_gateway_queue:get_puback(QueuePid, MsgId),
    ?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_ACCECPTED);

transform(?PUBACK_PACKET(?PUBREC, MsgId), _QueuePid) ->
    ?SN_PUBREC_MSG(?SN_PUBREC, MsgId);

transform(?PUBACK_PACKET(?PUBREL, MsgId), _QueuePid) ->
    ?SN_PUBREC_MSG(?SN_PUBREL, MsgId);

transform(?PUBACK_PACKET(?PUBCOMP, MsgId), _QueuePid) ->
    ?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId);

transform(?UNSUBACK_PACKET(MsgId), _QueuePid)->
    ?SN_UNSUBACK_MSG(MsgId).

send_publish(Dup, Qos, Retain, TopicIdType, TopicId, MsgId, Payload, StateData) ->
    MsgId1 = case Qos > 0 of
                 true -> MsgId;
                 false -> 0
             end,
    Flags = #mqtt_sn_flags{dup = Dup, qos = Qos, retain = Retain, topic_id_type = TopicIdType},
    Data = ?SN_PUBLISH_MSG(Flags, TopicId, MsgId1, Payload),
    send_message(Data, StateData).

send_register(TopicName, TopicId, MsgId, StateData) ->
    Data = ?SN_REGISTER_MSG(TopicId, MsgId, TopicName),
    send_message(Data, StateData).


send_message(Msg, StateData = #state{sock = Sock, peer = {Host, Port}}) ->
    ?LOG(debug, "SEND ~p~n", [format(Msg)], StateData),
    gen_udp:send(Sock, Host, Port, emq_sn_message:serialize(Msg)).

next_state(StateName, StateData) ->
    {next_state, StateName, StateData, hibernate}.

shutdown(Error, StateData) ->
    {stop, {shutdown, Error}, StateData}.

stop(Reason, StateData) ->
    {stop, Reason, StateData}.

mqttsn_to_mqtt(?SN_PUBACK) -> ?PUBACK;
mqttsn_to_mqtt(?SN_PUBREC) -> ?PUBREC;
mqttsn_to_mqtt(?SN_PUBREL) -> ?PUBREL;
mqttsn_to_mqtt(?SN_PUBCOMP) -> ?PUBCOMP.



do_subscribe(?SN_NORMAL_TOPIC, TopicId, Qos, MsgId, StateData=#state{client_id = ClientId}) ->
    case emq_sn_registry:register_topic(ClientId, TopicId)of
        undefined ->
            send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = Qos}, ?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            next_state(connected, StateData);
        wildcard_topic ->
            subscribe_broker(TopicId, Qos, MsgId, ?SN_INVALID_TOPIC_ID, StateData);
        NewTopicId ->
            subscribe_broker(TopicId, Qos, MsgId, NewTopicId, StateData)
    end;
do_subscribe(?SN_PREDEFINED_TOPIC, TopicId, Qos, MsgId, StateData=#state{client_id = ClientId}) ->
    case emq_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = Qos}, TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            next_state(connected, StateData);
        PredefinedTopic ->
            subscribe_broker(PredefinedTopic, Qos, MsgId, TopicId, StateData)
        end;
do_subscribe(?SN_SHORT_TOPIC, TopicId, Qos, MsgId, StateData) ->
    TopicName = case is_binary(TopicId) of
            true -> TopicId;
            false -> <<TopicId:16>>
        end,
    subscribe_broker(TopicName, Qos, MsgId, ?SN_INVALID_TOPIC_ID, StateData);
do_subscribe(_, _TopicId, Qos, MsgId, StateData) ->
    send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = Qos}, ?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
    next_state(connected, StateData).



do_unsubscribe(?SN_NORMAL_TOPIC, TopicId, MsgId, StateData) ->
    unsubscribe_broker(TopicId, MsgId, StateData);
do_unsubscribe(?SN_PREDEFINED_TOPIC, TopicId, MsgId, StateData=#state{client_id = ClientId}) ->
    case emq_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            send_message(?SN_UNSUBACK_MSG(MsgId), StateData),
            next_state(connected, StateData);
        PredefinedTopic ->
            unsubscribe_broker(PredefinedTopic, MsgId, StateData)
    end;
do_unsubscribe(?SN_SHORT_TOPIC, TopicId, MsgId, StateData) ->
    TopicName = case is_binary(TopicId) of
                    true -> TopicId;
                    false -> <<TopicId:16>>
                end,
    unsubscribe_broker(TopicName, MsgId, StateData);
do_unsubscribe(_, _TopicId, MsgId, StateData) ->
    send_message(?SN_UNSUBACK_MSG(MsgId), StateData),
    next_state(connected, StateData).


do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId, StateData=#state{client_id = ClientId}) ->
    #mqtt_sn_flags{qos = Qos, dup = Dup, retain = Retain} = Flags,
    case emq_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            (Qos =/= ?QOS0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            next_state(connected, StateData);
        PredefinedTopic ->
            publish_broker(PredefinedTopic, Data, Dup, Qos, Retain, MsgId, TopicId, StateData)
    end;
do_publish(?SN_SHORT_TOPIC, TopicId, Data, Flags, MsgId, StateData) ->
    #mqtt_sn_flags{qos = Qos, dup = Dup, retain = Retain} = Flags,
    TopicName = <<TopicId:16>>,
    case emq_sn_registry:wildcard(TopicName) of
        true ->
            (Qos =/= ?QOS0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_NOT_SUPPORTED), StateData),
            next_state(connected, StateData);
        false ->
            publish_broker(TopicName, Data, Dup, Qos, Retain, MsgId, TopicId, StateData)
    end;
do_publish(_, TopicId, _Data, #mqtt_sn_flags{qos = Qos}, MsgId, StateData) ->
    (Qos =/= ?QOS0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
    next_state(connected, StateData).



subscribe_broker(TopicName, Qos, MsgId, TopicId, StateData=#state{protocol = Proto, awaiting_suback = Awaiting}) ->
    ?LOG(debug, "subscribe Topic=~p, MsgId=~p, TopicId=~p", [TopicName, MsgId, TopicId], StateData),
    NewAwaiting = lists:append(Awaiting, [{MsgId, TopicId}]),
    case emqttd_protocol:received(?SUBSCRIBE_PACKET(MsgId, [{TopicName, Qos}]), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1, awaiting_suback = NewAwaiting});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end.


unsubscribe_broker(TopicName, MsgId, StateData=#state{protocol = Proto}) ->
    ?LOG(debug, "unsubscribe Topic=~p, MsgId=~p", [TopicName, MsgId], StateData),
    case emqttd_protocol:received(?UNSUBSCRIBE_PACKET(MsgId, [TopicName]), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end.

publish_broker(TopicName, Data, Dup, Qos, Retain, MsgId, TopicId, StateData=#state{protocol = Proto, queue_pid = QueuePid}) ->
    (Qos =/= ?QOS0) andalso emq_sn_gateway_queue:insert_puback(QueuePid, TopicId, MsgId),
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = Qos, retain = Retain},
        variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId},
        payload  = Data},
    case emqttd_protocol:received(Publish, Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end.


find_suback_topicid(MsgId, []) ->
    {MsgId, 0};
find_suback_topicid(MsgId, [{MsgId, TopicId}|_Rest]) ->
    {MsgId, TopicId};
find_suback_topicid(MsgId, [{_, _}|Rest]) ->
    find_suback_topicid(MsgId, Rest).



register_and_notify_client(TopicName, Payload, Dup, Qos, Retain, MsgId, StateData=#state{client_id = ClientId}) ->
    TopicId = emq_sn_registry:register_topic(ClientId, TopicName),
    ?LOG(debug, "register TopicId=~p, TopicName=~p, Payload=~p, Dup=~p, Qos=~p, Retain=~p, MsgId=~p",
        [TopicId, TopicName, Payload, Dup, Qos, Retain, MsgId], StateData),
    send_register(TopicName, TopicId, MsgId, StateData),
    send_publish(Dup, Qos, Retain, ?SN_PREDEFINED_TOPIC, TopicId, MsgId, Payload, StateData).


message_id(undefined) ->
    rand:uniform(16#FFFF);
message_id(MsgId) ->
    MsgId.

format(?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_PUBLISH, ~p, TopicId=~w, MsgId=~w, Payload=~w",
        [format_flag(Flags), TopicId, MsgId, Data]));
format(?SN_SUBSCRIBE_MSG(Flags, Msgid, Topic)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_SUBSCRIBE, ~p, MsgId=~w, TopicId=~w",
        [format_flag(Flags), Msgid, Topic]));
format(?SN_SUBACK_MSG(Flags, TopicId, MsgId, ReturnCode)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_SUBACK, ~p, MsgId=~w, TopicId=~w, ReturnCode=~w",
        [format_flag(Flags), MsgId, TopicId, ReturnCode]));
format(?SN_UNSUBSCRIBE_MSG(Flags, Msgid, Topic)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_UNSUBSCRIBE, ~p, MsgId=~w, TopicId=~w",
        [format_flag(Flags), Msgid, Topic]));
format(?SN_UNSUBACK_MSG(MsgId)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_UNSUBACK, MsgId=~w", [MsgId]));
format(?SN_REGISTER_MSG(TopicId, MsgId, TopicName)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_REGISTER, TopicId=~w, MsgId=~w, TopicName=~w",
        [TopicId, MsgId, TopicName]));
format(?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_REGACK, TopicId=~w, MsgId=~w, ReturnCode=~w",
        [TopicId, MsgId, ReturnCode]));
format(#mqtt_sn_message{type = Type, variable = Var}) ->
    lists:flatten(io_lib:format("mqtt_sn_message type=~s, Var=~w", [emq_sn_message:message_type(Type), Var])).


format_flag(#mqtt_sn_flags{dup = Dup, qos = Qos, retain = Retain, will = Will, clean_session = CleanSession, topic_id_type = TopicType}) ->
    lists:flatten(io_lib:format("mqtt_sn_flags{dup=~p, qos=~p, retain=~p, will=~p, clean_session=~p, topic_id_type=~p}", [Dup, Qos, Retain, Will, CleanSession, TopicType]));
format_flag(_Flag) ->
    "invalid flag".
