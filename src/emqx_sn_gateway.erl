%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_gateway).

-behaviour(gen_statem).

-include("emqx_sn.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%% API.
-export([start_link/3]).

%% SUB/UNSUB Asynchronously, called by plugins.
-export([ subscribe/2
        , unsubscribe/2
        ]).

-export([kick/1]).

%% state functions
-export([ idle/3
        , wait_for_will_topic/3
        , wait_for_will_msg/3
        , connected/3
        , asleep/3
        , awake/3
        ]).

%% gen_statem callbacks
-export([ init/1
        , callback_mode/0
        , handle_event/4
        , terminate/3
        , code_change/4
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-record(will_msg, {retain = false  :: boolean(),
                   qos    = ?QOS_0 :: emqx_mqtt_types:qos(),
                   topic           :: binary() | undefined,
                   payload         :: binary() | undefined
                  }).

-record(state, {gwid                 :: integer(),
                sockpid              :: pid(),
                sockname             :: {inet:ip_address(), inet:port()},
                peername             :: {inet:ip_address(), inet:port()},
                channel              :: emqx_channel:channel(),
                clientid             :: binary(),
                will_msg             :: #will_msg{},
                keepalive_interval   :: integer(),
                connpkt              :: term(),
                asleep_timer         :: tuple(),
                asleep_msg_queue     :: list(),
                enable_stats         :: boolean(),
                stats_timer          :: reference(),
                idle_timeout         :: integer(),
                enable_qos3 = false  :: boolean(),
                transform            :: fun((emqx_types:packet()) -> tuple())
               }).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(STAT_TIMEOUT, 10000).
-define(IDLE_TIMEOUT, 30000).
-define(DEFAULT_CHAN_OPTIONS, [{max_packet_size, 256}, {zone, external}]).
-define(LOG(Level, Format, Args, State),
        emqx_logger:Level("MQTT-SN(~s): " ++ Format, [esockd:format(State#state.peername) | Args])).

-define(NEG_QOS_CLIENT_ID, <<"NegQoS-Client">>).

-define(NO_PEERCERT, undefined).

-define(GATEWAY_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

%%--------------------------------------------------------------------
%% Exported APIs
%%--------------------------------------------------------------------

-spec(start_link(pos_integer(), esockd:udp_transport(), {inet:ip_address(), inet:port()})
      -> {ok, pid()} | {error, term()}).
start_link(GwId, Transport, Peername) ->
    gen_statem:start_link(?MODULE, [GwId, Transport, Peername], [{hibernate_after, 60000}]).

subscribe(GwPid, TopicTable) ->
    gen_statem:cast(GwPid, {subscribe, TopicTable}).

unsubscribe(GwPid, Topics) ->
    gen_statem:cast(GwPid, {unsubscribe, Topics}).

kick(GwPid) ->
    gen_statem:call(GwPid, kick).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init([GwId, {_, SockPid, Sock}, Peername]) ->
    case inet:sockname(Sock) of
        {ok, Sockname} ->
            Channel = emqx_channel:init(#{sockname => Sockname,
                                          peername => Peername,
                                          protocol => 'mqtt-sn',
                                          peercert => ?NO_PEERCERT,
                                          conn_mod => ?MODULE
                                         }, ?DEFAULT_CHAN_OPTIONS),
            EnableQos3 = emqx_sn_config:get_env(enable_qos3, false),
            IdleTimeout = emqx_sn_config:get_env(idle_timeout, 30000),
            State = #state{gwid             = GwId,
                           sockpid          = SockPid,
                           sockname         = Sockname,
                           peername         = Peername,
                           channel          = Channel,
                           asleep_timer     = emqx_sn_asleep_timer:init(),
                           asleep_msg_queue = [],
                           enable_qos3      = EnableQos3,
                           idle_timeout     = IdleTimeout,
                           transform        = transform_fun()
                          },
            {ok, idle, State, [IdleTimeout]};
        {error, Reason} when Reason =:= enotconn;
                             Reason =:= einval;
                             Reason =:= closed ->
            {stop, normal};
        {error, Reason} -> {stop, Reason}
    end.

callback_mode() -> state_functions.

idle(cast, {incoming, ?SN_SEARCHGW_MSG(_Radius)}, StateData = #state{gwid = GwId}) ->
    send_message(?SN_GWINFO_MSG(GwId, <<>>), StateData),
    {keep_state, StateData, StateData#state.idle_timeout};

idle(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, StateData) ->
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    do_connect(ClientId, CleanStart, Will, Duration, StateData);

idle(cast, {incoming, Packet = ?CONNECT_PACKET(_ConnPkt)}, StateData) ->
    handle_incoming(Packet, StateData);

idle(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, StateData) ->
    % ignore
    {keep_state, StateData, StateData#state.idle_timeout};

idle(cast, {incoming, ?SN_DISCONNECT_MSG(_Duration)}, StateData) ->
    % ignore
    {keep_state, StateData, StateData#state.idle_timeout};

idle(cast, {incoming, ?SN_PUBLISH_MSG(_Flag, _TopicId, _MsgId, _Data)}, StateData = #state{enable_qos3 = false}) ->
    ?LOG(debug, "The enable_qos3 is false, ignore the received publish with QoS=-1 in idle mode!", [], StateData),
    {keep_state_and_data, StateData#state.idle_timeout};

idle(cast, {incoming, ?SN_PUBLISH_MSG(#mqtt_sn_flags{qos = ?QOS_NEG1,
                                                     topic_id_type = TopicIdType
                                                    }, TopicId, _MsgId, Data)},
     StateData = #state{clientid = ClientId}) ->
    TopicName = case (TopicIdType =:= ?SN_SHORT_TOPIC) of
                    false ->
                        emqx_sn_registry:lookup_topic(ClientId, TopicId);
                    true  -> <<TopicId:16>>
                end,
    Msg = emqx_message:make({?NEG_QOS_CLIENT_ID, emqx_sn_config:get_env(username)},
                            ?QOS_0, TopicName, Data),
    (TopicName =/= undefined) andalso emqx_broker:publish(Msg),
    ?LOG(debug, "Client id=~p receives a publish with QoS=-1 in idle mode!", [ClientId], StateData),
    {keep_state_and_data, StateData#state.idle_timeout};

idle(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, StateData) ->
    handle_ping(PingReq, StateData);

idle(cast, {outgoing, Packet}, StateData) ->
    ok = handle_outgoing(Packet, StateData),
    {keep_state, StateData};

idle(cast, {connack, ConnAck}, StateData) ->
    ok = handle_outgoing(ConnAck, StateData),
    {next_state, connected, StateData};

idle(timeout, _Timeout, StateData) ->
    stop({shutdown, idle_timeout}, StateData);

idle(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, idle, State).

wait_for_will_topic(cast, {incoming, ?SN_WILLTOPIC_EMPTY_MSG}, StateData = #state{connpkt = ConnPkt}) ->
    % empty will topic means deleting will
    NStateData = StateData#state{will_msg = undefined},
    handle_incoming(?CONNECT_PACKET(ConnPkt), NStateData);

wait_for_will_topic(cast, {incoming, ?SN_WILLTOPIC_MSG(Flags, Topic)}, StateData) ->
    #mqtt_sn_flags{qos = QoS, retain = Retain} = Flags,
    WillMsg = #will_msg{retain = Retain, qos = QoS, topic = Topic},
    send_message(?SN_WILLMSGREQ_MSG(), StateData),
    {next_state, wait_for_will_msg, StateData#state{will_msg = WillMsg}};

wait_for_will_topic(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, _StateData) ->
    % ignore
    keep_state_and_data;

wait_for_will_topic(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, StateData) ->
    do_2nd_connect(Flags, Duration, ClientId, StateData);

wait_for_will_topic(cast, {outgoing, Packet}, StateData) ->
    ok = handle_outgoing(Packet, StateData),
    {keep_state, StateData};

wait_for_will_topic(cast, {connack, ConnAck}, StateData) ->
    ok = handle_outgoing(ConnAck, StateData),
    {next_state, connected, StateData};

wait_for_will_topic(cast, Event, StateData) ->
    ?LOG(error, "wait_for_will_topic UNEXPECTED Event: ~p", [Event], StateData),
    keep_state_and_data;

wait_for_will_topic(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, wait_for_will_topic, State).

wait_for_will_msg(cast, {incoming, ?SN_WILLMSG_MSG(Payload)},
                  StateData = #state{will_msg = WillMsg, connpkt = ConnPkt}) ->
    NStateData = StateData#state{will_msg = WillMsg#will_msg{payload = Payload}},
    handle_incoming(?CONNECT_PACKET(ConnPkt), NStateData);

wait_for_will_msg(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, _StateData) ->
    % ignore
    keep_state_and_data;

wait_for_will_msg(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, StateData) ->
    do_2nd_connect(Flags, Duration, ClientId, StateData);

wait_for_will_msg(cast, {outgoing, Packet}, StateData) ->
    ok = handle_outgoing(Packet, StateData),
    {keep_state, StateData};

wait_for_will_msg(cast, {connack, ConnAck}, StateData) ->
    ok = handle_outgoing(ConnAck, StateData),
    {next_state, connected, StateData};

wait_for_will_msg(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, wait_for_will_msg, StateData).

connected(cast, {incoming, ?SN_REGISTER_MSG(_TopicId, MsgId, TopicName)},
          StateData = #state{clientid = ClientId}) ->
    case emqx_sn_registry:register_topic(ClientId, TopicName) of
        TopicId when is_integer(TopicId) ->
            ?LOG(debug, "register ClientId=~p, TopicName=~p, TopicId=~p", [ClientId, TopicName, TopicId], StateData),
            send_message(?SN_REGACK_MSG(TopicId, MsgId, ?SN_RC_ACCEPTED), StateData);
        {error, too_large} ->
            ?LOG(error, "TopicId is full! ClientId=~p, TopicName=~p", [ClientId, TopicName], StateData),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), StateData);
        {error, wildcard_topic} ->
            ?LOG(error, "wildcard topic can not be registered! ClientId=~p, TopicName=~p", [ClientId, TopicName], StateData),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), StateData)
    end,
    {keep_state, StateData};

connected(cast, {incoming, ?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data)},
          StateData = #state{enable_qos3 = EnableQoS3}) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType, qos = QoS} = Flags,
    Skip = (EnableQoS3 =:= false) andalso (QoS =:= ?QOS_NEG1),
    case Skip of
        true  ->
            ?LOG(debug, "The enable_qos3 is false, ignore the received publish with QoS=-1 in connected mode!", [], StateData),
            {keep_state, StateData};
        false ->
            do_publish(TopicIdType, TopicId, Data, Flags, MsgId, StateData)
    end;

connected(cast, {incoming, ?SN_PUBACK_MSG(TopicId, MsgId, RC)}, StateData) ->
    do_puback(TopicId, MsgId, RC, connected, StateData);

connected(cast, {incoming, ?SN_PUBREC_MSG(PubRec, MsgId)}, StateData)
    when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, StateData);

connected(cast, {incoming, ?SN_SUBSCRIBE_MSG(Flags, MsgId, TopicId)}, StateData) ->
    #mqtt_sn_flags{qos = QoS, topic_id_type = TopicIdType} = Flags,
    handle_subscribe(TopicIdType, TopicId, QoS, MsgId, StateData);

connected(cast, {incoming, ?SN_UNSUBSCRIBE_MSG(Flags, MsgId, TopicId)}, StateData) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    handle_unsubscribe(TopicIdType, TopicId, MsgId, StateData);

connected(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, StateData) ->
    handle_ping(PingReq, StateData);

connected(cast, {incoming, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCEPTED)}, StateData) ->
    {keep_state, StateData};
connected(cast, {incoming, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)}, StateData) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode], StateData),
    {keep_state, StateData};

connected(cast, {incoming, ?SN_DISCONNECT_MSG(Duration)}, StateData) ->
    ok = send_message(?SN_DISCONNECT_MSG(undefined), StateData),
    case Duration of
        undefined ->
            handle_incoming(?DISCONNECT_PACKET(), StateData);
        _Other -> goto_asleep_state(Duration, StateData)
    end;

connected(cast, {incoming, ?SN_WILLTOPICUPD_MSG(Flags, Topic)}, StateData = #state{will_msg = WillMsg}) ->
    WillMsg1 = case Topic of
                   undefined -> undefined;
                   _         -> update_will_topic(WillMsg, Flags, Topic)
               end,
    send_message(?SN_WILLTOPICRESP_MSG(0), StateData),
    {keep_state, StateData#state{will_msg = WillMsg1}};

connected(cast, {incoming, ?SN_WILLMSGUPD_MSG(Payload)}, StateData = #state{will_msg = WillMsg}) ->
    ok = send_message(?SN_WILLMSGRESP_MSG(0), StateData),
    {keep_state, StateData#state{will_msg = update_will_msg(WillMsg, Payload)}};

connected(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, StateData) ->
    % ignore
    {keep_state, StateData};

connected(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, StateData) ->
    do_2nd_connect(Flags, Duration, ClientId, StateData);

connected(cast, {outgoing, Packet}, StateData) ->
    ok = handle_outgoing(Packet, StateData),
    {keep_state, StateData};

connected(cast, {shutdown, Reason, Packet}, StateData) ->
    ok = handle_outgoing(Packet, StateData),
    {stop, {shutdown, Reason}, StateData};

connected(cast, {shutdown, Reason}, StateData) ->
    {stop, {shutdown, Reason}, StateData};

connected(cast, {close, Reason}, StateData) ->
    {stop, {shutdown, Reason}, StateData};

connected(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, connected, StateData).

asleep(cast, {incoming, ?SN_DISCONNECT_MSG(Duration)}, StateData) ->
    ok = send_message(?SN_DISCONNECT_MSG(undefined), StateData),
    case Duration of
        undefined ->
            handle_incoming(?PACKET(?DISCONNECT), StateData);
        _Other ->
            goto_asleep_state(Duration, StateData)
    end;

asleep(cast, {incoming, ?SN_PINGREQ_MSG(undefined)}, StateData) ->
    % ClientId in PINGREQ is mandatory
    {keep_state, StateData};

asleep(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(ClientIdPing)},
       StateData = #state{clientid = ClientId}) ->
    case ClientIdPing of
        ClientId ->
            _ = handle_ping(PingReq, StateData),
            self() ! do_awake_jobs,
            % it is better to go awake state, since the jobs in awake may take long time
            % and asleep timer get timeout, it will cause disaster
            {next_state, awake, StateData};
        _Other   ->
            {next_state, asleep, StateData}
    end;

asleep(cast, {incoming, ?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode)}, StateData) ->
    do_puback(TopicId, MsgId, ReturnCode, asleep, StateData);

asleep(cast, {incoming, ?SN_PUBREC_MSG(PubRec, MsgId)}, StateData)
  when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, StateData);

% NOTE: what about following scenario:
%    1) client go to sleep
%    2) client reboot for manual reset or other reasons
%    3) client send a CONNECT
%    4) emq-sn regard this CONNECT as a signal to connected state, not a bootup CONNECT. For this reason, will procedure is lost
% this should be a bug in mqtt-sn channel.
asleep(cast, {incoming, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId)},
       StateData = #state{keepalive_interval = _Interval}) ->
    % device wakeup and goto connected state
    % keepalive timer may timeout in asleep state and delete itself, need to restart keepalive
    % TODO: Fixme later.
    %% self() ! {keepalive, start, Interval},
    send_connack(StateData),
    {next_state, connected, StateData};

asleep(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, asleep, StateData).

awake(cast, {incoming, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCEPTED)}, StateData) ->
    {keep_state, StateData};

awake(cast, {incoming, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)}, StateData) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode], StateData),
    {keep_state, StateData};

awake(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, StateData) ->
    handle_ping(PingReq, StateData);

awake(cast, {outgoing, Packet}, StateData) ->
    ok = handle_outgoing(Packet, StateData),
    {keep_state, StateData};

awake(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, awake, StateData).

handle_event(info, {datagram, SockPid, Data}, StateName,
             StateData = #state{sockpid = SockPid, channel = _Channel}) ->
    ?LOG(debug, "RECV ~p", [Data], StateData),
    Oct = iolist_size(Data),
    emqx_pd:inc_counter(recv_oct, Oct),
    try emqx_sn_frame:parse(Data) of
        {ok, Msg} ->
            emqx_pd:inc_counter(recv_cnt, 1),
            ?LOG(info, "RECV ~s at state ~s",
                 [emqx_sn_frame:format(Msg), StateName], StateData),
            {keep_state, StateData, next_event({incoming, Msg})}
    catch
        error:Error:Stacktrace ->
            ?LOG(info, "Parse frame error: ~p at state ~s, Stacktrace: ~p",
                 [Error, StateName, Stacktrace], StateData),
            shutdown(frame_error, StateData)
    end;

handle_event(info, Deliver = {deliver, _Topic, Msg}, asleep,
             StateData = #state{asleep_msg_queue = AsleepMsgQ}) ->
    % section 6.14, Support of sleeping clients
    ?LOG(debug, "enqueue downlink message in asleep state Msg=~p", [Msg], StateData),
    {keep_state, StateData#state{asleep_msg_queue = [Deliver|AsleepMsgQ]}};

handle_event(info, Deliver = {deliver, _Topic, _Msg}, _StateName,
             StateData = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_deliver([Deliver], Channel), StateData);

handle_event(info, {redeliver, {?PUBREL, MsgId}}, _StateName, StateData) ->
    send_message(?SN_PUBREC_MSG(?SN_PUBREL, MsgId), StateData),
    {keep_state, StateData};

handle_event(info, {timeout, TRef, emit_stats}, _StateName,
             StateData = #state{channel = Channel}) ->
    case emqx_channel:info(clientinfo, Channel) of
        #{clientid := undefined} -> {keep_state, StateData};
        _ -> handle_timeout(TRef, {emit_stats, stats(StateData)}, StateData)
    end;

handle_event(info, {timeout, TRef, keepalive}, _StateName, StateData) ->
    RecvOct = emqx_pd:get_counter(recv_oct),
    handle_timeout(TRef, {keepalive, RecvOct}, StateData);

handle_event(info, {timeout, TRef, TMsg}, _StateName, StateData) ->
    handle_timeout(TRef, TMsg, StateData);

handle_event(info, do_awake_jobs, StateName, StateData=#state{clientid = ClientId}) ->
    ?LOG(debug, "Do awake jobs, statename : ~p", [StateName], StateData),
    case process_awake_jobs(ClientId, StateData) of
        {keep_state, NewStateData} ->
            case StateName of
                awake  -> goto_asleep_state(undefined, NewStateData);
                _Other -> {keep_state, NewStateData}
                          %% device send a CONNECT immediately before this do_awake_jobs is handled
            end;
        Stop -> Stop
    end;

handle_event(info, {asleep_timeout, Ref}, StateName, StateData=#state{asleep_timer = AsleepTimer}) ->
    ?LOG(debug, "asleep_timeout at ~p", [StateName], StateData),
    case emqx_sn_asleep_timer:timeout(AsleepTimer, StateName, Ref) of
        terminate_process         -> stop(asleep_timeout, StateData);
        {restart_timer, NewTimer} -> goto_asleep_state(undefined, StateData#state{asleep_timer = NewTimer});
        {stop_timer, NewTimer}    -> {keep_state, StateData#state{asleep_timer = NewTimer}}
    end;

handle_event({call, From}, kick, _StateName, StateData = #state{clientid = ClientId}) ->
    ?LOG(warning, "Clientid '~s' will be kicked out", [ClientId], StateData),
    {stop_and_reply, kicked, [{reply, From, ok}], StateData};

handle_event(info, {shutdown, conflict, {ClientId, NewPid}}, _StateName, StateData) ->
    ?LOG(warning, "Clientid '~s' conflict with ~p", [ClientId, NewPid], StateData),
    stop({shutdown, conflict}, StateData);

handle_event(EventType, EventContent, StateName, StateData) ->
    ?LOG(error, "StateName: ~s, Unexpected Event: ~p",
         [StateName, {EventType, EventContent}], StateData),
    {keep_state, StateData}.

terminate(Reason, _StateName, #state{clientid = ClientId,
                                     channel  = Channel}) ->
    emqx_sn_registry:unregister_topic(ClientId),
    case {Channel, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} ->
            emqx_channel:terminate(Error, Channel);
        {_, Reason} ->
            emqx_channel:terminate(Reason, Channel)
    end.

code_change(_Vsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

handle_ping(_PingReq, StateData) ->
    emqx_pd:inc_counter(recv_oct, 2),
    emqx_pd:inc_counter(recv_msg, 1),
    ok = send_message(?SN_PINGRESP_MSG(), StateData),
    {keep_state, StateData}.

handle_timeout(TRef, TMsg, StateData = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_timeout(TRef, TMsg, Channel), StateData).

handle_return(ok, StateData) ->
    {keep_state, StateData};
handle_return({ok, NChannel}, StateData) ->
    {keep_state, StateData#state{channel = NChannel}};
handle_return({ok, Replies, NChannel}, StateData) ->
    {keep_state, StateData#state{channel = NChannel}, next_events(Replies)};

handle_return({shutdown, Reason, NChannel}, StateData) ->
    stop(Reason, StateData#state{channel = NChannel});
handle_return({stop, Reason, NChannel}, StateData) ->
    stop(Reason, StateData#state{channel = NChannel});
handle_return({stop, Reason, OutPacket, NChannel}, StateData) ->
    NStateData = StateData#state{channel = NChannel},
    ok = handle_outgoing(OutPacket, NStateData),
    stop(Reason, NStateData).

next_events(Packet) when is_record(Packet, mqtt_packet) ->
    next_event({outgoing, Packet});
next_events(Action) when is_tuple(Action) ->
    next_event(Action);
next_events(Actions) when is_list(Actions) ->
    lists:map(fun next_event/1, Actions).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

transform(?CONNACK_PACKET(0), _FuncMsgIdToTopicId) ->
    ?SN_CONNACK_MSG(0);

transform(?CONNACK_PACKET(_ReturnCode), _FuncMsgIdToTopicId) ->
    ?SN_CONNACK_MSG(?SN_RC_CONGESTION);

transform(?PUBLISH_PACKET(QoS, Topic, PacketId, Payload), _FuncMsgIdToTopicId) ->
    NewPacketId = if
                      QoS =:= ?QOS_0 -> 0;
                      true           -> PacketId
                  end,
    ClientId = get(clientid),
    {TopicIdType, TopicContent} = case emqx_sn_registry:lookup_topic_id(ClientId, Topic) of
                                      {predef, PredefTopicId} ->
                                          {?SN_PREDEFINED_TOPIC, PredefTopicId};
                                      TopicId when is_integer(TopicId) ->
                                          {?SN_NORMAL_TOPIC, TopicId};
                                      undefined ->
                                          {?SN_SHORT_TOPIC, Topic}
                                  end,

    Flags = #mqtt_sn_flags{qos = QoS, topic_id_type = TopicIdType},
    ?SN_PUBLISH_MSG(Flags, TopicContent, NewPacketId, Payload);

transform(?PUBACK_PACKET(MsgId, _ReasonCode), FuncMsgIdToTopicId) ->
    TopicIdFinal =  get_topic_id(puback, MsgId, FuncMsgIdToTopicId),
    ?SN_PUBACK_MSG(TopicIdFinal, MsgId, ?SN_RC_ACCEPTED);

transform(?PUBREC_PACKET(MsgId), _FuncMsgIdToTopicId) ->
    ?SN_PUBREC_MSG(?SN_PUBREC, MsgId);

transform(?PUBREL_PACKET(MsgId), _FuncMsgIdToTopicId) ->
    ?SN_PUBREC_MSG(?SN_PUBREL, MsgId);

transform(?PUBCOMP_PACKET(MsgId), _FuncMsgIdToTopicId) ->
    ?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId);

transform(?SUBACK_PACKET(MsgId, ReturnCodes), FuncMsgIdToTopicId)->
    % if success, suback is sent by handle_info({suback, MsgId, [GrantedQoS]}, ...)
    % if failure, suback is sent in this function.
    [ReturnCode | _ ] = ReturnCodes,
    {QoS, TopicId, NewReturnCode}
        = case ?IS_QOS(ReturnCode) of
              true ->
                  {ReturnCode, get_topic_id(suback, MsgId, FuncMsgIdToTopicId), ?SN_RC_ACCEPTED};
              _ ->
                  {?QOS_0, get_topic_id(suback, MsgId, FuncMsgIdToTopicId), ?SN_RC_NOT_SUPPORTED}
          end,
    Flags = #mqtt_sn_flags{qos = QoS},
    ?SN_SUBACK_MSG(Flags, TopicId, MsgId, NewReturnCode);

transform(?UNSUBACK_PACKET(MsgId), _FuncMsgIdToTopicId)->
    ?SN_UNSUBACK_MSG(MsgId).

send_register(TopicName, TopicId, MsgId, StateData) ->
    send_message(?SN_REGISTER_MSG(TopicId, MsgId, TopicName), StateData).

send_connack(StateData) ->
    send_message(?SN_CONNACK_MSG(?SN_RC_ACCEPTED), StateData).

send_message(Msg = #mqtt_sn_message{type = Type},
             StateData = #state{sockpid = SockPid, peername = Peername}) ->
    ?LOG(debug, "SEND ~s~n", [emqx_sn_frame:format(Msg)], StateData),
    inc_outgoing_stats(Type),
    Data = emqx_sn_frame:serialize(Msg),
    ok = emqx_metrics:inc('bytes.sent', iolist_size(Data)),
    SockPid ! {datagram, Peername, Data},
    ok.

goto_asleep_state(Duration, StateData=#state{asleep_timer = AsleepTimer}) ->
    ?LOG(debug, "goto_asleep_state Duration=~p", [Duration], StateData),
    NewTimer = emqx_sn_asleep_timer:start(AsleepTimer, Duration),
    {next_state, asleep, StateData#state{asleep_timer = NewTimer}, hibernate}.

shutdown(Error, StateData) ->
    ?LOG(error, "shutdown due to ~p", [Error], StateData),
    stop({shutdown, Error}, StateData).

stop(Reason, StateData) ->
    case Reason of
        asleep_timeout                    -> do_publish_will(StateData);
        {shutdown, keepalive_timeout}     -> do_publish_will(StateData);
        _                                 -> ok
    end,
    {stop, normal, StateData}.

mqttsn_to_mqtt(?SN_PUBACK, MsgId)  ->
    ?PUBACK_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBREC, MsgId)  ->
    ?PUBREC_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBREL, MsgId)  ->
    ?PUBREL_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBCOMP, MsgId) ->
    ?PUBCOMP_PACKET(MsgId).

do_connect(ClientId, CleanStart, WillFlag, Duration, StateData) ->
    {ok, Username} = emqx_sn_config:get_env(username),
    {ok, Password} = emqx_sn_config:get_env(password),
    ConnPkt = #mqtt_packet_connect{clientid    = ClientId,
                                   clean_start = CleanStart,
                                   username    = Username,
                                   password    = Password,
                                   keepalive   = Duration
                                  },
    put(clientid, ClientId),
    case WillFlag of
        true -> send_message(?SN_WILLTOPICREQ_MSG(), StateData),
                NState = StateData#state{connpkt  = ConnPkt,
                                         clientid = ClientId,
                                         keepalive_interval = Duration
                                        },
                {next_state, wait_for_will_topic, NState};
        false ->
            NStateData = StateData#state{clientid = ClientId,
                                         keepalive_interval = Duration
                                        },
            handle_incoming(?CONNECT_PACKET(ConnPkt), NStateData)
    end.

do_2nd_connect(Flags, Duration, ClientId, StateData = #state{sockname = Sockname,
                                                             peername = Peername,
                                                             clientid = OldClientId,
                                                             channel  = Channel}) ->
    emqx_channel:terminate(normal, Channel),
    emqx_sn_registry:unregister_topic(OldClientId),
    Channel1 = emqx_channel:init(#{sockname => Sockname,
                                   peername => Peername,
                                   protocol => 'mqtt-sn',
                                   peercert => ?NO_PEERCERT,
                                   conn_mod => ?MODULE
                                  }, ?DEFAULT_CHAN_OPTIONS),
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    NStateData = StateData#state{channel = Channel1},
    do_connect(ClientId, CleanStart, Will, Duration, NStateData).

handle_subscribe(?SN_NORMAL_TOPIC, TopicName, QoS, MsgId, StateData=#state{clientid = ClientId}) ->
    case emqx_sn_registry:register_topic(ClientId, TopicName) of
        {error, too_large} ->
            ok = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                             ?SN_INVALID_TOPIC_ID,
                                             MsgId,
                                             ?SN_RC_INVALID_TOPIC_ID), StateData),
            {keep_state, StateData};
        {error, wildcard_topic} ->
            proto_subscribe(TopicName, QoS, MsgId, ?SN_INVALID_TOPIC_ID, StateData);
        NewTopicId when is_integer(NewTopicId) ->
            proto_subscribe(TopicName, QoS, MsgId, NewTopicId, StateData)
    end;

handle_subscribe(?SN_PREDEFINED_TOPIC, TopicId, QoS, MsgId, StateData = #state{clientid = ClientId}) ->
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            ok = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                             TopicId,
                                             MsgId,
                                             ?SN_RC_INVALID_TOPIC_ID), StateData),
            {next_state, connected, StateData};
        PredefinedTopic ->
            proto_subscribe(PredefinedTopic, QoS, MsgId, TopicId, StateData)
    end;

handle_subscribe(?SN_SHORT_TOPIC, TopicId, QoS, MsgId, StateData) ->
    TopicName = case is_binary(TopicId) of
                    true  -> TopicId;
                    false -> <<TopicId:16>>
                end,
    proto_subscribe(TopicName, QoS, MsgId, ?SN_INVALID_TOPIC_ID, StateData);

handle_subscribe(_, _TopicId, QoS, MsgId, StateData) ->
    ok = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                     ?SN_INVALID_TOPIC_ID,
                                     MsgId,
                                     ?SN_RC_INVALID_TOPIC_ID), StateData),
    {keep_state, StateData}.

handle_unsubscribe(?SN_NORMAL_TOPIC, TopicId, MsgId, StateData) ->
    proto_unsubscribe(TopicId, MsgId, StateData);

handle_unsubscribe(?SN_PREDEFINED_TOPIC, TopicId, MsgId, StateData = #state{clientid = ClientId}) ->
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            ok = send_message(?SN_UNSUBACK_MSG(MsgId), StateData),
            {keep_state, StateData};
        PredefinedTopic ->
            proto_unsubscribe(PredefinedTopic, MsgId, StateData)
    end;

handle_unsubscribe(?SN_SHORT_TOPIC, TopicId, MsgId, StateData) ->
    TopicName = case is_binary(TopicId) of
                    true  -> TopicId;
                    false -> <<TopicId:16>>
                end,
    proto_unsubscribe(TopicName, MsgId, StateData);

handle_unsubscribe(_, _TopicId, MsgId, StateData) ->
    send_message(?SN_UNSUBACK_MSG(MsgId), StateData),
    {keep_state, StateData}.

do_publish(?SN_NORMAL_TOPIC, TopicId, Data, Flags, MsgId, StateData) ->
    %% Handle normal topic id as predefined topic id, to be compatible with paho mqtt-sn library
    do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId, StateData);
do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId, StateData=#state{clientid = ClientId}) ->
    #mqtt_sn_flags{qos = QoS, dup = Dup, retain = Retain} = Flags,
    NewQoS = get_corrected_qos(QoS, StateData),
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            (NewQoS =/= ?QOS_0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            {keep_state, StateData};
        TopicName ->
            proto_publish(TopicName, Data, Dup, NewQoS, Retain, MsgId, TopicId, StateData)
    end;
do_publish(?SN_SHORT_TOPIC, TopicId, Data, Flags, MsgId, StateData) ->
    #mqtt_sn_flags{qos = QoS, dup = Dup, retain = Retain} = Flags,
    NewQoS = get_corrected_qos(QoS, StateData),
    TopicName = <<TopicId:16>>,
    case emqx_topic:wildcard(TopicName) of
        true ->
            (NewQoS =/= ?QOS_0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_NOT_SUPPORTED), StateData),
            {keep_state, StateData};
        false ->
            proto_publish(TopicName, Data, Dup, NewQoS, Retain, MsgId, TopicId, StateData)
    end;
do_publish(_, TopicId, _Data, #mqtt_sn_flags{qos = QoS}, MsgId, StateData) ->
    (QoS =/= ?QOS_0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_NOT_SUPPORTED), StateData),
    {keep_state, StateData}.

do_publish_will(#state{will_msg = undefined}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{payload = undefined}}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{topic = undefined}}) ->
    ok;
do_publish_will(#state{channel = Channel, will_msg = WillMsg}) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic, payload = Payload} = WillMsg,
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = false,
                                                          qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = Topic, packet_id = 1000},
                           payload  = Payload},
    ClientInfo = emqx_channel:info(clientinfo, Channel),
    emqx_broker:publish(emqx_packet:to_message(ClientInfo, Publish)),
    ok.

do_puback(TopicId, MsgId, ReturnCode, _StateName, StateData=#state{clientid = ClientId}) ->
    case ReturnCode of
        ?SN_RC_ACCEPTED ->
            handle_incoming(?PUBACK_PACKET(MsgId), StateData);
        ?SN_RC_INVALID_TOPIC_ID ->
            case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
                undefined -> ok;
                TopicName ->
                    %%notice that this TopicName maybe normal or predefined,
                    %% involving the predefined topic name in register to enhance the gateway's robustness even inconsistent with MQTT-SN channels
                    send_register(TopicName, TopicId, MsgId, StateData),
                    {keep_state, StateData}
            end;
        _ ->
            ?LOG(error, "CAN NOT handle PUBACK ReturnCode=~p", [ReturnCode], StateData),
            {keep_state, StateData}
    end.

do_pubrec(PubRec, MsgId, StateData) ->
    handle_incoming(mqttsn_to_mqtt(PubRec, MsgId), StateData).

proto_subscribe(TopicName, QoS, MsgId, TopicId, StateData) ->
    ?LOG(debug, "subscribe Topic=~p, MsgId=~p, TopicId=~p",
         [TopicName, MsgId, TopicId], StateData),
    enqueue_msgid(suback, MsgId, TopicId),
    SubOpts = maps:put(qos, QoS, ?DEFAULT_SUBOPTS),
    handle_incoming(?SUBSCRIBE_PACKET(MsgId, [{TopicName, SubOpts}]), StateData).

proto_unsubscribe(TopicName, MsgId, StateData) ->
    ?LOG(debug, "unsubscribe Topic=~p, MsgId=~p", [TopicName, MsgId], StateData),
    handle_incoming(?UNSUBSCRIBE_PACKET(MsgId, [TopicName]), StateData).

proto_publish(TopicName, Data, Dup, QoS, Retain, MsgId, TopicId, StateData) ->
    (QoS =/= ?QOS_0) andalso enqueue_msgid(puback, MsgId, TopicId),
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId},
                           payload  = Data},
    ?LOG(debug, "[publish] Msg: ~p~n", [Publish], StateData),
    handle_incoming(Publish, StateData).

update_will_topic(undefined, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic};
update_will_topic(Will=#will_msg{}, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    Will#will_msg{qos = QoS, retain = Retain, topic = Topic}.

update_will_msg(undefined, Msg) ->
    #will_msg{payload = Msg};
update_will_msg(Will = #will_msg{}, Msg) ->
    Will#will_msg{payload = Msg}.

process_awake_jobs(_ClientId, StateData = #state{asleep_msg_queue = []}) ->
    {keep_state, StateData};
process_awake_jobs(_ClientId, StateData = #state{channel = Channel,
                                                 asleep_msg_queue = AsleepMsgQ}) ->
    Delivers = lists:reverse(AsleepMsgQ),
    NStateData = StateData#state{asleep_msg_queue = []},
    Result = emqx_channel:handle_deliver(Delivers, Channel),
    handle_return(Result, NStateData).

enqueue_msgid(suback, MsgId, TopicId) ->
    put({suback, MsgId}, TopicId);
enqueue_msgid(puback, MsgId, TopicId) ->
    put({puback, MsgId}, TopicId).

dequeue_msgid(suback, MsgId) ->
    erase({suback, MsgId});
dequeue_msgid(puback, MsgId) ->
    erase({puback, MsgId}).

stats(#state{channel = Channel}) ->
    ConnStats = [{Name, emqx_pd:get_counter(Name)} || Name <- ?GATEWAY_STATS],
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_misc:proc_stats(),
    lists:append([ConnStats, ChanStats, ProcStats]).

get_corrected_qos(?QOS_NEG1, StateData) ->
    ?LOG(debug, "Receive a publish with QoS=-1", [], StateData),
    ?QOS_0;

get_corrected_qos(QoS, _StateData) ->
    QoS.

get_topic_id(Type, MsgId, Func) ->
    case Func(Type, MsgId) of
        undefined -> 0;
        TopicId -> TopicId
    end.

handle_incoming(Packet = ?PACKET(Type), StateData = #state{channel = Channel}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)], StateData),
    Result = emqx_channel:handle_in(Packet, Channel),
    handle_return(Result, StateData).

handle_outgoing(Packets, StateData) when is_list(Packets) ->
    lists:foreach(fun(Packet) -> handle_outgoing(Packet, StateData) end, Packets);

handle_outgoing(PubPkt = ?PUBLISH_PACKET(QoS, TopicName, PacketId, Payload),
                StateData = #state{clientid = ClientId, transform = Transform}) ->
    #mqtt_packet{header = #mqtt_packet_header{dup = Dup, retain = Retain}} = PubPkt,
    MsgId = message_id(PacketId),
    ?LOG(debug, "Handle outgoing: ~p", [PubPkt], StateData),

    (emqx_sn_registry:lookup_topic_id(ClientId, TopicName) == undefined)
        andalso (byte_size(TopicName) =/= 2)
            andalso register_and_notify_client(TopicName, Payload, Dup, QoS,
                                               Retain, MsgId, ClientId, StateData),
    send_message(Transform(PubPkt), StateData);


handle_outgoing(Packet, State = #state{transform = Transform}) ->
    send_message(Transform(Packet), State).

register_and_notify_client(TopicName, Payload, Dup, QoS, Retain, MsgId, ClientId, StateData) ->
    TopicId = emqx_sn_registry:register_topic(ClientId, TopicName),
    ?LOG(debug, "register TopicId=~p, TopicName=~p, Payload=~p, Dup=~p, QoS=~p, Retain=~p, MsgId=~p",
        [TopicId, TopicName, Payload, Dup, QoS, Retain, MsgId], StateData),
    send_register(TopicName, TopicId, MsgId, StateData).

message_id(undefined) ->
    rand:uniform(16#FFFF);
message_id(MsgId) -> MsgId.

transform_fun() ->
    FunMsgIdToTopicId = fun(Type, MsgId) -> dequeue_msgid(Type, MsgId) end,
    fun(Packet) -> transform(Packet, FunMsgIdToTopicId) end.

inc_incoming_stats(Type) ->
    emqx_pd:inc_counter(recv_pkt, 1),
    case Type == ?PUBLISH of
        true ->
            emqx_pd:inc_counter(recv_msg, 1),
            emqx_pd:inc_counter(incoming_pubs, 1);
        false -> ok
    end.

inc_outgoing_stats(Type) ->
    emqx_pd:inc_counter(send_pkt, 1),
    (Type == ?SN_PUBLISH)
        andalso emqx_pd:inc_counter(send_msg, 1).

next_event(Content) ->
    {next_event, cast, Content}.

