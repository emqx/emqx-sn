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
                   payload         :: binary() | undefined}).

-record(state, {gwid                 :: integer(),
                sockpid              :: pid(),
                peer                 :: {inet:ip_address(), inet:port()},
                chan_state           :: emqx_channel:chan_state(),
                client_id            :: binary(),
                will_msg             :: #will_msg{},
                keepalive_interval   :: integer(),
                keepalive            :: emqx_sn_keepalive:keepalive() | undefined,
                connpkt              :: term(),
                awaiting_suback = [] :: list(),
                asleep_timer         :: tuple(),
                asleep_msg_queue     :: term(),
                enable_stats         :: boolean(),
                stats_timer          :: reference(),
                idle_timeout         :: integer(),
                enable_qos3 = false  :: boolean(),
                transformer          :: fun((emqx_types:packet()) -> tuple())
               }).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(STAT_TIMEOUT, 10000).
-define(IDLE_TIMEOUT, 30000).
-define(DEFAULT_CHAN_OPTIONS, [{max_packet_size, 256}, {zone, external}]).
-define(LOG(Level, Format, Args, State),
        emqx_logger:Level("MQTT-SN(~s): " ++ Format, [esockd_net:format(State#state.peer) | Args])).

-define(NEG_QOS_CLIENT_ID, <<"NegQoS-Client">>).

-define(NO_PEERCERT, undefined).

-define(GATEWAY_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

%%--------------------------------------------------------------------
%% Exported APIs
%%--------------------------------------------------------------------

-spec(start_link(pos_integer(), esockd:udp_transport(), {inet:ip_address(), inet:port()})
      -> {ok, pid()} | {error, term()}).
start_link(GwId, Transport, Peer) ->
    gen_statem:start_link(?MODULE, [GwId, Transport, Peer], []).

subscribe(GwPid, TopicTable) ->
    gen_statem:cast(GwPid, {subscribe, TopicTable}).

unsubscribe(GwPid, Topics) ->
    gen_statem:cast(GwPid, {unsubscribe, Topics}).

kick(GwPid) ->
    gen_statem:call(GwPid, kick).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init([GwId, {_, SockPid, _Sock}, Peer]) ->
    State = #state{gwid             = GwId,
                   sockpid          = SockPid,
                   peer             = Peer,
                   asleep_timer     = emqx_sn_asleep_timer:init(),
                   asleep_msg_queue = queue:new(),
                   enable_stats     = emqx_sn_config:get_env(enable_stats, false),
                   idle_timeout     = emqx_sn_config:get_env(idle_timeout, 30000),
                   enable_qos3      = emqx_sn_config:get_env(enable_qos3, false),
                   transformer      = transformer_fun()},
    {ok, idle, State#state{chan_state = channel_init(State)}}.

callback_mode() -> state_functions.

idle(cast, ?SN_SEARCHGW_MSG(_Radius), StateData = #state{gwid = GwId}) ->
    send_message(?SN_GWINFO_MSG(GwId, <<>>), StateData),
    {keep_state, StateData, StateData#state.idle_timeout};

idle(cast, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId), StateData) ->
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    do_connect(ClientId, CleanStart, Will, Duration, StateData);

idle(cast, ?SN_ADVERTISE_MSG(_GwId, _Radius), StateData) ->
    % ignore
    {keep_state, StateData, StateData#state.idle_timeout};

idle(cast, ?SN_DISCONNECT_MSG(_Duration), StateData) ->
    % ignore
    {keep_state, StateData, StateData#state.idle_timeout};

idle(cast, ?SN_PUBLISH_MSG(_Flag, _TopicId, _MsgId, _Data), StateData = #state{enable_qos3 = false}) ->
    ?LOG(debug, "The enable_qos3 is false, ignore the received publish with QoS=-1 in idle mode!", [], StateData),
    {keep_state_and_data, StateData#state.idle_timeout};

idle(cast, ?SN_PUBLISH_MSG(#mqtt_sn_flags{qos = ?QOS_NEG1, topic_id_type = TopicIdType},
                           TopicId, _MsgId, Data), StateData = #state{client_id = ClientId}) ->
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

idle(timeout, _Timeout, StateData) ->
    stop(idle_timeout, StateData);

idle(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, idle, State).

wait_for_will_topic(cast, ?SN_WILLTOPIC_EMPTY_MSG, StateData = #state{connpkt = ConnPkt}) ->
    % empty will topic means deleting will
    SuccFun = fun(NStateData) ->
                      {next_state, connected, NStateData#state{will_msg = undefined}}
              end,
    handle_incoming(?CONNECT_PACKET(ConnPkt), SuccFun, StateData);

wait_for_will_topic(cast, ?SN_WILLTOPIC_MSG(Flags, Topic), StateData) ->
    #mqtt_sn_flags{qos = QoS, retain = Retain} = Flags,
    WillMsg = #will_msg{retain = Retain, qos = QoS, topic = Topic},
    send_message(?SN_WILLMSGREQ_MSG(), StateData),
    {next_state, wait_for_will_msg, StateData#state{will_msg = WillMsg}};

wait_for_will_topic(cast, ?SN_ADVERTISE_MSG(_GwId, _Radius), _StateData) ->
    % ignore
    keep_state_and_data;

wait_for_will_topic(cast, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId), StateData) ->
    do_2nd_connect(Flags, Duration, ClientId, StateData);

wait_for_will_topic(cast, Event, StateData) ->
    ?LOG(error, "wait_for_will_topic UNEXPECTED Event: ~p", [Event], StateData),
    keep_state_and_data;

wait_for_will_topic(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, wait_for_will_topic, State).

wait_for_will_msg(cast, ?SN_WILLMSG_MSG(Payload), StateData = #state{will_msg = WillMsg, connpkt = ConnPkt}) ->
    SuccFun = fun(NStateData) ->
                      {next_state, connected, NStateData#state{will_msg = WillMsg#will_msg{payload = Payload}}}
              end,
    handle_incoming(?CONNECT_PACKET(ConnPkt), SuccFun, StateData);

wait_for_will_msg(cast, ?SN_ADVERTISE_MSG(_GwId, _Radius), _StateData) ->
    % ignore
    keep_state_and_data;

wait_for_will_msg(cast, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId), StateData) ->
    do_2nd_connect(Flags, Duration, ClientId, StateData);

wait_for_will_msg(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, wait_for_will_msg, StateData).

connected(cast, ?SN_REGISTER_MSG(_TopicId, MsgId, TopicName),
          StateData = #state{client_id = ClientId}) ->
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

connected(cast, ?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data),
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

connected(cast, ?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode), StateData) ->
    do_puback(TopicId, MsgId, ReturnCode, connected, StateData);

connected(cast, ?SN_PUBREC_MSG(PubRec, MsgId), StateData)
    when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, StateData);

connected(cast, ?SN_SUBSCRIBE_MSG(Flags, MsgId, TopicId), StateData) ->
    #mqtt_sn_flags{qos = QoS, topic_id_type = TopicIdType} = Flags,
    do_subscribe(TopicIdType, TopicId, QoS, MsgId, StateData);

connected(cast, ?SN_UNSUBSCRIBE_MSG(Flags, MsgId, TopicId), StateData) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    do_unsubscribe(TopicIdType, TopicId, MsgId, StateData);

connected(cast, ?SN_PINGREQ_MSG(_ClientId), StateData) ->
    send_message(?SN_PINGRESP_MSG(), StateData),
    {keep_state, StateData};

connected(cast, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCEPTED), StateData) ->
    {keep_state, StateData};
connected(cast, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode), StateData) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p", [TopicId, MsgId, ReturnCode], StateData),
    {keep_state, StateData};

connected(cast, ?SN_DISCONNECT_MSG(Duration), StateData) ->
    ok = send_message(?SN_DISCONNECT_MSG(undefined), StateData),
    case Duration of
        undefined ->
            handle_incoming(?DISCONNECT_PACKET(), fun keep_state/1, StateData);
        Other ->
            goto_asleep_state(StateData, Other)
    end;

connected(cast, ?SN_WILLTOPICUPD_MSG(Flags, Topic), StateData = #state{will_msg = WillMsg}) ->
    WillMsg1 = case Topic of
                   undefined -> undefined;
                   _         -> update_will_topic(WillMsg, Flags, Topic)
               end,
    send_message(?SN_WILLTOPICRESP_MSG(0), StateData),
    {keep_state, StateData#state{will_msg = WillMsg1}};

connected(cast, ?SN_WILLMSGUPD_MSG(Payload), StateData = #state{will_msg = WillMsg}) ->
    send_message(?SN_WILLMSGRESP_MSG(0), StateData),
    {keep_state, StateData#state{will_msg = update_will_msg(WillMsg, Payload)}};

connected(cast, ?SN_ADVERTISE_MSG(_GwId, _Radius), StateData) ->
    % ignore
    {keep_state, StateData};

connected(cast, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId), StateData) ->
    do_2nd_connect(Flags, Duration, ClientId, StateData);

connected(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, connected, StateData).

asleep(cast, ?SN_DISCONNECT_MSG(Duration), StateData) ->
    ok = send_message(?SN_DISCONNECT_MSG(undefined), StateData),
    case Duration of
        undefined ->
            handle_incoming(?PACKET(?DISCONNECT), fun keep_state/1, StateData);
        Other     ->
            goto_asleep_state(StateData, Other)
    end;

asleep(cast, ?SN_PINGREQ_MSG(undefined), StateData) ->
    % ClientId in PINGREQ is mandatory
    {keep_state, StateData};

asleep(cast, ?SN_PINGREQ_MSG(ClientIdPing), StateData = #state{client_id = ClientId}) ->
    case ClientIdPing of
        ClientId ->
            self() ! do_awake_jobs,
            % it is better to go awake state, since the jobs in awake may take long time
            % and asleep timer get timeout, it will cause disaster
            {next_state, awake, StateData};
        _Other   ->
            {next_state, asleep, StateData}
    end;

asleep(cast, ?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode), StateData) ->
    do_puback(TopicId, MsgId, ReturnCode, asleep, StateData);

asleep(cast, ?SN_PUBREC_MSG(PubRec, MsgId), StateData)
  when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, StateData);

% NOTE: what about following scenario:
%    1) client go to sleep
%    2) client reboot for manual reset or other reasons
%    3) client send a CONNECT
%    4) emq-sn regard this CONNECT as a signal to connected state, not a bootup CONNECT. For this reason, will procedure is lost
% this should be a bug in mqtt-sn chan_state.
asleep(cast, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId),
       StateData = #state{keepalive_interval = Interval}) ->
    % device wakeup and goto connected state
    % keepalive timer may timeout in asleep state and delete itself, need to restart keepalive
    self() ! {keepalive, start, Interval},
    send_connack(StateData),
    {next_state, connected, StateData};

asleep(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, asleep, StateData).

awake(cast, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCEPTED), StateData) ->
    {keep_state, StateData};

awake(cast, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode), StateData) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode], StateData),
    {keep_state, StateData};

awake(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, awake, StateData).

handle_event(info, {datagram, SockPid, Data}, StateName, StateData = #state{sockpid = SockPid}) ->
    StateData1 = ensure_stats_timer(StateData),
    try emqx_sn_frame:parse(Data) of
        {ok, Msg} ->
            gen_statem:cast(self(), Msg),
            ?LOG(info, "RECV ~s at state ~s", [emqx_sn_frame:format(Msg), StateName], StateData1),
            emqx_pd:update_counter(recv_oct, iolist_size(Data)),
            emqx_pd:update_counter(recv_cnt, 1),
            {keep_state, StateData1}
    catch
        error : Error : Stacktrace ->
            ?LOG(info, "Parse frame error: ~p at state ~s, Stacktrace: ~p", [Error, StateName, Stacktrace], StateData1),
            shutdown(frame_error, StateData1)
    end;

handle_event(info, Deliver = {deliver, _Topic, Msg}, asleep,
             StateData = #state{asleep_msg_queue = AsleepMsgQue}) ->
    % section 6.14, Support of sleeping clients
    StateData1 = ensure_stats_timer(StateData),
    ?LOG(debug, "enqueue downlink message in asleep state Msg=~p", [Msg], StateData1),
    NewAsleepMsgQue = queue:in(Deliver, AsleepMsgQue),
    {keep_state, StateData1#state{asleep_msg_queue = NewAsleepMsgQue}};

handle_event(info, Deliver = {deliver, _Topic, _Msg}, _StateName,
             StateData = #state{client_id = ClientId}) ->
    StateData1 = ensure_stats_timer(StateData),
    {ok, NChanState} = send_message_to_device(Deliver, ClientId, StateData1),
    {keep_state, StateData#state{chan_state = NChanState}};

handle_event(info, {timeout, Timer, emit_stats}, _StateName,
             StateData = #state{stats_timer = Timer,
                                chan_state    = ChanState}) ->
    emqx_cm:set_chan_stats(emqx_channel:info(client_id, ChanState), stats(StateData)),
    {keep_state, StateData#state{stats_timer = undefined}};

handle_event(info, {redeliver, {?PUBREL, MsgId}}, _StateName, StateData) ->
    send_message(?SN_PUBREC_MSG(?SN_PUBREL, MsgId), StateData),
    {keep_state, StateData};

handle_event(info, {keepalive, start, Interval}, _StateName,
             StateData = #state{keepalive = undefined}) ->
    ?LOG(debug, "Keepalive at the interval of ~p seconds", [Interval], StateData),
    StatFun = fun() -> {ok, emqx_pd:get_counter(recv_oct)} end,
    case emqx_sn_keepalive:start(StatFun, Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {keep_state, StateData#state{keepalive = KeepAlive}};
        {error, Reason} ->
            ?LOG(warning, "Keepalive error - ~p", [Reason], StateData),
            shutdown(Reason, StateData)
    end;

handle_event(info, {keepalive, start, _Interval}, _StateName, StateData) ->
    %% keepalive is still running, do nothing
    {keep_state, StateData};

handle_event(info, {keepalive, check}, StateName, StateData = #state{keepalive = KeepAlive}) ->
    case emqx_sn_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            ?LOG(debug, "Keepalive check ok StateName=~p, KeepAlive=~p, KeepAlive1=~p",
                 [StateName, KeepAlive, KeepAlive1], StateData),
            {keep_state, StateData#state{keepalive = KeepAlive1}};
        {error, timeout} ->
            case StateName of
                asleep ->
                    % ignore keepalive timeout in asleep
                    ?LOG(debug, "Keepalive timeout, ignore it in asleep", [], StateData),
                    {keep_state, StateData#state{keepalive = undefined}};
                Other ->
                    ?LOG(debug, "Keepalive timeout in ~p", [Other], StateData),
                    shutdown(keepalive_timeout, StateData)
            end;
        {error, Error}   ->
            ?LOG(warning, "Keepalive error - ~p", [Error], StateData),
            shutdown(Error, StateData)
    end;

handle_event(info, do_awake_jobs, StateName, StateData=#state{client_id = ClientId}) ->
    ?LOG(debug, "Do awake jobs, statename : ~p", [StateName], StateData),
    NewStateData = process_awake_jobs(ClientId, StateData),
    case StateName of
        awake  -> goto_asleep_state(NewStateData, undefined);
        _Other -> {keep_state, NewStateData} %% device send a CONNECT immediately before this do_awake_jobs is handled
    end;

%% handle_event(info, {subscribe, [Topics]}, _StateName, StateData) ->
%%     ?LOG(debug, "Ignore subscribe Topics: ~p", [Topics], StateData),
%%     {keep_state, StateData};

%% %% Asynchronous SUBACK
%% handle_event(info, {suback, MsgId, [GrantedQoS]}, _StateName,
%%              StateData = #state{awaiting_suback = Awaiting}) ->
%%     Flags = #mqtt_sn_flags{qos = GrantedQoS},
%%     {MsgId, TopicId} = find_suback_topicid(MsgId, Awaiting),
%%     ?LOG(debug, "suback Awaiting=~p, MsgId=~p, TopicId=~p", [Awaiting, MsgId, TopicId], StateData),
%%     send_message(?SN_SUBACK_MSG(Flags, TopicId, MsgId, ?SN_RC_ACCEPTED), StateData),
%%     {keep_state, StateData#state{awaiting_suback = lists:delete({MsgId, TopicId}, Awaiting)}};

handle_event(info, {asleep_timeout, Ref}, StateName, StateData=#state{asleep_timer = AsleepTimer}) ->
    ?LOG(debug, "asleep_timeout at ~p", [StateName], StateData),
    case emqx_sn_asleep_timer:timeout(AsleepTimer, StateName, Ref) of
        terminate_process         -> stop(asleep_timeout, StateData);
        {restart_timer, NewTimer} -> goto_asleep_state(StateData#state{asleep_timer = NewTimer}, undefined);
        {stop_timer, NewTimer}    -> {keep_state, StateData#state{asleep_timer = NewTimer}}
    end;

handle_event({call, From}, kick, _StateName, StateData = #state{client_id = ClientId}) ->
    ?LOG(warning, "Clientid '~s' will be kicked out", [ClientId], StateData),
    {stop_and_reply, kick, [{reply, From, ok}], StateData};

handle_event(info, {shutdown, conflict, {ClientId, NewPid}}, _StateName, StateData) ->
    ?LOG(warning, "Clientid '~s' conflict with ~p", [ClientId, NewPid], StateData),
    stop({shutdown, conflict}, StateData);

handle_event(EventType, EventContent, StateName, StateData) ->
    ?LOG(error, "StateName: ~s, Unexpected Event: ~p",
         [StateName, {EventType, EventContent}], StateData),
    {keep_state, StateData}.

terminate(Reason, _StateName, #state{client_id = ClientId,
                                     keepalive = Keepalive,
                                     chan_state  = ChanState}) ->
    if
        Keepalive =:= undefined -> ok;
        true -> emqx_sn_keepalive:cancel(Keepalive)
    end,
    emqx_sn_registry:unregister_topic(ClientId),
    case {ChanState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} ->
            emqx_channel:terminate(Error, ChanState);
        {_, Reason} ->
            emqx_channel:terminate(Reason, ChanState)
    end.

code_change(_Vsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

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
    ClientId = get(client_id),
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

send_pingresp(StateData) ->
    send_message(?SN_PINGRESP_MSG(), StateData).

send_connack(StateData) ->
    send_message(?SN_CONNACK_MSG(?SN_RC_ACCEPTED), StateData).

send_message(Msg = #mqtt_sn_message{type = Type}, StateData = #state{sockpid = SockPid, peer = Peer}) ->
    ?LOG(debug, "SEND ~s~n", [emqx_sn_frame:format(Msg)], StateData),
    inc_outgoing_stats(Type),
    Data = emqx_sn_frame:serialize(Msg),
    ok = emqx_metrics:inc('bytes.sent', iolist_size(Data)),
    SockPid ! {datagram, Peer, Data},
    ok.

goto_asleep_state(StateData=#state{asleep_timer = AsleepTimer}, Duration) ->
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
    ConnPkt = #mqtt_packet_connect{client_id   = ClientId,
                                   clean_start = CleanStart,
                                   username    = Username,
                                   password    = Password,
                                   keepalive   = Duration},
    put(client_id, ClientId),
    case WillFlag of
        true  ->
            send_message(?SN_WILLTOPICREQ_MSG(), StateData),
            {next_state, wait_for_will_topic, StateData#state{connpkt = ConnPkt, client_id = ClientId, keepalive_interval = Duration}};
        false ->
            SuccFun = fun(NStateData = #state{chan_state = NChanState}) ->
                              NStateData1 =
                                  NStateData#state{client_id = ClientId,
                                                   keepalive_interval = Duration,
                                                   chan_state = NChanState},
                              {next_state, connected, NStateData1}
                      end,
            handle_incoming(?CONNECT_PACKET(ConnPkt), SuccFun, StateData)
    end.

do_2nd_connect(Flags, Duration, ClientId, StateData = #state{client_id = OldClientId, chan_state = Channel}) ->
    emqx_channel:terminate(normal, Channel),
    emqx_sn_registry:unregister_topic(OldClientId),
    Channel1 = channel_init(StateData),
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    do_connect(ClientId, CleanStart, Will, Duration, StateData#state{chan_state = Channel1}).

do_subscribe(?SN_NORMAL_TOPIC, TopicId, QoS, MsgId, StateData=#state{client_id = ClientId}) ->
    case emqx_sn_registry:register_topic(ClientId, TopicId)of
        {error, too_large} ->
            send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS}, ?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            {next_state, connected, StateData};
        {error, wildcard_topic} ->
            proto_subscribe(TopicId, QoS, MsgId, ?SN_INVALID_TOPIC_ID, StateData);
        NewTopicId when is_integer(NewTopicId) ->
            proto_subscribe(TopicId, QoS, MsgId, NewTopicId, StateData)
    end;
do_subscribe(?SN_PREDEFINED_TOPIC, TopicId, QoS, MsgId, StateData = #state{client_id = ClientId}) ->
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS}, TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            {next_state, connected, StateData};
        PredefinedTopic ->
            proto_subscribe(PredefinedTopic, QoS, MsgId, TopicId, StateData)
    end;
do_subscribe(?SN_SHORT_TOPIC, TopicId, QoS, MsgId, StateData) ->
    TopicName = case is_binary(TopicId) of
                    true  -> TopicId;
                    false -> <<TopicId:16>>
                end,
    proto_subscribe(TopicName, QoS, MsgId, ?SN_INVALID_TOPIC_ID, StateData);
do_subscribe(_, _TopicId, QoS, MsgId, StateData) ->
    send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                ?SN_INVALID_TOPIC_ID, MsgId,
                                ?SN_RC_INVALID_TOPIC_ID), StateData),
    {keep_state, StateData}.

do_unsubscribe(?SN_NORMAL_TOPIC, TopicId, MsgId, StateData) ->
    proto_unsubscribe(TopicId, MsgId, StateData);

do_unsubscribe(?SN_PREDEFINED_TOPIC, TopicId, MsgId, StateData = #state{client_id = ClientId}) ->
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            send_message(?SN_UNSUBACK_MSG(MsgId), StateData),
            {keep_state, StateData};
        PredefinedTopic ->
            proto_unsubscribe(PredefinedTopic, MsgId, StateData)
    end;
do_unsubscribe(?SN_SHORT_TOPIC, TopicId, MsgId, StateData) ->
    TopicName = case is_binary(TopicId) of
                    true  -> TopicId;
                    false -> <<TopicId:16>>
                end,
    proto_unsubscribe(TopicName, MsgId, StateData);
do_unsubscribe(_, _TopicId, MsgId, StateData) ->
    send_message(?SN_UNSUBACK_MSG(MsgId), StateData),
    {keep_state, StateData}.

do_publish(?SN_NORMAL_TOPIC, TopicId, Data, Flags, MsgId, StateData) ->
    %% Handle normal topic id as predefined topic id, to be compatible with paho mqtt-sn library
    do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId, StateData);
do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId, StateData=#state{client_id = ClientId}) ->
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
do_publish_will(StateData = #state{will_msg = WillMsg}) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic, payload = Payload} = WillMsg,
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = false,
                                                          qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = Topic, packet_id = 1000},
                           payload  = Payload},
    handle_incoming(Publish, fun keep_state/1, StateData),
    %% 1000?
    QoS =:= ?QOS_2 andalso handle_incoming(?PUBREL_PACKET(1000), fun keep_state/1, StateData).

do_puback(TopicId, MsgId, ReturnCode, _StateName, StateData=#state{client_id = ClientId}) ->
    case ReturnCode of
        ?SN_RC_ACCEPTED ->
            handle_incoming(?PUBACK_PACKET(MsgId), fun keep_state/1, StateData);
        ?SN_RC_INVALID_TOPIC_ID ->
            case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
                undefined -> ok;
                TopicName ->
                    %%notice that this TopicName maybe normal or predefined,
                    %% involving the predefined topic name in register to enhance the gateway's robustness even inconsistent with MQTT-SN chan_states
                    send_register(TopicName, TopicId, MsgId, StateData),
                    {keep_state, StateData}
            end;
        _ ->
            ?LOG(error, "CAN NOT handle PUBACK ReturnCode=~p", [ReturnCode], StateData),
            {keep_state, StateData}
    end.

do_pubrec(PubRec, MsgId, StateData) ->
    handle_incoming(mqttsn_to_mqtt(PubRec, MsgId), fun keep_state/1, StateData).

channel_init(#state{peer = Peername}) ->
    emqx_channel:init(#{sockname => element(1, hd(element(2, inet:getif()))),
                        peername => Peername,
                        peercert => ?NO_PEERCERT,
                        conn_mod => ?MODULE}, ?DEFAULT_CHAN_OPTIONS).

proto_subscribe(TopicName, QoS, MsgId, TopicId,
                StateData = #state{awaiting_suback = Awaiting}) ->
    ?LOG(debug, "subscribe Topic=~p, MsgId=~p, TopicId=~p", [TopicName, MsgId, TopicId], StateData),
    enqueue_msgid(suback, MsgId, TopicId),
    NewAwaiting = lists:append(Awaiting, [{MsgId, TopicId}]),
    SuccFun = fun(NStateData) -> keep_state(NStateData#state{awaiting_suback = NewAwaiting}) end,
    SubPacket = ?SUBSCRIBE_PACKET(MsgId, [{TopicName, #{nl  => 0, qos => QoS, rap => 0,
                                                        rc  => 0, rh  => 0}}]),
    handle_incoming(SubPacket, SuccFun, StateData).

proto_unsubscribe(TopicName, MsgId, StateData) ->
    ?LOG(debug, "unsubscribe Topic=~p, MsgId=~p", [TopicName, MsgId], StateData),
    handle_incoming(?UNSUBSCRIBE_PACKET(MsgId, [TopicName]), fun keep_state/1, StateData).

proto_publish(TopicName, Data, Dup, QoS, Retain, MsgId, TopicId, StateData) ->
    (QoS =/= ?QOS_0) andalso enqueue_msgid(puback, MsgId, TopicId),
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId},
                           payload  = Data},
    ?LOG(debug, "[publish] Msg: ~p~n", [Publish], StateData),
    handle_incoming(Publish, fun keep_state/1, StateData).

send_message_to_device([], _ClientId, #state{chan_state = ChanState}) ->
    {ok, ChanState};
send_message_to_device([PubMsg | LeftMsgs], ClientId, StateData) ->
    send_message_to_device(PubMsg, ClientId, StateData),
    send_message_to_device(LeftMsgs, ClientId, StateData);
send_message_to_device(Deliver = {deliver, _Topic, _Msgs}, ClientId, StateData = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_out({deliver, emqx_misc:drain_deliver([Deliver])}, ChanState) of
        {ok, NChanState} ->
            keep_state(StateData#state{chan_state = NChanState});
        {ok, Packets, NChanState} when is_list(Packets) ->
            NState = StateData#state{chan_state = NChanState},
            lists:foldl(fun(PubPkt, {ok, NChanState1}) ->
                            {ok, NChanState2} =
                                send_message_to_device({publish, PubPkt}, ClientId, NState#state{chan_state = NChanState1}),
                            {ok, NChanState2}
                        end, {ok, NChanState}, Packets);
        {ok, Packet, NChanState} ->
            NState = StateData#state{chan_state = NChanState},
            send_message_to_device({publish, Packet}, ClientId, NState#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, StateData#state{chan_state = NChanState})
    end;
send_message_to_device({suback, PacketId, ReasonCodes}, _ClientId, StateData) ->
    ?LOG(debug, "[suback] msgid: ~p, reason codes: ~p", [PacketId, ReasonCodes], StateData),
    SubackPkt = ?SUBACK_PACKET(PacketId, [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes]),
    handle_outgoing(SubackPkt, fun msg2device_succfun/1, StateData);
send_message_to_device({unsuback, PacketId, _ReasonCodes}, _ClientId, StateData) ->
    ?LOG(debug, "[unsuback] msgid: ~p", [PacketId], StateData),
    handle_outgoing(?UNSUBACK_PACKET(PacketId), fun msg2device_succfun/1, StateData);
send_message_to_device({publish, PubPkt}, ClientId, StateData) ->
    #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = QoS, retain = Retain},
                          variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId0},
                          payload  = Payload} = PubPkt,
    MsgId = message_id(MsgId0),
    ?LOG(debug, "The TopicName of mqtt_message=~p~n", [TopicName], StateData),
    case emqx_sn_registry:lookup_topic_id(ClientId, TopicName) of
        undefined ->
            case byte_size(TopicName) of
                2 -> handle_outgoing(PubPkt, fun msg2device_succfun/1, StateData);
                _ -> register_and_notify_client(TopicName, Payload, Dup, QoS,
                                                Retain, MsgId, ClientId, StateData),
                     handle_outgoing(PubPkt, fun msg2device_succfun/1, StateData)
            end;
        _ ->
            handle_outgoing(PubPkt, fun msg2device_succfun/1, StateData)
    end.

publish_asleep_messages_to_device(ClientId, StateData = #state{asleep_msg_queue = AsleepMsgQueue}, QoS2Count) ->

    case queue:is_empty(AsleepMsgQueue) of
        false ->
            Msg = queue:get(AsleepMsgQueue),
            {ok, NChanState} = send_message_to_device(Msg, ClientId, StateData),
            NewCount = case is_qos2_msg(Msg) of
                           true -> QoS2Count + 1;
                           false -> QoS2Count
                       end,
            publish_asleep_messages_to_device(
              ClientId, StateData#state{asleep_msg_queue = queue:drop(AsleepMsgQueue),
                                        chan_state = NChanState
                                       }, NewCount);
        true  ->
            {QoS2Count, StateData}
    end.

register_and_notify_client(TopicName, Payload, Dup, QoS, Retain, MsgId, ClientId, StateData) ->
    TopicId = emqx_sn_registry:register_topic(ClientId, TopicName),
    ?LOG(debug, "register TopicId=~p, TopicName=~p, Payload=~p, Dup=~p, QoS=~p, Retain=~p, MsgId=~p",
        [TopicId, TopicName, Payload, Dup, QoS, Retain, MsgId], StateData),
    send_register(TopicName, TopicId, MsgId, StateData).

message_id(undefined) ->
    rand:uniform(16#FFFF);
message_id(MsgId) ->
    MsgId.

update_will_topic(undefined, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic};
update_will_topic(Will=#will_msg{}, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    Will#will_msg{qos = QoS, retain = Retain, topic = Topic}.

update_will_msg(undefined, Msg) ->
    #will_msg{payload = Msg};
update_will_msg(Will = #will_msg{}, Msg) ->
    Will#will_msg{payload = Msg}.

process_awake_jobs(ClientId, StateData) ->
    {_, NewStateData} = publish_asleep_messages_to_device(ClientId, StateData, 0),
    %% TODO: what about publishing qos2 messages? wait more time for pubrec & pubcomp from device?
    %%       or ask device to go connected state?
    send_pingresp(StateData),
    NewStateData.

ensure_stats_timer(State = #state{enable_stats = true,
                                  stats_timer  = undefined}) ->
    State#state{stats_timer = emqx_misc:start_timer(?STAT_TIMEOUT, emit_stats)};
ensure_stats_timer(State) -> State.

enqueue_msgid(suback, MsgId, TopicId) ->
    put({suback, MsgId}, TopicId);
enqueue_msgid(puback, MsgId, TopicId) ->
    put({puback, MsgId}, TopicId).

dequeue_msgid(suback, MsgId) ->
    erase({suback, MsgId});
dequeue_msgid(puback, MsgId) ->
    erase({puback, MsgId}).

is_qos2_msg({publish, _PacketId, #message{qos = 2}})->
    true;
is_qos2_msg(_)->
    false.

stats(#state{chan_state = ChanState}) ->
    ChanStats = [{Name, emqx_pd:get_counter(Name)} || Name <- ?GATEWAY_STATS],
    SessStats = emqx_session:stats(emqx_channel:info(session, ChanState)),
    lists:append([ChanStats, SessStats, emqx_misc:proc_stats()]).

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

handle_incoming(Packet = ?PACKET(Type), SuccFun, State = #state{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)], State),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#state{chan_state= NChanState});
        {ok, OutPackets, NChanState} ->
            handle_outgoing(OutPackets, SuccFun, State#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, OutPacket, NChanState} ->
            Shutdown = fun(NewSt) -> shutdown(Reason, NewSt) end,
            handle_outgoing(OutPacket, Shutdown, State#state{chan_state = NChanState})
    end.

handle_outgoing(Packets, SuccFun, State = #state{transformer = Transformer})
  when is_list(Packets) ->
    send(lists:map(Transformer, Packets), SuccFun, State);
handle_outgoing(Packet, SuccFun, State = #state{transformer = Transformer}) ->
    send(Transformer(Packet), SuccFun, State).

transformer_fun() ->
    FunMsgIdToTopicId = fun(Type, MsgId) -> dequeue_msgid(Type, MsgId) end,
    fun(Packet) -> transform(Packet, FunMsgIdToTopicId) end.

send([], SuccFun, State) ->
    SuccFun(State);
send([Msg | LeftMsgs], SuccFun, State) ->
    ok = send_message(Msg, State),
    send(LeftMsgs, SuccFun, State);
send(Msg, SuccFun, State) ->
    ok = send_message(Msg, State),
    SuccFun(State).

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    case Type == ?PUBLISH of
        true ->
            emqx_pd:update_counter(recv_msg, 1),
            emqx_pd:update_counter(incoming_pubs, 1);
        false -> ok
    end.

msg2device_succfun(NStateData) ->
    #state{chan_state = NChanState} = NStateData,
    {ok, NChanState}.

inc_outgoing_stats(Type) ->
    emqx_pd:update_counter(send_pkt, 1),
    (Type == ?SN_PUBLISH)
        andalso emqx_pd:update_counter(send_msg, 1).

keep_state(State) ->
    {keep_state, State}.
