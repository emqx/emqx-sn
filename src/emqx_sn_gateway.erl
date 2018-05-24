%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_sn_gateway).

-behaviour(gen_statem).

-include("emqx_sn.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%% API.
-export([start_link/3]).

%% SUB/UNSUB Asynchronously, called by plugins.
-export([subscribe/2, unsubscribe/2]).
-export([kick/1]).

%% state functions
-export([idle/3, wait_for_will_topic/3, wait_for_will_msg/3, connected/3,
         asleep/3, awake/3]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, handle_event/4, terminate/3, code_change/4]).

-record(will_msg, {retain = false  :: boolean(),
                   qos    = ?QOS_0 :: mqtt_qos(),
                   topic           :: binary() | undefined,
                   payload         :: binary() | undefined}).

-record(state, {gwid                 :: integer(),
                sock                 :: inet:socket(),
                peer                 :: {inet:ip_address(), inet:port()},
                protocol             :: term(),
                client_id            :: binary(),
                will_msg             :: #will_msg{},
                keepalive_interval   :: integer(),
                keepalive            :: emqx_keepalive:keepalive() | undefined,
                connpkt              :: term(),
                awaiting_suback = [] :: list(),
                asleep_timer         :: tuple(),
                asleep_msg_queue     :: term(),
                enable_stats         :: boolean(),
                enable_qos3 = false  :: boolean()}).

-define(IDLE_TIMEOUT, 10000).
-define(DEFAULT_PROTO_OPTIONS, [{max_clientid_len, 24}, {max_packet_size, 256}]).
-define(LOG(Level, Format, Args, State),
        emqx_logger:Level("MQTT-SN(~s): " ++ Format,
                          [esockd_net:format(State#state.peer) | Args])).

-ifdef(TEST).
-define(PROTO_INIT(A, B, C),            test_mqtt_broker:proto_init(A, B, C)).
-define(PROTO_RECEIVE(A, B),            test_mqtt_broker:proto_receive(A, B)).
-define(PROTO_SHUTDOWN(A, B),           ok).
-define(PROTO_STATS(A),                 test_mqtt_broker:stats(A)).
-define(PROTO_GET_CLIENT_ID(A),         test_mqtt_broker:clientid(A)).
-define(SET_CLIENT_STATS(A,B),          test_mqtt_broker:set_client_stats(A,B)).
-define(PROTO_SEND(A, B),               test_mqtt_broker:send(A, B)).
-else.
-define(PROTO_INIT(A, B, C),            emqx_protocol:init(A, B, C)).
-define(PROTO_RECEIVE(A, B),            emqx_protocol:received(A, B)).
-define(PROTO_SHUTDOWN(A, B),           emqx_protocol:shutdown(A, B)).
-define(PROTO_STATS(A),                 emqx_protocol:stats(A)).
-define(PROTO_GET_CLIENT_ID(A),         emqx_protocol:clientid(A)).
-define(PROTO_SEND(A, B),               emqx_protocol:send(A, B)).
-define(SET_CLIENT_STATS(A,B),          emqx_stats:set_client_stats(A,B)).
-endif.

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).
-define(NEG_QOS_CLIENT_ID, <<"NegQos-Client">>).

%%--------------------------------------------------------------------
%% Exported APIs
%%--------------------------------------------------------------------

-spec(start_link(inet:socket(), {inet:ip_address(), inet:port()}, pos_integer())
      -> {ok, pid()} | {error, term()}).
start_link(Sock, Peer, GwId) ->
    gen_statem:start_link(?MODULE, [Sock, Peer, GwId], []).

subscribe(GwPid, TopicTable) ->
    gen_statem:cast(GwPid, {subscribe, TopicTable}).

unsubscribe(GwPid, Topics) ->
    gen_statem:cast(GwPid, {unsubscribe, Topics}).

kick(GwPid) ->
    gen_statem:call(GwPid, kick).

%%--------------------------------------------------------------------
%% gen_fsm Callbacks
%%--------------------------------------------------------------------

init([Sock, Peer, GwId]) ->
    EnableStats = emqx_sn_config:get_env(enable_stats, false),
    State = #state{gwid             = GwId,
                   sock             = Sock,
                   peer             = Peer,
                   asleep_timer     = emqx_sn_asleep_timer:init(),
                   asleep_msg_queue = queue:new(),
                   enable_stats     = EnableStats,
                   enable_qos3      = emqx_sn_config:get_env(enable_qos3, false)},
    {ok, idle, State#state{protocol = proto_init(State)}}.%%, ?IDLE_TIMEOUT}.

callback_mode() -> state_functions.

idle(cast, ?SN_SEARCHGW_MSG(_Radius), StateData = #state{gwid = GwId}) ->
    send_message(?SN_GWINFO_MSG(GwId, <<>>), StateData),
    {keep_state, StateData, ?IDLE_TIMEOUT};

idle(cast, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId), StateData) ->
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    do_connect(ClientId, CleanStart, Will, Duration, StateData);

idle(cast, ?SN_ADVERTISE_MSG(_GwId, _Radius), StateData) ->
    % ignore
    {keep_state, StateData, ?IDLE_TIMEOUT};

idle(cast, ?SN_DISCONNECT_MSG(_Duration), StateData) ->
    % ignore
    {keep_state, StateData, ?IDLE_TIMEOUT};

idle(cast, ?SN_PUBLISH_MSG(_Flag, _TopicId, _MsgId, _Data), StateData = #state{enable_qos3 = false}) ->
    ?LOG(debug, "The enable_qos3 is false, ignore the received publish with Qos=-1 in idle mode!", [], StateData),
    {keep_state_and_data, ?IDLE_TIMEOUT};

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
    ?LOG(debug, "Client id=~p receives a publish with Qos=-1 in idle mode!", [ClientId], StateData),
    {keep_state_and_data, ?IDLE_TIMEOUT};

idle(timeout, _Timeout, StateData) ->
    shutdown(idle_timeout, StateData);

idle(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, idle, State).

wait_for_will_topic(cast, ?SN_WILLTOPIC_EMPTY_MSG,
                    StateData = #state{connpkt = ConnPkt, protocol = Proto}) ->
    % empty willtopic means deleting will
    case ?PROTO_RECEIVE(?CONNECT_PACKET(ConnPkt), Proto) of
        {ok, Proto1}           -> {next_state, connected, StateData#state{protocol = Proto1, will_msg = undefined}};
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

wait_for_will_topic(cast, ?SN_WILLTOPIC_MSG(Flags, Topic), StateData) ->
    #mqtt_sn_flags{qos = Qos, retain = Retain} = Flags,
    WillMsg = #will_msg{retain = Retain, qos = Qos, topic = Topic},
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

wait_for_will_msg(cast, ?SN_WILLMSG_MSG(Payload),
                  StateData = #state{protocol = Proto, will_msg = WillMsg, connpkt = ConnPkt}) ->
    case ?PROTO_RECEIVE(?CONNECT_PACKET(ConnPkt), Proto) of
        {ok, Proto1} ->
            {next_state, connected, StateData#state{protocol = Proto1,
                                                    will_msg = WillMsg#will_msg{payload = Payload}}};
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

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
            send_message(?SN_REGACK_MSG(TopicId, MsgId, ?SN_RC_ACCECPTED), StateData);
        {error, too_large} ->
            ?LOG(error, "TopicId is full! ClientId=~p, TopicName=~p", [ClientId, TopicName], StateData),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), StateData);
        {error, wildcard_topic} ->
            ?LOG(error, "wildcard topic can not be registered! ClientId=~p, TopicName=~p", [ClientId, TopicName], StateData),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), StateData)
    end,
    {keep_state, StateData};

connected(cast, ?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data),
          StateData = #state{enable_qos3 = EnableQos3}) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType, qos = Qos} = Flags,
    Skip = (EnableQos3 =:= false) andalso (Qos =:= ?QOS_NEG1),
    case Skip of
        true  ->
            ?LOG(debug, "The enable_qos3 is false, ignore the received publish with Qos=-1 in connected mode!", [], StateData),
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
    #mqtt_sn_flags{qos = Qos, topic_id_type = TopicIdType} = Flags,
    do_subscribe(TopicIdType, TopicId, Qos, MsgId, StateData);

connected(cast, ?SN_UNSUBSCRIBE_MSG(Flags, MsgId, TopicId), StateData) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    do_unsubscribe(TopicIdType, TopicId, MsgId, StateData);

connected(cast, ?SN_PINGREQ_MSG(_ClientId), StateData) ->
    send_message(?SN_PINGRESP_MSG(), StateData),
    {keep_state, StateData};

connected(cast, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCECPTED), StateData) ->
    {keep_state, StateData};
connected(cast, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode), StateData) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p", [TopicId, MsgId, ReturnCode], StateData),
    {keep_state, StateData};

connected(cast, ?SN_DISCONNECT_MSG(Duration), StateData = #state{protocol = Proto}) ->
    send_message(?SN_DISCONNECT_MSG(undefined), StateData),
    case Duration of
        undefined ->
            {stop, Reason, Proto1} = ?PROTO_RECEIVE(?PACKET(?DISCONNECT), Proto),
            stop(Reason, StateData#state{protocol = Proto1});
        Other -> goto_asleep_state(StateData, Other)
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

asleep(cast, ?SN_DISCONNECT_MSG(Duration), StateData = #state{protocol = Proto}) ->
    send_message(?SN_DISCONNECT_MSG(undefined), StateData),
    case Duration of
        undefined ->
            {stop, Reason, Proto1} = ?PROTO_RECEIVE(?PACKET(?DISCONNECT), Proto),
            stop(Reason, StateData#state{protocol = Proto1});
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
% this should be a bug in mqtt-sn protocol.
asleep(cast, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId),
       StateData = #state{keepalive_interval = Interval}) ->
    % device wakeup and goto connected state
    % keepalive timer may timeout in asleep state and delete itself, need to restart keepalive
    self() ! {keepalive, start, Interval},
    send_connack(StateData),
    {next_state, connected, StateData};

asleep(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, asleep, StateData).

awake(cast, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCECPTED), StateData) ->
    {keep_state, StateData};

awake(cast, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode), StateData) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode], StateData),
    {keep_state, StateData};

awake(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, awake, StateData).

handle_event(info, {datagram, _From, Data}, StateName, StateData) ->
    case catch emqx_sn_frame:parse(Data) of
        {ok, Msg} ->
            ?LOG(info, "RECV ~p at state ~s",
                 [emqx_sn_frame:format(Msg), StateName], StateData),
            gen_statem:cast(self(), Msg),
            {keep_state, StateData};
        {'EXIT', Error} ->
            ?LOG(info, "Parse frame error: ~p at state ~s",
                 [Error, StateName], StateData),
           shutdown(frame_error, StateData)
    end;

handle_event(info, {deliver, Msg}, asleep,
             StateData = #state{asleep_msg_queue = AsleepMsgQue}) ->
    % section 6.14, Support of sleeping clients
    ?LOG(debug, "enqueue downlink message in asleep state Msg=~p", [Msg], StateData),
    NewAsleepMsgQue = queue:in(Msg, AsleepMsgQue),
    {keep_state, StateData#state{asleep_msg_queue = NewAsleepMsgQue}};

handle_event(info, {deliver, Msg}, _StateName,
             StateData = #state{client_id = ClientId}) ->
    {ok, ProtoState} = publish_message_to_device(Msg, ClientId, StateData),
    {keep_state, StateData#state{protocol = ProtoState}};

handle_event(info, {redeliver, {?PUBREL, MsgId}}, _StateName, StateData) ->
    send_message(?SN_PUBREC_MSG(?SN_PUBREL, MsgId), StateData),
    {keep_state, StateData};

handle_event(info, {keepalive, start, Interval}, _StateName,
             StateData = #state{sock = Sock, keepalive = undefined}) ->
    ?LOG(debug, "Keepalive at the interval of ~p seconds", [Interval], StateData),
    emit_stats(StateData),
    StatFun = fun() ->
                  case inet:getstat(Sock, [recv_oct]) of
                      {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                      {error, Reason}             -> {error, Reason}
                  end
              end,
    case emqx_keepalive:start(StatFun, Interval, {keepalive, check}) of
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
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            ?LOG(debug, "Keepalive check ok StateName=~p, KeepAlive=~p",
                 [StateName, KeepAlive], StateData),
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
    NewStateData = process_awake_jobs(ClientId, StateData),
    case StateName of
        awake  -> goto_asleep_state(NewStateData, undefined);
        _Other -> {keep_state, NewStateData} %% device send a CONNECT immediately before this do_awake_jobs is handled
    end;

handle_event(info, {subscribe, [Topics]}, _StateName, StateData) ->
    ?LOG(debug, "Ignore subscribe Topics: ~p", [Topics], StateData),
    {keep_state, StateData};

%% Asynchronous SUBACK
handle_event(info, {suback, MsgId, [GrantedQos]}, _StateName,
             StateData = #state{awaiting_suback = Awaiting}) ->
    Flags = #mqtt_sn_flags{qos = GrantedQos},
    {MsgId, TopicId} = find_suback_topicid(MsgId, Awaiting),
    ?LOG(debug, "suback Awaiting=~p, MsgId=~p, TopicId=~p", [Awaiting, MsgId, TopicId], StateData),
    send_message(?SN_SUBACK_MSG(Flags, TopicId, MsgId, ?SN_RC_ACCECPTED), StateData),
    {keep_state, StateData#state{awaiting_suback = lists:delete({MsgId, TopicId}, Awaiting)}};

handle_event(info, {asleep_timeout, Ref}, StateName, StateData=#state{asleep_timer = AsleepTimer}) ->
    ?LOG(debug, "asleep_timeout at ~p", [StateName], StateData),
    case emqx_sn_asleep_timer:timeout(AsleepTimer, StateName, Ref) of
        terminate_process         -> stop(asleep_timeout, StateData);
        {restart_timer, NewTimer} -> goto_asleep_state(StateData#state{asleep_timer = NewTimer}, undefined);
        {stop_timer, NewTimer}    -> {keep_state, StateData#state{asleep_timer = NewTimer}}
    end;

handle_event(info, emit_stats, _StateName, StateData) ->
    emit_stats(StateData),
    {keep_state, StateData};

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
                                     protocol  = Proto}) ->
    emqx_keepalive:cancel(Keepalive),
    emqx_sn_registry:unregister_topic(ClientId),
    case {Proto, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} ->
            ?PROTO_SHUTDOWN(Error, Proto);
        {_, Reason} ->
            ?PROTO_SHUTDOWN(Reason, Proto)
    end.

code_change(_Vsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

sock_stats(Sock, Stats) when is_port(Sock) ->
    inet:getstat(Sock, Stats).

transform(?CONNACK_PACKET(0), _FuncMsgIdToTopicId) ->
    ?SN_CONNACK_MSG(0);

transform(?CONNACK_PACKET(_ReturnCode), _FuncMsgIdToTopicId) ->
    ?SN_CONNACK_MSG(?SN_RC_CONGESTION);

transform(?PUBLISH_PACKET(Qos, Topic, PacketId, Payload), _FuncMsgIdToTopicId) ->
    NewPacketId =   if
                        Qos =:= ?QOS_0 -> 0;
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
    Flags = #mqtt_sn_flags{qos = Qos, topic_id_type = TopicIdType},
    ?SN_PUBLISH_MSG(Flags, TopicContent, NewPacketId, Payload);

transform(?PUBACK_PACKET(MsgId), FuncMsgIdToTopicId) ->
    TopicIdFinal =  case FuncMsgIdToTopicId(MsgId) of
                        undefined -> 0;
                        TopicId -> TopicId
                    end,
    ?SN_PUBACK_MSG(TopicIdFinal, MsgId, ?SN_RC_ACCECPTED);

transform(?PUBREC_PACKET(MsgId), _FuncMsgIdToTopicId) ->
    ?SN_PUBREC_MSG(?SN_PUBREC, MsgId);

transform(?PUBREL_PACKET(MsgId), _FuncMsgIdToTopicId) ->
    ?SN_PUBREC_MSG(?SN_PUBREL, MsgId);

transform(?PUBCOMP_PACKET(MsgId), _FuncMsgIdToTopicId) ->
    ?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId);

transform(?SUBACK_PACKET(MsgId, _QosTable), _FuncMsgIdToTopicId)->
    % if success, suback is sent by handle_info({suback, MsgId, [GrantedQos]}, ...)
    % if failure, suback is sent in this function.
    Flags = #mqtt_sn_flags{qos = 0},
    ?SN_SUBACK_MSG(Flags, ?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_MQTT_FAILURE);

transform(?UNSUBACK_PACKET(MsgId), _FuncMsgIdToTopicId)->
    ?SN_UNSUBACK_MSG(MsgId).

send_register(TopicName, TopicId, MsgId, StateData) ->
    send_message(?SN_REGISTER_MSG(TopicId, MsgId, TopicName), StateData).

send_pingresp(StateData) ->
    send_message(?SN_PINGRESP_MSG(), StateData).

send_connack(StateData) ->
    send_message(?SN_CONNACK_MSG(?SN_RC_ACCECPTED), StateData).

send_message(Msg, StateData = #state{sock = Sock, peer = {Host, Port}}) ->
    ?LOG(debug, "SEND ~p~n", [emqx_sn_frame:format(Msg)], StateData),
    gen_udp:send(Sock, Host, Port, emqx_sn_frame:serialize(Msg)).

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

do_connect(ClientId, CleanStart, WillFlag, Duration, StateData = #state{protocol = Proto}) ->
    Username = emqx_sn_config:get_env(username),
    Password = emqx_sn_config:get_env(password),
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
            case ?PROTO_RECEIVE(?CONNECT_PACKET(ConnPkt), Proto) of
                {ok, Proto1}           -> {next_state, connected, StateData#state{client_id = ClientId, protocol = Proto1, keepalive_interval = Duration}};
                {error, Error}         -> shutdown(Error, StateData);
                {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
                {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
            end
    end.

do_2nd_connect(Flags, Duration, ClientId, StateData = #state{client_id = OldClientId, protocol = Proto}) ->
    ?PROTO_SHUTDOWN(normal, Proto),
    emqx_sn_registry:unregister_topic(OldClientId),
    NewProto = proto_init(StateData),
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    do_connect(ClientId, CleanStart, Will, Duration, StateData#state{protocol = NewProto}).

do_subscribe(?SN_NORMAL_TOPIC, TopicId, Qos, MsgId, StateData=#state{client_id = ClientId}) ->
    case emqx_sn_registry:register_topic(ClientId, TopicId)of
        {error, too_large} ->
            send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = Qos}, ?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            {next_state, connected, StateData};
        {error, wildcard_topic} ->
            proto_subscribe(TopicId, Qos, MsgId, ?SN_INVALID_TOPIC_ID, StateData);
        NewTopicId when is_integer(NewTopicId) ->
            proto_subscribe(TopicId, Qos, MsgId, NewTopicId, StateData)
    end;
do_subscribe(?SN_PREDEFINED_TOPIC, TopicId, Qos, MsgId, StateData = #state{client_id = ClientId}) ->
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = Qos}, TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            {next_state, connected, StateData};
        PredefinedTopic ->
            proto_subscribe(PredefinedTopic, Qos, MsgId, TopicId, StateData)
    end;
do_subscribe(?SN_SHORT_TOPIC, TopicId, Qos, MsgId, StateData) ->
    TopicName = case is_binary(TopicId) of
                    true  -> TopicId;
                    false -> <<TopicId:16>>
                end,
    proto_subscribe(TopicName, Qos, MsgId, ?SN_INVALID_TOPIC_ID, StateData);
do_subscribe(_, _TopicId, Qos, MsgId, StateData) ->
    send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = Qos},
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
    #mqtt_sn_flags{qos = Qos, dup = Dup, retain = Retain} = Flags,
    NewQos = get_corrected_qos(Qos, StateData),
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            (NewQos =/= ?QOS0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData),
            {keep_state, StateData};
        TopicName ->
            proto_publish(TopicName, Data, Dup, NewQos, Retain, MsgId, TopicId, StateData)
    end;
do_publish(?SN_SHORT_TOPIC, TopicId, Data, Flags, MsgId, StateData) ->
    #mqtt_sn_flags{qos = Qos, dup = Dup, retain = Retain} = Flags,
    NewQos = get_corrected_qos(Qos, StateData),
    TopicName = <<TopicId:16>>,
    case emqx_topic:wildcard(TopicName) of
        true ->
            (NewQos =/= ?QOS0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_NOT_SUPPORTED), StateData),
            {keep_state, StateData};
        false ->
            proto_publish(TopicName, Data, Dup, NewQos, Retain, MsgId, TopicId, StateData)
    end;
do_publish(_, TopicId, _Data, #mqtt_sn_flags{qos = Qos}, MsgId, StateData) ->
    (Qos =/= ?QOS0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_NOT_SUPPORTED), StateData),
    {keep_state, StateData}.

do_publish_will(#state{will_msg = undefined}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{payload = undefined}}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{topic = undefined}}) ->
    ok;
do_publish_will(#state{will_msg = WillMsg, protocol = Proto}) ->
    #will_msg{qos = Qos, retain = Retain, topic = Topic, payload = Payload} = WillMsg,
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = false,
                                                          qos = Qos, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = Topic, packet_id = 1000},
                           payload  = Payload},
    ?PROTO_RECEIVE(Publish, Proto),
    %% 1000?
    Qos =:= ?QOS2 andalso ?PROTO_RECEIVE(?PUBREL_PACKET(1000), Proto).

do_puback(TopicId, MsgId, ReturnCode, _StateName, StateData=#state{client_id = ClientId, protocol = Proto}) ->
    case ReturnCode of
        ?SN_RC_ACCECPTED ->
            case ?PROTO_RECEIVE(?PUBACK_PACKET(MsgId), Proto) of
                {ok, Proto1}           -> {keep_state, StateData#state{protocol = Proto1}};
                {error, Error}         -> shutdown(Error, StateData);
                {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
                {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
            end;
        ?SN_RC_INVALID_TOPIC_ID ->
            case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
                undefined -> ok;
                TopicName ->
                    %%notice that this TopicName maybe normal or predefined,
                    %% involving the predefined topic name in register to enhance the gateway's robustness even inconsistent with MQTT-SN protocols
                    send_register(TopicName, TopicId, MsgId, StateData),
                    {keep_state, StateData}
            end;
        _ ->
            ?LOG(error, "CAN NOT handle PUBACK ReturnCode=~p", [ReturnCode], StateData),
            {keep_state, StateData}
    end.

do_pubrec(PubRec, MsgId, StateData = #state{protocol = Proto}) ->
    case ?PROTO_RECEIVE(mqttsn_to_mqtt(PubRec, MsgId), Proto) of
        {ok, Proto1}           -> {keep_state, StateData#state{protocol = Proto1}};
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end.

proto_init(StateData = #state{peer = Peername, enable_stats = EnableStats}) ->
    SendFun = fun(Packet) ->
                  send_message(transform(Packet, fun(MsgId) ->
                                                     dequeue_puback_msgid(MsgId)
                                                 end), StateData)
              end,
    PktOpts = [{client_enable_stats, EnableStats} | ?DEFAULT_PROTO_OPTIONS],
    ?PROTO_INIT(Peername, SendFun, PktOpts).

proto_subscribe(TopicName, Qos, MsgId, TopicId,
                StateData = #state{protocol = Proto, awaiting_suback = Awaiting}) ->
    ?LOG(debug, "subscribe Topic=~p, MsgId=~p, TopicId=~p", [TopicName, MsgId, TopicId], StateData),
    NewAwaiting = lists:append(Awaiting, [{MsgId, TopicId}]),
    case ?PROTO_RECEIVE(?SUBSCRIBE_PACKET(MsgId, [{TopicName, Qos}]), Proto) of
        {ok, Proto1}           -> {keep_state, StateData#state{protocol = Proto1, awaiting_suback = NewAwaiting}};
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end.

proto_unsubscribe(TopicName, MsgId, StateData = #state{protocol = Proto}) ->
    ?LOG(debug, "unsubscribe Topic=~p, MsgId=~p", [TopicName, MsgId], StateData),
    case ?PROTO_RECEIVE(?UNSUBSCRIBE_PACKET(MsgId, [TopicName]), Proto) of
        {ok, Proto1}           -> {keep_state, StateData#state{protocol = Proto1}};
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end.

proto_publish(TopicName, Data, Dup, Qos, Retain, MsgId, TopicId,
              StateData = #state{protocol = Proto}) ->
    (Qos =/= ?QOS0) andalso enqueue_puback_msgid(TopicId, MsgId),
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = Qos, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId},
                           payload  = Data},
    case ?PROTO_RECEIVE(Publish, Proto) of
        {ok, Proto1}           -> {keep_state, StateData#state{protocol = Proto1}};
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

publish_message_to_device(Msg, ClientId, StateData = #state{protocol = ProtoState}) ->
    #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = Qos, retain = Retain},
                 variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId0},
                 payload  = Payload} = emqx_message:to_packet(Msg),
    MsgId = message_id(MsgId0),
    ?LOG(debug, "the TopicName of mqtt_message=~p~n", [TopicName], StateData),
    case emqx_sn_registry:lookup_topic_id(ClientId, TopicName) of
        undefined ->
            case byte_size(TopicName) of
                2 ->
                    ?PROTO_SEND(Msg, ProtoState);
                _ ->
                    register_and_notify_client(TopicName, Payload, Dup, Qos, Retain, MsgId, ClientId, StateData),
                    ?PROTO_SEND(Msg, ProtoState)
            end;
        _         ->
            ?PROTO_SEND(Msg, ProtoState)
    end.

publish_asleep_messages_to_device(ClientId, StateData = #state{asleep_msg_queue = AsleepMsgQueue}, Qos2Count) ->
    case queue:is_empty(AsleepMsgQueue) of
        false ->
            Msg = queue:get(AsleepMsgQueue),
            {ok, NewProtoState} = publish_message_to_device(Msg, ClientId, StateData),
            NewCount = case is_qos2_msg(Msg) of
                           true -> Qos2Count + 1;
                           false -> Qos2Count
                       end,
            publish_asleep_messages_to_device(ClientId, StateData#state{protocol = NewProtoState, asleep_msg_queue = queue:drop(AsleepMsgQueue)}, NewCount);
        true  ->
            {Qos2Count, StateData}
    end.

register_and_notify_client(TopicName, Payload, Dup, Qos, Retain, MsgId, ClientId, StateData) ->
    TopicId = emqx_sn_registry:register_topic(ClientId, TopicName),
    ?LOG(debug, "register TopicId=~p, TopicName=~p, Payload=~p, Dup=~p, Qos=~p, Retain=~p, MsgId=~p",
        [TopicId, TopicName, Payload, Dup, Qos, Retain, MsgId], StateData),
    send_register(TopicName, TopicId, MsgId, StateData).

message_id(undefined) ->
    rand:uniform(16#FFFF);
message_id(MsgId) ->
    MsgId.

update_will_topic(undefined, #mqtt_sn_flags{qos = Qos, retain = Retain}, Topic) ->
    #will_msg{qos = Qos, retain = Retain, topic = Topic};
update_will_topic(Will=#will_msg{}, #mqtt_sn_flags{qos = Qos, retain = Retain}, Topic) ->
    Will#will_msg{qos = Qos, retain = Retain, topic = Topic}.

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

enqueue_puback_msgid(TopicId, MsgId) ->
    put({puback_msgid, MsgId}, TopicId).

dequeue_puback_msgid(MsgId) ->
    erase({puback_msgid, MsgId}).

is_qos2_msg(#mqtt_message{qos = 2})->
    true;
is_qos2_msg(#mqtt_message{})->
    false.

emit_stats(StateData = #state{protocol=ProtoState}) ->
    emit_stats(?PROTO_GET_CLIENT_ID(ProtoState), StateData).

emit_stats(_ClientId, State = #state{enable_stats = false}) ->
    ?LOG(debug, "The enable_stats is false, skip emit_state~n", [], State),
    State;
emit_stats(ClientId, #state{sock = Sock, protocol = ProtoState}) ->
    StatsList = lists:append([emqx_misc:proc_stats(),
                              ?PROTO_STATS(ProtoState),
                              sock_stats(Sock, ?SOCK_STATS)]),
    ?SET_CLIENT_STATS(ClientId, StatsList).

get_corrected_qos(?QOS_NEG1, StateData) ->
    ?LOG(debug, "Receive a publish with Qos=-1", [], StateData),
    ?QOS0;

get_corrected_qos(Qos, _StateData) ->
    Qos.

