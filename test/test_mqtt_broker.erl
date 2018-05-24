%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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
%%%-------------------------------------------------------------------

-module(test_mqtt_broker).

-compile(export_all).
-compile(nowarn_export_all).

-behaviour(gen_server).

-record(proto_stats, {enable_stats = false, recv_pkt = 0, recv_msg = 0,
    send_pkt = 0, send_msg = 0}).

-record(state, {subscriber, peer, pkt_opts, connect_pkt, last_puback, last_pubrel, rx_message, subed, unsubed, stats_data}).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_misc.hrl").

-define(LOG(Format, Args), ct:print("TEST broker: " ++ Format, Args)).

proto_init(Peer, SendFun, PktOpts) ->
    KeepaliveDuration = 3,   % seconds
    self() ! {keepalive, start, KeepaliveDuration},
    put(debug_unit_test_send_func, SendFun),
    gen_server:call(?MODULE, {init, self(), Peer, PktOpts}).

proto_receive(Packet, _Proto) ->
    case gen_server:call(?MODULE, {received_packet, Packet}) of
        {ok, State, OutPkt} ->
            case OutPkt of
                undefined -> ok;
                _ ->
                    Send = get(debug_unit_test_send_func),
                    Send(OutPkt)
            end,
            {ok, State};
        Other -> Other
    end.

get_online_user() ->
    gen_server:call(?MODULE, get_online_user).

get_puback() ->
    gen_server:call(?MODULE, get_puback).

get_pubrel() ->
    gen_server:call(?MODULE, get_pubrel).

get_published_msg() ->
    gen_server:call(?MODULE, get_published_msg).

get_subscrbied_topic() ->
    gen_server:call(?MODULE, get_subscribed_topic).

get_unsubscrbied_topic() ->
    gen_server:call(?MODULE, get_unsubscribed_topic).

dispatch(MsgId, Qos, Retain, Topic, Msg) ->
    gen_server:call(?MODULE, {dispatch, {MsgId, Qos, Retain, Topic, Msg}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

init(_Param) ->
    ets:new(test_client_stats, [set, named_table, public]),
    {ok, #state{subscriber = undefined}}.

handle_call({init, Subscriber, Peer, PktOpts}, _From, State) ->
    case proplists:get_value(client_enable_stats, PktOpts, false) of
        true ->
            Subscriber ! emit_stats,
            ?LOG("send emit_stats", []);
        false ->
            ?LOG("client_enable_stats is false, stats case may fail!", [])
    end,
    ?LOG("test broker init Subscriber=~p, Peer=~p, PktOpts=~p, broker_pid=~p~n", [Subscriber, Peer, PktOpts, self()]),
    {reply, ok, State#state{subscriber = Subscriber, peer = Peer, pkt_opts = PktOpts}};

handle_call({received_packet, ?CONNECT_PACKET(ConnPkt)}, _From, State=#state{}) ->
    #mqtt_packet_connect{client_id = ClientId, username = Username} = ConnPkt,
    ?LOG("test broker get CONNECT ~p~n", [ConnPkt]),
    (is_binary(ClientId) and is_binary(Username)) orelse error("ClientId and Username should be binary"),
    OutPkt = ?CONNACK_PACKET(?RC_SUCCESS),
    {reply, {ok, [], OutPkt}, State#state{connect_pkt = ConnPkt}};

handle_call({received_packet, Msg = ?PUBLISH_PACKET(Qos, PacketId)}, _From, State=#state{}) ->
    ?LOG("test broker get PUBLISH ~p~n", [Msg]),
    #mqtt_packet{header = Header, variable = Var, payload = Payload} = Msg,
    #mqtt_packet_header{type = ?PUBLISH, dup = _Dup, qos = Qos, retain = Retain} = Header,
    #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId} = Var,
    OutPkt = case Qos of
                 ?QOS1 -> ?PUBACK_PACKET(MsgId);
                 ?QOS2 -> ?PUBREC_PACKET(MsgId);
                 _ -> undefined
             end,
    {reply, {ok, [], OutPkt}, State#state{rx_message = {MsgId, Qos, Retain, TopicName, Payload}}};

handle_call({received_packet, ?PUBACK_PACKET(MsgId)}, _From, State=#state{}) ->
    ?LOG("test broker get PUBACK MsgId=~p~n", [MsgId]),
    {reply, {ok, []}, State#state{last_puback = MsgId}};

handle_call({received_packet, ?PUBREC_PACKET(MsgId)}, _From, State=#state{}) ->
    ?LOG("test broker get PUBREC MsgId=~p~n", [MsgId]),
    OutPkt = ?PUBREL_PACKET(MsgId),
    {reply, {ok, [], OutPkt}, State};

handle_call({received_packet, ?PUBREL_PACKET(MsgId)}, _From, State=#state{}) ->
    ?LOG("test broker get PUBREL MsgId=~p~n", [MsgId]),
    OutPkt = ?PUBCOMP_PACKET(MsgId),
    {reply, {ok, [], OutPkt}, State#state{last_pubrel = MsgId}};

handle_call({received_packet, ?SUBSCRIBE_PACKET(MsgId, [{TopicName, Qos}])}, _From, State=#state{subscriber = Pid}) ->
    ?LOG("test broker get SUBSCRIBE MsgId=~p, TopicName=~p, Qos=~p, Client Pid=~p~n", [MsgId, TopicName, Qos, Pid]),
    Pid ! {suback, MsgId, [Qos]},
    {reply, {ok, []}, State#state{subed = {TopicName, Qos}}};

handle_call({received_packet, ?UNSUBSCRIBE_PACKET(MsgId, [TopicName])}, _From, State=#state{}) ->
    ?LOG("test broker get UNSUBSCRIBE MsgId=~p, TopicName=~p~n", [MsgId, TopicName]),
    OutPkt = ?UNSUBACK_PACKET(MsgId),
    {reply, {ok, [], OutPkt}, State#state{unsubed = TopicName}};

handle_call({received_packet, ?PACKET(?DISCONNECT)}, _From, State=#state{}) ->
    ?LOG("test broker get DISCONNECT~n", []),
    {reply, {stop, normal, []}, State#state{connect_pkt = undefined}};

handle_call(get_online_user, _From, State=#state{connect_pkt = undefined}) ->
    ?LOG("test broker get online user none~n", []),
    {reply, {undefined, undefined}, State};

handle_call(get_online_user, _From, State=#state{connect_pkt = ConnPkt}) ->
    #mqtt_packet_connect{client_id  = ClientId, username = Username} = ConnPkt,
    ?LOG("test broker get online ClientId=~p, Username=~p~n", [ClientId, Username]),
    {reply, {ClientId, Username}, State};

handle_call(get_puback, _From, State=#state{last_puback = MsgId}) ->
    ?LOG("test broker get published PUBACK MsgId=~p~n", [MsgId]),
    {reply, MsgId, State};

handle_call(get_pubrel, _From, State=#state{last_pubrel = MsgId}) ->
    ?LOG("test broker get published PUBREL MsgId=~p~n", [MsgId]),
    {reply, MsgId, State};

handle_call(get_published_msg, _From, State=#state{rx_message = undefined}) ->
    ?LOG("test broker get published none~n", []),
    {reply, {undefined, undefined, undefined, undefined, undefined}, State};

handle_call(get_published_msg, _From, State=#state{rx_message = {MsgId, Qos, Retain, TopicName, Payload}}) ->
    ?LOG("test broker get published MsgId=~p, Qos=~p, Retain=~p, TopicName=~p, Payload=~p~n", [MsgId, Qos, Retain, TopicName, Payload]),
    {reply, {MsgId, Qos, Retain, TopicName, Payload}, State};

handle_call(get_subscribed_topic, _From, State=#state{subed = undefined}) ->
    ?LOG("test broker get subscribed topic=undefined~n", []),
    {reply, undefined, State};

handle_call(get_subscribed_topic, _From, State=#state{subed = {TopicName, Qos}}) ->
    ?LOG("test broker get subscribed topic=~p, Qos=~p~n", [TopicName, Qos]),
    {reply, TopicName, State};

handle_call(get_unsubscribed_topic, _From, State=#state{unsubed = TopicName}) ->
    ?LOG("test broker get unsubscribed topic=~p~n", [TopicName]),
    {reply, TopicName, State};

handle_call({dispatch, {_MsgId, _Qos, _Retain, Topic, Msg}}, _From, State=#state{subscriber = undefined}) ->
    ?LOG("test broker CAN NOT dispatch topic=~p, Msg=~p due to no subscriber~n", [Topic, Msg]),
    {reply, ok, State};

handle_call({dispatch, {MsgId, Qos, Retain, Topic, Msg}}, _From, State=#state{subscriber = SubProc}) ->
    ?LOG("test broker dispatch topic=~p, Msg=~p~n", [Topic, Msg]),
    (is_binary(Topic) and is_binary(Msg)) orelse error("Topic and Msg should be binary"),
    SubProc ! {deliver, #mqtt_message{packet_id = MsgId, qos = Qos, retain = Retain, topic = Topic, payload = Msg}},
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(Req, _From, State) ->
    ?LOG("test_mqtt_broker: ignore call Req=~p~n", [Req]),
    {reply, {error, badreq}, State}.


handle_cast(Msg, State) ->
    ?LOG("test_mqtt_broker: ignore cast msg=~p~n", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG("test_mqtt_broker: ignore info=~p~n", [Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG("test_mqtt_broker: terminate Reason=~p~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-record(keepalive, {statfun, statval, tsec, tmsg, tref, repeat = 0}).

-type(keepalive() :: #keepalive{}).

%% @doc Start a keepalive
-spec(start(fun(), integer(), any()) -> undefined | keepalive()).
start(_, 0, _) ->
    undefined;
start(StatFun, TimeoutSec, TimeoutMsg) ->
    {ok, StatVal} = StatFun(),
    #keepalive{statfun = StatFun, statval = StatVal,
        tsec = TimeoutSec, tmsg = TimeoutMsg,
        tref = timer(TimeoutSec, TimeoutMsg)}.

%% @doc Check keepalive, called when timeout.
-spec(check(keepalive()) -> {ok, keepalive()} | {error, any()}).
check(KeepAlive = #keepalive{statfun = StatFun, statval = LastVal, repeat = Repeat}) ->
    case StatFun() of
        {ok, NewVal} ->
            if NewVal =/= LastVal ->
                {ok, resume(KeepAlive#keepalive{statval = NewVal, repeat = 0})};
                Repeat < 1 ->
                    {ok, resume(KeepAlive#keepalive{statval = NewVal, repeat = Repeat + 1})};
                true ->
                    {error, timeout}
            end;
        {error, Error} ->
            {error, Error}
    end.

resume(KeepAlive = #keepalive{tsec = TimeoutSec, tmsg = TimeoutMsg}) ->
    KeepAlive#keepalive{tref = timer(TimeoutSec, TimeoutMsg)}.

%% @doc Cancel Keepalive
-spec(cancel(keepalive()) -> ok).
cancel(#keepalive{tref = TRef}) ->
    cancel(TRef);
cancel(undefined) ->
    ok;
cancel(TRef) ->
        catch erlang:cancel_timer(TRef).

timer(Sec, Msg) ->
    erlang:send_after(timer:seconds(Sec), self(), Msg).

stats(_) ->
    Stats = #proto_stats{enable_stats = true, recv_pkt = 3, recv_msg = 3,
        send_pkt = 2, send_msg = 2},
    tl(?record_to_proplist(proto_stats, Stats)).

set_client_stats(ClientId, Statlist) ->
    ets:insert(test_client_stats, {ClientId, [{'$ts', emqx_time:now_secs()}|Statlist]}).

print_table(client_stats) ->
    List = ets:tab2list(test_client_stats),
    ?LOG("The table ~p with the content is ~p~n", [ets:info(test_client_stats, name), List]).

clientid(_) ->
     cleintid_test.

send(Msg, ProtoState) ->
    Packet =#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}} = emqx_message:to_packet(Msg),
    ?LOG("The topic_name is ~p~n", [Topic]),
    Send = get(debug_unit_test_send_func),
    Send(Packet),
    {ok, ProtoState}.

