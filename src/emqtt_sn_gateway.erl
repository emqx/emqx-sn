%%--------------------------------------------------------------------
%% Copyright (c) 2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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

-module(emqtt_sn_gateway).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_fsm).

-include("emqtt_sn.hrl").

%% API.
-export([start_link/2]).

%% SUB/UNSUB Asynchronously. Called by plugins.
-export([subscribe/2, unsubscribe/2]).

%% gen_fsm.

-export([idle/2, wait_for_will_topic/2, wait_for_will_msg/2, connected/2, state_name/2]).

-export([init/1, handle_event/3, state_name/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(state, {gwid, gwinfo = <<>>, sock, peer, proto, keepalive}).

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
    {ok, idle, #state{gwid = 1, sock = Sock, peer = Peer}, 3000}.

idle(timeout, StateData) ->
    {stop, idle_timeout, StateData};

idle(?SN_SEARCHGW_MSG(Radius), StateData = #state{gwid = GwId, gwinfo = GwInfo}) ->
    send_message(?SN_GWINFO(GwId, GwInfo), StateData),
    {next_state, idle, StateData};

idle(_Event, StateData) ->
    %%TODO:...
    {next_state, idle, StateData}.

idle(?SN_CONNECT_MSG(Flags, ProtoId, Duration, ClientId), StateData = #state{connpkt = ConnPkt}) ->
    %%TODO: 
    send_message(?SN_WILLTOPICREQ_MSG(), StateData),
	{next_state, state_name, StateData};

idle(_Event, StateData) ->
	{next_state, state_name, StateData}.

wait_for_will_topic(?SN_WILLTOPIC_MSG(Flags, Topic), StateData = #state{connpkt = ConnPkt}) ->
    send_message(?SN_WILLMSGREQ_MSG(), StateData),
    {next_state, wait_for_will_msg, StateData};

wait_for_will_topic(_Event, StateData) ->
    %%TODO: LOG error
    {next_state, wait_for_will_topic, StateData}.
    
wait_for_will_msg(?SN_WILLMSG_MSG(Msg), StateData = #state{connpkt = ConnPkt}) ->
    %%TODO: protocol connect
    ReturnCode = 0,
    %% send connack
    send_message(?SN_CONNACK_MSG(ReturnCode), StateData),
    {next_state, connected, StateData}; 

wait_for_will_msg(_Event, StateData) ->
    %%TODO: LOG error
    {next_state, wait_for_will_msg, StateData}.

connected(_Event, StateData) ->
	{next_state, connected, StateData};

connected(_Event, StateData) ->
	{next_state, connected, StateData};

connected(_Event, StateData) ->
	{next_state, connected, StateData};

connected(_Event, StateData) ->
	{next_state, connected, StateData};

connected(_Event, StateData) ->
	{next_state, connected, StateData}.

state_name(_Event, StateData) ->
	{next_state, state_name, StateData}.

handle_event(_Event, StateName, StateData) ->
	{next_state, StateName, StateData}.

state_name(_Event, _From, StateData) ->
	{reply, ignored, state_name, StateData}.

handle_sync_event(_Event, _From, StateName, StateData) ->
	{reply, ignored, StateName, StateData}.

handle_info({datagram, Data}, StateName, StateData) ->
    {ok, Msg} = emqtt_sn_message:parse(Data),
    ?LOG(info, "RECV ~p", [Msg], StateData),
    ?MODULE:StateName(Msg, StateData); %% cool?

handle_info(_Info, StateName, StateData) ->
	{next_state, StateName, StateData}.

terminate(_Reason, _StateName, _StateData) ->
	ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.

send_message(Msg, #state{sock = Sock, peer = {Host, Port}}) ->
    io:format("SEND ~p~n", [Msg]),
    gen_udp:send(Sock, Host, Port, emqtt_sn_message:serialize(Msg)).

