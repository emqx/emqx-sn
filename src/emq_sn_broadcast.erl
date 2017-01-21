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

-module(emq_sn_broadcast).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

-define(LOG(Level, Format, Args),
    lager:Level("MQTT-SN(broadcast): " ++ Format, Args)).

-include("emq_sn.hrl").

%% API.
-export([start_link/1, stop/0]).

-export([]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {broadaddr, sock, duration, gwid, tref}).

-define(PORT, 1884).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link(integer()) -> {ok, pid()}).
start_link([Duration]) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [Duration], []).

-spec(stop() -> ok).
stop() ->
    gen_server:call(?MODULE, stop).


%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Duration, GwId]) ->
    {ok, IfList} = inet:getifaddrs(),
    {_, LoOpts} = proplists:lookup("lo", IfList),
    {_, BroadAddress} = proplists:lookup(broadaddr, LoOpts),
    {ok, Sock} = gen_udp:open( 0, [binary, {broadcast, true}]),
    State = #state{broadaddr = BroadAddress, sock = Sock, duration = Duration, gwid = GwId},
    send_advertise(State),
	{ok, State#state{tref = start_timer(Duration)}}.


handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info({udp, _Socket, _, _, Bin}, State) ->
    case emq_sn_message:parse(Bin) of
        {ok, ?SN_SEARCHGW_MSG(Radius)} ->
            ?LOG(info, "RECV SN_SEARCHGW Radius=~p", [Radius]),
            send_gwinfo(State);
        {ok, _} -> ignore;
        format_error -> ignore
    end,
    {noreply, State};

handle_info(broadcast_advertise, State=#state{duration = Duration}) ->
    send_advertise(State),
    {noreply, State#state{tref = start_timer(Duration)}};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, #state{tref = Timer}) ->
    erlang:cancel_timer(Timer),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.



%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

send_advertise(#state{sock = Sock, broadaddr = Host, gwid = GwId, duration = Duration}) ->
    ?LOG(debug, "SEND SN_ADVERTISE~n", []),
    gen_udp:send(Sock, Host, ?PORT, emq_sn_message:serialize(?SN_ADVERTISE_MSG(GwId, Duration))).

send_gwinfo(#state{sock = Sock, broadaddr = Host, gwid = GwId}) ->
    ?LOG(debug, "SEND SN_ADVERTISE GwId=~p~n", [GwId]),
    gen_udp:send(Sock, Host, ?PORT, emq_sn_message:serialize(?SN_GWINFO_MSG(GwId, <<>>))).

start_timer(Duration) ->
    erlang:send_after(timer:seconds(Duration), self(), broadcast_advertise).

