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

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {bc_address, sock, duration, gwid, tref}).

-define(PORT, 1884).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link(list()) -> {ok, pid()}).
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec(stop() -> ok).
stop() ->
    gen_server:call(?MODULE, stop).


%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Duration, GwId]) ->
    {ok, Sock} = gen_udp:open(0, [binary, {broadcast, true}]),
    IpList = get_boradcast_ip(),
    State = #state{bc_address = IpList, sock = Sock, duration = Duration, gwid = GwId},
    send_advertise(State),
	{ok, State#state{tref = start_timer(Duration)}}.


handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

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

send_advertise(#state{bc_address = IpList, sock = Sock, gwid = GwId, duration = Duration}) ->
    Data = emq_sn_message:serialize(?SN_ADVERTISE_MSG(GwId, Duration)),
    send_advertise(Sock, IpList, Data).

send_advertise(_Sock, [], _Data) ->
    ok;
send_advertise(Sock, [IP|T], Data) ->
    ?LOG(debug, "SEND SN_ADVERTISE to ~p~n", [IP]),
    gen_udp:send(Sock, IP, ?PORT, Data),
    send_advertise(Sock, T, Data).

start_timer(Duration) ->
    erlang:send_after(timer:seconds(Duration), self(), broadcast_advertise).


get_boradcast_ip() ->
    {ok, IfList} = inet:getiflist(),
    B = [inet:ifget(X, [broadaddr]) || X <- IfList],
    extract_ip(B, []).

extract_ip([], Acc) ->
    Acc;
extract_ip([{ok,[{broadaddr,IP}]}|T], Acc) ->
    case lists:member(IP, Acc) of
        true -> extract_ip(T, Acc);
        false -> extract_ip(T, [IP | Acc])
    end.

