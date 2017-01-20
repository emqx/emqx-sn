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

-module(emq_sn_gateway_queue).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

-define(LOG(Level, Format, Args),
    lager:Level("MQTT-SN(pending_id): " ++ Format, Args)).

%% API.
-export([start_link/0, stop/1]).

-export([insert_puback/3, get_puback/2]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {awaiting_puback}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> {ok, pid()}).
start_link() ->
	gen_server:start_link(?MODULE, [], []).

-spec(stop(pid()) -> ok).
stop(Pid) ->
    gen_server:call(Pid, stop).

insert_puback(Pid, TopicId, MsgId) ->
    gen_server:call(Pid, {insert_puback, TopicId, MsgId}).

get_puback(Pid, MsgId) ->
    gen_server:call(Pid, {get_puback, MsgId}).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
	{ok, #state{awaiting_puback = maps:new()}}.

handle_call({insert_puback, TopicId, MsgId}, _From, State=#state{awaiting_puback = Awaiting}) ->
    AwaitingNew = maps:put(MsgId, TopicId, Awaiting),
    {reply, ok, State#state{awaiting_puback = AwaitingNew}, hibernate};

handle_call({get_puback, MsgId}, _From, State=#state{awaiting_puback = Awaiting}) ->
    TopicId = maps:get(MsgId, Awaiting, 0),
    AwaitingNew = case TopicId of
                      undefined -> Awaiting;
                      _ -> maps:remove(MsgId, Awaiting)
                  end,
    {reply, {TopicId, MsgId}, State#state{awaiting_puback = AwaitingNew}, hibernate};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.



%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------


