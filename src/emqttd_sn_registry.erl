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

-module(emqttd_sn_registry).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

%% API.
-export([start_link/0, stop/0]).

-export([register_topic/3, lookup_topic/2, unregister_topic/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> {ok, pid()}).
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(stop() -> ok).
stop() ->
    gen_server:call(?MODULE, stop).

-spec(register_topic(binary(), pos_integer(), binary()) -> ok).
register_topic(ClientId, TopicId, TopicName) ->
    gen_server:call(?MODULE, {register, ClientId, TopicId, TopicName}).

-spec(lookup_topic(binary(), pos_integer()) -> undefined | binary()).
lookup_topic(ClientId, TopicId) ->
    try ets:lookup_element(sn_topic_name, {ClientId, TopicId}, 2)
    catch
        error:badarg -> undefined
    end.

-spec(unregister_topic(binary()) -> ok).
unregister_topic(ClientId) ->
    gen_server:cast(?MODULE, {unregister, ClientId}).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% ClientId -> [TopicId]
    ets:new(sn_topic_id, [bag, named_table, protected]),
    %% {ClientId, TopicId} -> TopicName
    ets:new(sn_topic_name, [set, named_table, protected]),
	{ok, #state{}}.

handle_call({register, ClientId, TopicId, TopicName}, _From, State) ->
    ets:insert(sn_topic_id, {ClientId, TopicId}),
    ets:insert(sn_topic_name, {{ClientId, TopicId}, TopicName}),
	{reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast({unregister, ClientId}, State) ->
    [ets:delete(sn_topic_name, {ClientId, TopicId})
        || {_, TopicId} <- ets:lookup(sn_topic_id, ClientId)],
    ets:delete(sn_topic_id, ClientId),
	{noreply, State};

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

register_topic_test() ->
    start_link(),
    register_topic(<<"ClientId">>, 1, <<"Topic1">>),
    register_topic(<<"ClientId">>, 2, <<"Topic2">>),
    ?assertEqual(<<"Topic1">>, lookup_topic(<<"ClientId">>, 1)),
    ?assertEqual(<<"Topic2">>, lookup_topic(<<"ClientId">>, 2)),
    unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, 1)),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, 2)),
    stop().

-endif.

