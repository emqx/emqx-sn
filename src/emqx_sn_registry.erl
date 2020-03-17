%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_registry).

-behaviour(gen_server).

-include("emqx_sn.hrl").

-define(LOG(Level, Format, Args),
        emqx_logger:Level("MQTT-SN(registry): " ++ Format, Args)).

-export([ start_link/0
        , stop/0
        ]).

-export([ register_topic/2
        , unregister_topic/1
        ]).

-export([ lookup_topic/2
        , lookup_topic_id/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(TAB, ?MODULE).

-record(state, {max_predef_topic_id = 0}).

%%-----------------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(stop() -> ok).
stop() ->
    gen_server:stop(?MODULE, normal, infinity).

-spec(register_topic(binary(), binary()) -> integer() | {error, term()}).
register_topic(ClientId, TopicName) when is_binary(TopicName) ->
    case emqx_topic:wildcard(TopicName) of
        false ->
            gen_server:call(?MODULE, {register, ClientId, TopicName});
        %% TopicId: in case of “accepted” the value that will be used as topic
        %% id by the gateway when sending PUBLISH messages to the client (not
        %% relevant in case of subscriptions to a short topic name or to a topic
        %% name which contains wildcard characters)
        true  -> {error, wildcard_topic}
    end.

-spec(lookup_topic(binary(), pos_integer()) -> undefined | binary()).
lookup_topic(ClientId, TopicId) when is_integer(TopicId) ->
    case lookup_element({predef, TopicId}, 2) of
        undefined ->
            lookup_element({ClientId, TopicId}, 2);
        Topic -> Topic
    end.

-spec(lookup_topic_id(binary(), binary()) -> undefined | pos_integer()).
lookup_topic_id(ClientId, TopicName) when is_binary(TopicName) ->
    case lookup_element({predef, TopicName}, 2) of
        undefined ->
            lookup_element({ClientId, TopicName}, 2);
        TopicId -> {predef, TopicId}
    end.

lookup_element(Key, Pos) ->
    try ets:lookup_element(?TAB, Key, Pos) catch error:badarg -> undefined end.

-spec(unregister_topic(binary()) -> ok).
unregister_topic(ClientId) ->
    gen_server:call(?MODULE, {unregister, ClientId}).

%%-----------------------------------------------------------------------------

init([]) ->
    %% {predef, TopicId}     -> TopicName
    %% {predef, TopicName}   -> TopicId
    %% {ClientId, TopicId}   -> TopicName
    %% {ClientId, TopicName} -> TopicId
    PredefTopics = application:get_env(emqx_sn, predefined, []),
    _ = ets:new(?TAB, [set, named_table, protected, {read_concurrency, true}]),
    MaxPredefId = lists:foldl(
                    fun({TopicId, TopicName}, AccId) ->
                        _ = ets:insert(?TAB, {{predef, TopicId}, TopicName}),
                        _ = ets:insert(?TAB, {{predef, TopicName}, TopicId}),
                        if TopicId > AccId -> TopicId; true -> AccId end
                    end, 0, PredefTopics),
    {ok, #state{max_predef_topic_id = MaxPredefId}}.

handle_call({register, ClientId, TopicName}, _From,
            State = #state{max_predef_topic_id = PredefId}) ->
    case lookup_topic_id(ClientId, TopicName) of
        {predef, PredefTopicId}  when is_integer(PredefTopicId) ->
            {reply, PredefTopicId, State};
        TopicId when is_integer(TopicId) ->
            {reply, TopicId, State};
        undefined ->
            case next_topic_id(PredefId, ClientId) of
                TopicId when TopicId >= 16#FFFF ->
                    {reply, {error, too_large}, State};
                TopicId ->
                    _ = ets:insert(?TAB, {{ClientId, next_topic_id}, TopicId + 1}),
                    _ = ets:insert(?TAB, {{ClientId, TopicName}, TopicId}),
                    _ = ets:insert(?TAB, {{ClientId, TopicId}, TopicName}),
                    {reply, TopicId, State}
            end
    end;

handle_call({unregister, ClientId}, _From, State) ->
    ets:match_delete(?TAB, {{ClientId, '_'}, '_'}),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected request: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------

next_topic_id(PredefId, ClientId) ->
    case ets:lookup(?TAB, {ClientId, next_topic_id}) of
        [{_, Id}] -> Id;
        []        -> PredefId + 1
    end.
