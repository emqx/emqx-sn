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

-module(emq_sn_predefined_topics).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

-define(LOG(Level, Format, Args),
    lager:Level("MQTT-SN(predefined topics): " ++ Format, Args)).

-include("emq_sn.hrl").

%% API.
-export([start_link/1, stop/0, get_max_predef_topic_id/0, lookup_predef_topic/1, lookup_predef_topic_id/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {max_topic_id = 0   ::integer()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link(list()) -> {ok, pid()}).
start_link(PreDefTopics) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [PreDefTopics], []).

-spec(stop() -> ok).
stop() ->
    gen_server:call(?MODULE, stop).

-spec(get_max_predef_topic_id() -> {integer()}).
get_max_predef_topic_id() ->
    gen_server:call(?MODULE, get_max_topic_id).

-spec(lookup_predef_topic(pos_integer()) -> {undefine | binary()}).
lookup_predef_topic(TopicId) ->
    try
        ets:lookup_element(sn_predef_topic_name, TopicId, 2)
    catch
        error:badarg -> undefined
    end.

-spec(lookup_predef_topic_id(binary()) -> {undefine | pos_integer()}).
lookup_predef_topic_id(TopicName) ->
    try
        ets:lookup_element(sn_predef_topic_id, TopicName, 2)
    catch
        error:badarg -> undefined
    end.
%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([PreDefTopics]) ->
    %% TopicName -> TopicId
    ets:new(sn_predef_topic_id, [set, named_table, public]),
    %% TopicId -> TopicName
    ets:new(sn_predef_topic_name, [set, named_table, public]),
    MaxTopicId = lists:foldl( fun(Element = {TopicId, TopicName}, AccIn) ->
                                  ets:insert(sn_predef_topic_name, Element),
                                  ets:insert(sn_predef_topic_id, Element1 = {TopicName, TopicId}),
                                  ?LOG(debug, "insert ~p in the sn_predef_topic_name table, insert ~p in the sn_predef_topic_id table~n", [Element, Element1]),
                                  case TopicId > AccIn of
                                      true -> TopicId;
                                      _    -> AccIn
                                  end
                              end, 0, PreDefTopics),
    ?LOG(debug, "The max predefined topic id is ~p~n", [MaxTopicId]),
	{ok, #state{max_topic_id = MaxTopicId}}.

handle_call(get_max_topic_id, _From, State = #state{max_topic_id = MaxTopicId}) ->
    {reply, MaxTopicId, State, hibernate};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State, hibernate}.

terminate(_Reason, #state{}) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.



%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------


