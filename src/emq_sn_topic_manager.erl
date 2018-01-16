%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emq_sn_topic_manager).

-author("Feng Lee <feng@emqtt.io>").

-define(LOG(Level, Format, Args),
    lager:Level("MQTT-SN(topic manager): " ++ Format, Args)).

%% API.
-export([register_topic/2, lookup_topic/2, unregister_topic/1, lookup_topic_id/2, wildcard/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(register_topic(binary(), binary()) -> integer()).
register_topic(ClientId, TopicName) ->
    case emq_sn_predefined_topics:lookup_predef_topic_id(TopicName) of
        undefined ->
            emq_sn_normal_topics:register_topic(ClientId, TopicName);
        PredefTopicId ->
            ?LOG(debug, "register a predefined topic name, now we just assign the corresponding predefined topic id. ClientId=~p, TopicName=~p, PredefTopicId=~p", [ClientId, TopicName, PredefTopicId]),
            PredefTopicId
    end.

-spec(lookup_topic(binary(), pos_integer()) -> undefined | binary()).
lookup_topic(ClientId, TopicId) ->
    case emq_sn_predefined_topics:get_max_predef_topic_id() < TopicId of
        true ->
            emq_sn_normal_topics:lookup_topic(ClientId, TopicId);
        false ->
            emq_sn_predefined_topics:lookup_predef_topic(TopicId)
    end.

-spec(lookup_topic_id(binary(), binary) -> undefined | tuple()).
lookup_topic_id(ClientId, TopicName) ->
    case emq_sn_normal_topics:lookup_topic_id(ClientId, TopicName) of
        undefined ->
            case emq_sn_predefined_topics:lookup_predef_topic_id(TopicName) of
                undefined ->
                    undefined;
                PredefTopicId ->
                    {predef, PredefTopicId}
            end;
        TopicId ->
            {normal, TopicId}
    end.

-spec(unregister_topic(binary()) -> ok).
unregister_topic(ClientId) ->
    emq_sn_normal_topics:unregister_topic(ClientId).


%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

-spec(wildcard(binary()|list()) -> true | false).
wildcard(Topic) when is_binary(Topic) ->
    TopicString = binary_to_list(Topic),
    wildcard(TopicString);
wildcard([]) ->
    false;
wildcard([$#|_]) ->
    true;
wildcard([$+|_]) ->
    true;
wildcard([_H|T]) ->
    wildcard(T).
