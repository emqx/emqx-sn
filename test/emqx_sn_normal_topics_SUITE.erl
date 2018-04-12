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

-module(emqx_sn_normal_topics_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_sn.hrl").

-import(emqx_sn_normal_topics, [start_link/0, register_topic/2, unregister_topic/1, stop/0, lookup_topic/2, lookup_topic_id/2]).

-compile(export_all).
-define(MAX_PRED_TOPIC_ID, 2).
-define(PREDEF_TOPIC_LIST, [{1,<<"/predefined/topic/name/hello">>},{2,<<"/predefined/topic/name/nice">>}]).

all() -> [register_topic_test, register_topic_test2, register_topic_test3, register_topic_test4, register_topic_test4].


init_per_suite(Config) ->
    lager_common_test_backend:bounce(debug),
    Config.

end_per_suite(_Config) ->
    ok.



register_topic_test(_Config) ->
    emqx_sn_predefined_topics:start_link(?PREDEF_TOPIC_LIST),
    start_link(),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+2, register_topic(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(<<"Topic1">>, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+1)),
    ?assertEqual(<<"Topic2">>, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+2)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+2, lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+1)),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+2)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    stop().


register_topic_test2(_Config) ->
    emqx_sn_predefined_topics:start_link(?PREDEF_TOPIC_LIST),
    start_link(),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+2, register_topic(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(<<"Topic1">>, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+1)),
    ?assertEqual(<<"Topic2">>, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+2)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+2, lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, <<"Topic3">>)),
    unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+1)),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+2)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    stop().


register_topic_test3(_Config) ->
    lager:debug("register_topic_test3 will take long long time ...~n"),
    emqx_sn_predefined_topics:start_link(?PREDEF_TOPIC_LIST),
    start_link(),
    register_a_lot(?MAX_PRED_TOPIC_ID+1, 16#fffe),
    lager:debug("start overflow test~n"),
    ?assertEqual(undefined, register_topic(<<"ClientId">>, <<"TopicABC">>)),
    timer:sleep(500),
    Topic1 = list_to_binary(io_lib:format("Topic~p", [?MAX_PRED_TOPIC_ID+1])),
    Topic2 = list_to_binary(io_lib:format("Topic~p", [?MAX_PRED_TOPIC_ID+2])),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, lookup_topic_id(<<"ClientId">>, Topic1)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+2, lookup_topic_id(<<"ClientId">>, Topic2)),
    unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+1)),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, ?MAX_PRED_TOPIC_ID+2)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, Topic1)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, Topic2)),
    stop().


register_topic_test4(_Config) ->
    emqx_sn_predefined_topics:start_link(?PREDEF_TOPIC_LIST),
    start_link(),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, register_topic(<<"ClientId">>, <<"TopicA">>)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+2, register_topic(<<"ClientId">>, <<"TopicB">>)),
    ?assertEqual(?MAX_PRED_TOPIC_ID+3, register_topic(<<"ClientId">>, <<"TopicC">>)),
    unregister_topic(<<"ClientId">>),
    ?assertEqual(?MAX_PRED_TOPIC_ID+1, register_topic(<<"ClientId">>, <<"TopicD">>)),
    stop().


register_topic_test5(_Config) ->
    emqx_sn_predefined_topics:start_link(?PREDEF_TOPIC_LIST),
    start_link(),
    ?assertEqual(wildcard_topic, register_topic(<<"ClientId">>, <<"/TopicA/#">>)),
    ?assertEqual(wildcard_topic, register_topic(<<"ClientId">>, <<"/+/TopicB">>)),
    stop().


register_a_lot(Max, Max) ->
    TopicString = io_lib:format("Topic~p", [Max]),
    ?assertEqual(Max, register_topic(<<"ClientId">>, list_to_binary(TopicString))),
    ok;
register_a_lot(N, Max) ->
    case (N rem 1024) of
        0 -> lager:debug("register_a_lot N=~p/65536, long long time, please be patient~n", [N]);
        _ -> ok
    end,
    TopicString = io_lib:format("Topic~p", [N]),
    ?assertEqual(N, register_topic(<<"ClientId">>, list_to_binary(TopicString))),
    register_a_lot(N+1, Max).


