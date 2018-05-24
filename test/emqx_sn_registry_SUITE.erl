%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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
%%%===================================================================

-module(emqx_sn_registry_SUITE).

-include("emqx_sn.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(REGISTRY, emqx_sn_registry).
-define(MAX_PREDEF_ID, 2).
-define(PREDEF_TOPICS, [{1, <<"/predefined/topic/name/hello">>},
                        {2, <<"/predefined/topic/name/nice">>}]).

all() ->
    [register_topic_test, register_topic_test2, register_topic_test3,
     register_topic_test4, register_topic_test4].

init_per_suite(Config) ->
    lager_common_test_backend:bounce(debug),
    _ = application:set_env(emqx_sn, predefined, ?PREDEF_TOPICS),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, _Pid} = ?REGISTRY:start_link(),
    Config;

init_per_testcase(_TestCase, Config) ->
    ?REGISTRY:stop(),
    Config.

register_topic_test(_Config) ->
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(<<"Topic1">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    emqx_sn_registry:unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)).

register_topic_test2(_Config) ->
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(<<"Topic1">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic3">>)),
    ?REGISTRY:unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)).

register_topic_test3(_Config) ->
    register_a_lot(?MAX_PREDEF_ID+1, 16#ffff),
    ?assertEqual({error, too_large}, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicABC">>)),
    Topic1 = iolist_to_binary(io_lib:format("Topic~p", [?MAX_PREDEF_ID+1])),
    Topic2 = iolist_to_binary(io_lib:format("Topic~p", [?MAX_PREDEF_ID+2])),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic1)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic2)),
    ?REGISTRY:unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic2)).

register_topic_test4(_Config) ->
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicA">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicB">>)),
    ?assertEqual(?MAX_PREDEF_ID+3, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicC">>)),
    ?REGISTRY:unregister_topic(<<"ClientId">>),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicD">>)).

register_topic_test5(_Config) ->
    ?assertEqual(wildcard_topic, ?REGISTRY:register_topic(<<"ClientId">>, <<"/TopicA/#">>)),
    ?assertEqual(wildcard_topic, ?REGISTRY:register_topic(<<"ClientId">>, <<"/+/TopicB">>)).

register_a_lot(Max, Max) ->
    ok;
register_a_lot(N, Max) when N < Max ->
    Topic = iolist_to_binary(["Topic", integer_to_list(N)]),
    ?assertEqual(N, ?REGISTRY:register_topic(<<"ClientId">>, Topic)),
    register_a_lot(N+1, Max).

