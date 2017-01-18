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

-module(emq_sn_registry_SUITE).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("eunit/include/eunit.hrl").
-include("emq_sn.hrl").

-import(emq_sn_registry, [start_link/0, register_topic/3, unregister_topic/1, stop/0, lookup_topic/2, lookup_topic_id/2]).

-compile(export_all).

all() -> [register_topic_test].


init_per_suite(Config) ->
    application:start(lager),
    Config.

end_per_suite(_Config) ->
    application:stop(lager).



register_topic_test(_Config) ->
    start_link(),
    register_topic(<<"ClientId">>, 1, <<"Topic1">>),
    register_topic(<<"ClientId">>, 2, <<"Topic2">>),
    ?assertEqual(<<"Topic1">>, lookup_topic(<<"ClientId">>, 1)),
    ?assertEqual(<<"Topic2">>, lookup_topic(<<"ClientId">>, 2)),
    ?assertEqual(1, lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(2, lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, 1)),
    ?assertEqual(undefined, lookup_topic(<<"ClientId">>, 2)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    stop().




