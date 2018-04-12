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

-module(emqx_sn_app).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(application).

-export([start/2, stop/1]).

-define(APP, emqx_sn).

-define(LOG(Level, Format, Args),
    lager:Level("MQTT-SN(app): " ++ Format, Args)).
%%--------------------------------------------------------------------
%% Application Callback
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    Port = application:get_env(?APP, port, 1884),
    Duration = application:get_env(?APP, advertise_duration, 15*60),
    GwId = application:get_env(?APP, gateway_id, 1),
    EnableStats = application:get_env(?APP, enable_stats, false),
    PredefTopicList = application:get_env(?APP, predefined, []),
    ?LOG(debug, "The PredefTopicList is ~p~n", [PredefTopicList]),
    emqx_sn_config:register(),
    emqx_sn_sup:start_link({Port, []}, Duration, GwId, EnableStats, PredefTopicList).

stop(_State) ->
    emqx_sn_config:unregister(),
    ok.

