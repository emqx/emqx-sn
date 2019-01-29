%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_config).

-export([get_env/1, get_env/2, register/0, unregister/0]).

-define(APP, emqx_sn).

get_env(Par) ->
    application:get_env(?APP, Par).

get_env(Par, Default) ->
    application:get_env(?APP, Par, Default).

register() ->
    clique_config:load_schema([code:priv_dir(?APP)], ?APP),
    register_config().

unregister() ->
    unregister_config(),
    clique_config:unload_schema(?APP).

register_config() ->
    Keys = keys(),
    [clique:register_config(Key , fun config_callback/2) || Key <- Keys],
    clique:register_config_whitelist(Keys, ?APP).

config_callback([_, _, "username"], Value) ->
    application:set_env(?APP, username, list_to_binary(Value)),
    " successfully\n";
config_callback([_, _, "password"], Value) ->
    application:set_env(?APP, password, list_to_binary(Value)),
    " successfully\n";
config_callback([_, _, Key], Value) ->
    application:set_env(?APP, list_to_atom(Key), Value),
    " successfully\n".

unregister_config() ->
    Keys = keys(),
    [clique:unregister_config(Key) || Key <- Keys],
    clique:unregister_config_whitelist(Keys, ?APP).

keys() ->
    ["mqtt.sn.port",
     "mqtt.sn.advertise_duration",
     "mqtt.sn.gateway_id",
     "mqtt.sn.enable_stats",
     "mqtt.sn.enable_qos3",
     "mqtt.sn.username",
     "mqtt.sn.password"].

