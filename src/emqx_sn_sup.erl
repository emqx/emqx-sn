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

-module(emqx_sn_sup).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(supervisor).

-export([start_link/5, init/1]).

-define(CHILD(I), {I, {I, start_link, []}, permanent, 5000, worker, [I]}).

start_link(Listener, Duration, GwId, EnableStats, PredefTopicList) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, [Listener, Duration, GwId, EnableStats, PredefTopicList]).

init([{Port, Opts}, Duration, GwId, EnableStats, PredefTopicList]) ->

    BcSrv = {emqx_sn_broadcast,
                {emqx_sn_broadcast, start_link, [[Duration, GwId]]},
                    permanent, brutal_kill, worker, [emqx_sn_broadcast]},

    GwSup = {emqx_sn_gateway_sup,
                {emqx_sn_gateway_sup, start_link, []},
                    permanent, infinity, supervisor, [emqx_sn_gateway_sup]},

    MFA = {emqx_sn_gateway_sup, start_gateway, [GwId, EnableStats]},

    UdpSrv = {emqx_sn_udp_server,
                 {esockd_udp, server, [mqtt_sn, Port, Opts, MFA]},
                     permanent, 5000, worker, [esockd_udp]},

    PreDefTopics = {emqx_sn_predefined_topics,
                       {emqx_sn_predefined_topics, start_link, [PredefTopicList]},
                           permanent, 5000, worker, [emqx_sn_predefined_topics]},

    {ok, { {one_for_all, 10, 3600}, [BcSrv, ?CHILD(emqx_sn_normal_topics), GwSup, UdpSrv, PreDefTopics] }}.

