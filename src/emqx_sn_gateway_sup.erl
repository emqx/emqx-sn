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

-module(emqx_sn_gateway_sup).

-behaviour(supervisor).

-export([ start_link/1
        , start_gateway/2
        , init/1
        ]).

-spec(start_link(pos_integer()) -> {ok, pid()}).
start_link(GwId) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, [GwId]).

%% @doc Start a MQTT-SN Gateway.
-spec(start_gateway(esockd:udp_transport(), {inet:ip_address(), inet:port()})
      -> {ok, pid()} | {error, term()}).
start_gateway(Transport, Peer) ->
    supervisor:start_child(?MODULE, [Transport, Peer]).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([GwId]) ->
    Gateway = #{id       => emqx_sn_gateway,
                start    => {emqx_sn_gateway, start_link, [GwId]},
                restart  => temporary,
                shutdown => 5000,
                type     => worker,
                modules  => [emqx_sn_gateway]},
    {ok, {{simple_one_for_one, 0, 1}, [Gateway]}}.

