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

-module(emqx_sn_gateway_sup).

-behaviour(supervisor).

-export([start_link/0, start_gateway/3, init/1]).

-spec(start_link() -> {ok, pid()}).
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a MQTT-SN Gateway.
-spec(start_gateway(inet:socket(), {inet:ip_address(), inet:port()},
                    pos_integer()) -> {ok, pid()}).
start_gateway(Sock, Peer, GwId) ->
    supervisor:start_child(?MODULE, [Sock, Peer, GwId]).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    Gateway = #{id       => emqx_sn_gateway,
                start    => {emqx_sn_gateway, start_link, []},
                restart  => temporary,
                shutdown => 5000,
                type     => worker,
                modules  => [emqx_sn_gateway]},
    {ok, {{simple_one_for_one, 0, 1}, [Gateway]}}.

