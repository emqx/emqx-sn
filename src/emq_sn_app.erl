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

-module(emq_sn_app).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(application).

-export([start/2, stop/1]).

-define(APP, emq_sn).

%%--------------------------------------------------------------------
%% Application Callback
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    Listener = application:get_env(?APP, listener, {1884, []}),
    Duration = application:get_env(?APP, advertise_duration, 15*60),
    GwId = application:get_env(?APP, gateway_id, 1),
    emq_sn_sup:start_link(Listener, Duration, GwId).

stop(_State) ->
	ok.

