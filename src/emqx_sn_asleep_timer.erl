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

-module(emqx_sn_asleep_timer).

-export([init/0, start/2, timeout/3]).

-record(asleep_state, {duration :: integer(),
                       timer_on :: boolean(),
                       ref      :: integer()}).

-define(LOG(Level, Format, Args),
        emqx_logger:Level("MQTT-SN(ASLEEP-TIMER): " ++ Format, Args)).

init() ->
    #asleep_state{duration = 0, timer_on = false, ref = 0}.

start(State=#asleep_state{duration = Duration, timer_on = TimerIsRunning, ref = Ref}, undefined) ->
    NewRef = Ref + 1,
    start_timer(TimerIsRunning, Duration, NewRef),
    State#asleep_state{timer_on = true, ref = NewRef};
start(State=#asleep_state{timer_on = TimerIsRunning, ref = Ref}, Duration) ->
    NewRef = Ref + 1,
    start_timer(TimerIsRunning, Duration, NewRef),
    State#asleep_state{duration = Duration, timer_on = true, ref = NewRef}.

timeout(#asleep_state{ref = Ref}, asleep, Ref) ->
    % if two reference are identical, no event happened during device's sleep state
    % terminate process
    ?LOG(debug, "asleep timer timeout decide to terminate Ref=~p", [Ref]),
    terminate_process;
timeout(State=#asleep_state{ref = OtherRef}, asleep, Ref) ->
    % two references are different means this timer has been canceled once.
    % restart asleep timer again since it is still in asleep state
    ?LOG(debug, "asleep timer timeout decide to restart OtherRef=~p, Ref=~p", [OtherRef, Ref]),
    {restart_timer, State#asleep_state{timer_on = false}};
timeout(StateData, StateName, Ref) ->
    % in other state, stop asleep timer
    ?LOG(debug, "asleep timer timeout no decision Ref=~p StateName=~p", [Ref, StateName]),
    {stop_timer, StateData#asleep_state{timer_on = false}}.

start_timer(true, Duration, Ref) ->
    % if an asleep timer has already been running, do not start a new timer
    ?LOG(debug, "asleep start_timer, timer is already running, Duration=~p, NewRef=~p", [Duration, Ref]),
    ok;
start_timer(false, Duration, Ref) ->
    ?LOG(debug, "asleep start_timer Duration=~p, Ref=~p", [Duration, Ref]),
    erlang:send_after(timer:seconds(Duration), self(), {asleep_timeout, Ref}).

