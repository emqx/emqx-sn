%%%-------------------------------------------------------------------
%%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emq_sn_asleep_msg_queue).

-author("Feng Lee <feng@emqtt.io>").



-export([init/0, enqueue/2, dequeue/1, size/1]).


-record(asleep_queue,  {msg_queue   :: queue:queue()}).



init() ->
    #asleep_queue{msg_queue = queue:new()}.

enqueue(Msg, Context=#asleep_queue{msg_queue = Que}) ->
    case queue:member(Msg, Que) of
        true  -> Context;  % EMQ core has retransmit this message, since device is sleeping
        false -> Context#asleep_queue{msg_queue = queue:in(Msg, Que)}
    end.

dequeue(Context=#asleep_queue{msg_queue = Que}) ->
    Msg = queue:get(Que),
    {Msg, Context#asleep_queue{msg_queue = queue:drop(Que)}}.

size(#asleep_queue{msg_queue = Que}) ->
    queue:len(Que).

