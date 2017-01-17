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

-module(emq_sn_message_SUITE).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("eunit/include/eunit.hrl").
-include("emq_sn.hrl").

-import(emq_sn_message, [parse/1, serialize/1]).

-compile(export_all).

all() -> [advertise_test, searchgw_test].


init_per_suite(Config) ->
    application:start(lager),
    Config.

end_per_suite(_Config) ->
    application:stop(lager).





advertise_test(_Config) ->
    Adv = ?SN_ADVERTISE_MSG(1, 100),
    ?assertEqual({ok, Adv}, parse(serialize(Adv))).

searchgw_test(_Config) ->
    Sgw = #mqtt_sn_message{type = ?SN_SEARCHGW, variable = 1},
    ?assertEqual({ok, Sgw}, parse(serialize(Sgw))).

gwinfo_test() ->
    GwInfo = #mqtt_sn_message{type = ?SN_GWINFO, variable = {2, <<"EMQGW">>}},
    ?assertEqual({ok, GwInfo}, parse(serialize(GwInfo))).

connect_test() ->
    Flags = #mqtt_sn_flags{will = true, clean_session = true},
    Conn = #mqtt_sn_message{type = ?SN_CONNECT, variable = {Flags, 4, 300, <<"ClientId">>}},
    ?assertEqual({ok, Conn}, parse(serialize(Conn))).

connack_test() ->
    ConnAck = #mqtt_sn_message{type = ?SN_CONNACK, variable = 2},
    ?assertEqual({ok, ConnAck}, parse(serialize(ConnAck))).

willtopicreq_test() ->
    WtReq = #mqtt_sn_message{type = ?SN_WILLTOPICREQ},
    ?assertEqual({ok, WtReq}, parse(serialize(WtReq))).

willtopic_test() ->
    Flags = #mqtt_sn_flags{qos = 1, retain = false},
    Wt = #mqtt_sn_message{type = ?SN_WILLTOPIC, variable = {Flags, <<"WillTopic">>}},
    ?assertEqual({ok, Wt}, parse(serialize(Wt))).

willmsgreq_test() ->
    WmReq = #mqtt_sn_message{type = ?SN_WILLMSGREQ},
    ?assertEqual({ok, WmReq}, parse(serialize(WmReq))).

willmsg_test() ->
    WlMsg = #mqtt_sn_message{type = ?SN_WILLMSG, variable = <<"WillMsg">>},
    ?assertEqual({ok, WlMsg}, parse(serialize(WlMsg))).

register_test() ->
    RegMsg = ?SN_REGISTER_MSG(1, 2, <<"Topic">>),
    ?assertEqual({ok, RegMsg}, parse(serialize(RegMsg))).

regack_test() ->
    RegAck = ?SN_REGACK_MSG(1, 2, 0),
    ?assertEqual({ok, RegAck}, parse(serialize(RegAck))).

publish_test() ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, retain = false, topic_id_type = 2#01},
    PubMsg = #mqtt_sn_message{type = ?SN_PUBLISH, variable = {Flags, 1, 2, <<"Payload">>}},
    ?assertEqual({ok, PubMsg}, parse(serialize(PubMsg))).

puback_test() ->
    PubAck = #mqtt_sn_message{type = ?SN_PUBACK, variable = {1, 2, 0}},
    ?assertEqual({ok, PubAck}, parse(serialize(PubAck))).

pubrec_test() ->
    PubRec =  #mqtt_sn_message{type = ?SN_PUBREC, variable = 16#1234},
    ?assertEqual({ok, PubRec}, parse(serialize(PubRec))).

pubrel_test() ->
    PubRel =  #mqtt_sn_message{type = ?SN_PUBREL, variable = 16#1234},
    ?assertEqual({ok, PubRel}, parse(serialize(PubRel))).

pubcomp_test() ->
    PubComp =  #mqtt_sn_message{type = ?SN_PUBCOMP, variable = 16#1234},
    ?assertEqual({ok, PubComp}, parse(serialize(PubComp))).

subscribe_test() ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = 16#01},
    SubMsg = #mqtt_sn_message{type = ?SN_SUBSCRIBE, variable = {Flags, 16#4321, 16}},
    ?assertEqual({ok, SubMsg}, parse(serialize(SubMsg))).

suback_test() ->
    Flags = #mqtt_sn_flags{qos = 1},
    SubAck = #mqtt_sn_message{type = ?SN_SUBACK, variable = {Flags, 98, 89, 0}},
    ?assertEqual({ok, SubAck}, parse(serialize(SubAck))).

unsubscribe_test() ->
    Flags = #mqtt_sn_flags{dup = false, qos = 1, topic_id_type = 16#01},
    UnSub = #mqtt_sn_message{type = ?SN_UNSUBSCRIBE, variable = {Flags, 16#4321, 16}},
    ?assertEqual({ok, UnSub}, parse(serialize(UnSub))).

unsuback_test() ->
    UnsubAck = #mqtt_sn_message{type = ?SN_UNSUBACK, variable = 72},
    ?assertEqual({ok, UnsubAck}, parse(serialize(UnsubAck))).

pingreq_test() ->
    Ping = #mqtt_sn_message{type = ?SN_PINGREQ, variable = <<>>},
    ?assertEqual({ok, Ping}, parse(serialize(Ping))),
    Ping1 = #mqtt_sn_message{type = ?SN_PINGREQ, variable = <<"ClientId">>},
    ?assertEqual({ok, Ping1}, parse(serialize(Ping1))).

pingresp_test() ->
    PingResp = #mqtt_sn_message{type = ?SN_PINGRESP},
    ?assertEqual({ok, PingResp}, parse(serialize(PingResp))).

disconnect_test() ->
    Disconn = #mqtt_sn_message{type = ?SN_DISCONNECT},
    ?assertEqual({ok, Disconn}, parse(serialize(Disconn))).

willtopicupd_test() ->
    Flags = #mqtt_sn_flags{qos = 1, retain = true},
    WtUpd = #mqtt_sn_message{type = ?SN_WILLTOPICUPD, variable = {Flags, <<"Topic">>}},
    ?assertEqual({ok, WtUpd}, parse(serialize(WtUpd))).

willmsgupd_test() ->
    WlMsgUpd = #mqtt_sn_message{type = ?SN_WILLMSGUPD, variable = <<"WillMsg">>},
    ?assertEqual({ok, WlMsgUpd}, parse(serialize(WlMsgUpd))).

willmsgresp_test() ->
    UpdResp = #mqtt_sn_message{type = ?SN_WILLMSGRESP, variable = 0},
    ?assertEqual({ok, UpdResp}, parse(serialize(UpdResp))).


