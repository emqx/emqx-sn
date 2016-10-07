%%--------------------------------------------------------------------
%% Copyright (c) 2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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

-module(emq_sn_message).

-author("Feng Lee <feng@emqtt.io>").

-include("emq_sn.hrl").

-export([parse/1, serialize/1]).

-define(flag,  1/binary).
-define(byte,  8/big-integer).
-define(short, 16/big-integer).

%%--------------------------------------------------------------------
%% Parse MQTT-SN Message
%%--------------------------------------------------------------------

parse(<<16#01:?byte, Len:?short, Type:?byte, Var/binary>>) ->
    parse(Type, Len - 4, Var);
parse(<<Len:?byte, Type:?byte, Var/binary>>) ->
    parse(Type, Len - 2, Var).

parse(Type, Len, Var) when Len =:= size(Var) ->
    {ok, #mqtt_sn_message{type = Type, variable = parse_var(Type, Var)}}.

parse_var(?SN_ADVERTISE, <<GwId:?byte, Duration:?short>>) ->
    {GwId, Duration};
parse_var(?SN_SEARCHGW, <<Radius:?byte>>) ->
    Radius;
parse_var(?SN_GWINFO, <<GwId:?byte, GwAdd/binary>>) ->
    {GwId, GwAdd};
parse_var(?SN_CONNECT, <<Flags:?flag, ProtocolId:?byte, Duration:?short, ClientId/binary>>) ->
    {parse_flags(?SN_CONNECT, Flags), ProtocolId, Duration, ClientId};
parse_var(?SN_CONNACK, <<ReturnCode:?byte>>) ->
    ReturnCode;
parse_var(?SN_WILLTOPICREQ, <<>>) ->
    undefined;
parse_var(?SN_WILLTOPIC, <<Flags:?flag, WillTopic/binary>>) ->
    {parse_flags(?SN_WILLTOPIC, Flags), WillTopic};
parse_var(?SN_WILLMSGREQ, <<>>) ->
    undefined;
parse_var(?SN_WILLMSG, <<WillMsg/binary>>) ->
    WillMsg;
parse_var(?SN_REGISTER, <<TopicId:?short, MsgId:?short, TopicName/binary>>) ->
    {TopicId, MsgId, TopicName};
parse_var(?SN_REGACK, <<TopicId:?short, MsgId:?short, ReturnCode:?byte>>) ->
    {TopicId, MsgId, ReturnCode};
parse_var(?SN_PUBLISH, <<Flags:?flag, TopicId:?short, MsgId:?short, Data/binary>>) ->
    {parse_flags(?SN_PUBLISH, Flags), TopicId, MsgId, Data};
parse_var(?SN_PUBACK, <<TopicId:?short, MsgId:?short, ReturnCode:?byte>>) ->
    {TopicId, MsgId, ReturnCode};
parse_var(PubRec, <<MsgId:?short>>) when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    MsgId;
parse_var(Sub, <<FlagsBin:?flag, MsgId:?short, Topic/binary>>) when Sub == ?SN_SUBSCRIBE; Sub == ?SN_UNSUBSCRIBE ->
    #mqtt_sn_flags{topic_id_type = IdType} = Flags = parse_flags(Sub, FlagsBin),
    {Flags, MsgId, parse_topic(IdType, Topic)};
parse_var(?SN_SUBACK, <<Flags:?flag, TopicId:?short, MsgId:?short, ReturnCode:?byte>>) ->
    {parse_flags(?SN_SUBACK, Flags), TopicId, MsgId, ReturnCode};
parse_var(?SN_UNSUBACK, <<MsgId:?short>>) ->
    MsgId;
parse_var(?SN_PINGREQ, ClientId) ->
    ClientId;
parse_var(?SN_PINGRESP, _) ->
    undefined;
parse_var(?SN_DISCONNECT, <<>>) ->
    undefined;
parse_var(?SN_DISCONNECT, <<Duration:?short>>) ->
    Duration;
parse_var(?SN_WILLTOPICUPD, <<Flags:?flag, WillTopic/binary>>) ->
    {parse_flags(?SN_WILLTOPICUPD, Flags), WillTopic};
parse_var(?SN_WILLMSGUPD, WillMsg) ->
    WillMsg;
parse_var(?SN_WILLTOPICRESP, <<ReturnCode:?byte>>) ->
    ReturnCode;
parse_var(?SN_WILLMSGRESP, <<ReturnCode:?byte>>) ->
    ReturnCode.
 
parse_flags(?SN_CONNECT, <<_D:1, _Q:2, _R:1, Will:1, CleanSession:1, _IdType:2>>) ->
    #mqtt_sn_flags{will = bool(Will), clean_session = bool(CleanSession)};
parse_flags(?SN_WILLTOPIC, <<_D:1, Qos:2, Retain:1, _Will:1, _C:1, _:2>>) ->
    #mqtt_sn_flags{qos = Qos, retain = bool(Retain)};
parse_flags(?SN_PUBLISH, <<Dup:1, Qos:2, Retain:1, _Will:1, _C:1, IdType:2>>) ->
    #mqtt_sn_flags{dup = bool(Dup), qos = Qos, retain = bool(Retain), topic_id_type = IdType};
parse_flags(Sub, <<Dup:1, Qos:2, _R:1, _Will:1, _C:1, IdType:2>>)
    when Sub == ?SN_SUBSCRIBE; Sub == ?SN_UNSUBSCRIBE ->
    #mqtt_sn_flags{dup = bool(Dup), qos = Qos, topic_id_type = IdType};
parse_flags(?SN_SUBACK, <<_D:1, Qos:2, _R:1, _W:1, _C:1, _Id:2>>) ->
    #mqtt_sn_flags{qos = Qos};
parse_flags(?SN_WILLTOPICUPD, <<_D:1, Qos:2, Retain:1, _W:1, _C:1, _Id:2>>) ->
    #mqtt_sn_flags{qos = Qos, retain = bool(Retain)}.

parse_topic(2#00, Topic)     -> Topic;
parse_topic(2#01, <<Id:16>>) -> Id;
parse_topic(2#10, Topic)     -> Topic;
parse_topic(2#11, Topic)     -> Topic.

%%--------------------------------------------------------------------
%% Serialize MQTT-SN Message
%%--------------------------------------------------------------------

serialize(#mqtt_sn_message{type = Type, variable = Var}) ->
    VarBin = serialize(Type, Var), VarLen = size(VarBin),
    if
        VarLen < 254 -> <<(VarLen + 2), Type, VarBin/binary>>;
        true         -> <<16#01, (VarLen + 4):?short, Type, VarBin/binary>>
    end.

serialize(?SN_ADVERTISE, {GwId, Duration}) ->
    <<GwId, Duration:?short>>;
serialize(?SN_SEARCHGW, Radius) ->
    <<Radius>>;
serialize(?SN_GWINFO, {GwId, GwAdd}) ->
    <<GwId, GwAdd/binary>>;
serialize(?SN_CONNECT, {Flags, ProtocolId, Duration, ClientId}) ->
    <<(serialize_flags(Flags))/binary, ProtocolId, Duration:?short, ClientId/binary>>;
serialize(?SN_CONNACK, ReturnCode) ->
    <<ReturnCode>>;
serialize(?SN_WILLTOPICREQ, _) ->
    <<>>;
serialize(?SN_WILLTOPIC, {Flags, Topic}) ->
    <<(serialize_flags(Flags))/binary, Topic/binary>>;
serialize(?SN_WILLMSGREQ, _) ->
    <<>>;
serialize(?SN_WILLMSG, WillMsg) ->
    WillMsg;
serialize(?SN_REGISTER, {TopicId, MsgId, TopicName}) ->
    <<TopicId:?short, MsgId:?short, TopicName/binary>>;
serialize(?SN_REGACK, {TopicId, MsgId, ReturnCode}) ->
    <<TopicId:?short, MsgId:?short, ReturnCode>>;
serialize(?SN_PUBLISH, {Flags, TopicId, MsgId, Data}) ->
    <<(serialize_flags(Flags))/binary, TopicId:?short, MsgId:?short, Data/binary>>;
serialize(?SN_PUBACK, {TopicId, MsgId, ReturnCode}) ->
    <<TopicId:?short, MsgId:?short, ReturnCode>>;
serialize(PubRec, MsgId) when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    <<MsgId:?short>>;
serialize(Sub, {Flags = #mqtt_sn_flags{topic_id_type = IdType}, MsgId, Topic})
    when Sub == ?SN_SUBSCRIBE; Sub == ?SN_UNSUBSCRIBE ->
    <<(serialize_flags(Flags))/binary, MsgId:16, (serialize_topic(IdType, Topic))/binary>>;
serialize(?SN_SUBACK, {Flags, TopicId, MsgId, ReturnCode}) ->
    <<(serialize_flags(Flags))/binary, TopicId:?short, MsgId:?short, ReturnCode>>;
serialize(?SN_UNSUBACK, MsgId) ->
    <<MsgId:?short>>;
serialize(?SN_PINGREQ, ClientId) ->
    ClientId;
serialize(?SN_PINGRESP, _) ->
    <<>>;
serialize(?SN_WILLTOPICUPD, {Flags, WillTopic}) ->
    <<(serialize_flags(Flags))/binary, WillTopic/binary>>;
serialize(?SN_WILLMSGUPD, WillMsg) ->
    WillMsg;
serialize(?SN_WILLTOPICRESP, ReturnCode) ->
    <<ReturnCode>>;
serialize(?SN_WILLMSGRESP, ReturnCode) ->
    <<ReturnCode>>;
serialize(?SN_DISCONNECT, undefined) ->
    <<>>;
serialize(?SN_DISCONNECT, Duration) ->
    <<Duration:?short>>.

serialize_flags(#mqtt_sn_flags{dup = Dup, qos = Qos, retain = Retain, will = Will,
                               clean_session = CleanSession, topic_id_type = IdType}) ->
    <<(bool(Dup)):1, (i(Qos)):2, (bool(Retain)):1, (bool(Will)):1, (bool(CleanSession)):1, (i(IdType)):2>>.

serialize_topic(2#00, Topic) -> Topic;
serialize_topic(2#01, Id)    -> <<Id:?short>>;
serialize_topic(2#10, Topic) -> Topic;
serialize_topic(2#11, Topic) -> Topic.


bool(0) -> false;
bool(1) -> true;
bool(false) -> 0;
bool(true)  -> 1;
bool(undefined) -> 0.

i(undefined) -> 0;
i(I) when is_integer(I) -> I.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

advertise_test() ->
    Adv = ?SN_ADVERTISE_MSG(1, 100),
    ?assertEqual({ok, Adv}, parse(serialize(Adv))).

searchgw_test() ->
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

-endif.

