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

-module(emq_sn_message).

-author("Feng Lee <feng@emqtt.io>").

-include("emq_sn.hrl").

-export([parse/1, serialize/1, message_type/1, format/1]).

-define(flag,  1/binary).
-define(byte,  8/big-integer).
-define(short, 16/big-integer).

-define(LOG(Level, Format, Args),
    lager:Level("MQTT-SN(message): " ++ Format, Args)).

%%--------------------------------------------------------------------
%% Parse MQTT-SN Message
%%--------------------------------------------------------------------

parse(<<16#01:?byte, Len:?short, Type:?byte, Var/binary>>) ->
    parse(Type, Len - 4, Var);
parse(<<Len:?byte, Type:?byte, Var/binary>>) ->
    parse(Type, Len - 2, Var).

parse(Type, Len, Var) when Len =:= size(Var) ->
    {ok, #mqtt_sn_message{type = Type, variable = parse_var(Type, Var)}};
parse(Type, Len, Var) ->
    ?LOG(error, "format error: type=~p, len=~p, var=~p", [Type, Len, Var]),
    error(format_error).

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
parse_var(?SN_WILLTOPIC, <<>>) ->
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
parse_var(?SN_WILLTOPICUPD, <<>>) ->
    {undefined, undefined};
parse_var(?SN_WILLTOPICUPD, <<Flags:?flag, WillTopic/binary>>) ->
    {parse_flags(?SN_WILLTOPICUPD, Flags), WillTopic};
parse_var(?SN_WILLMSGUPD, WillMsg) ->
    WillMsg;
parse_var(?SN_WILLTOPICRESP, <<ReturnCode:?byte>>) ->
    ReturnCode;
parse_var(?SN_WILLMSGRESP, <<ReturnCode:?byte>>) ->
    ReturnCode;
parse_var(_Type, _Var) ->
    error(format_error).
 
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
    #mqtt_sn_flags{qos = Qos, retain = bool(Retain)};
parse_flags(_Type, _) ->
    error(format_error).

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
%serialize(?SN_PUBLISH, {Flags, TopicId, MsgId, Data}) ->
%    <<(serialize_flags(Flags))/binary, TopicId/binary, MsgId:?short, Data/binary>>;
serialize(?SN_PUBLISH, {Flags=#mqtt_sn_flags{topic_id_type = ?SN_NORMAL_TOPIC}, TopicId, MsgId, Data}) ->
    <<(serialize_flags(Flags))/binary, TopicId:?short, MsgId:?short, Data/binary>>;
serialize(?SN_PUBLISH, {Flags=#mqtt_sn_flags{topic_id_type = ?SN_PREDEFINED_TOPIC}, TopicId, MsgId, Data}) ->
    <<(serialize_flags(Flags))/binary, TopicId:?short, MsgId:?short, Data/binary>>;
serialize(?SN_PUBLISH, {Flags=#mqtt_sn_flags{topic_id_type = ?SN_SHORT_TOPIC}, TopicId, MsgId, Data}) ->
    <<(serialize_flags(Flags))/binary, TopicId:2/binary, MsgId:?short, Data/binary>>;
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



message_type(16#00) ->
    "SN_ADVERTISE";
message_type(16#01) ->
    "SN_SEARCHGW";
message_type(16#02) ->
    "SN_GWINFO";
message_type(16#04) ->
    "SN_CONNECT";
message_type(16#05) ->
    "SN_CONNACK";
message_type(16#06) ->
    "SN_WILLTOPICREQ";
message_type(16#07) ->
    "SN_WILLTOPIC";
message_type(16#08) ->
    "SN_WILLMSGREQ";
message_type(16#09) ->
    "SN_WILLMSG";
message_type(16#0a) ->
    "SN_REGISTER";
message_type(16#0b) ->
    "SN_REGACK";
message_type(16#0c) ->
    "SN_PUBLISH";
message_type(16#0d) ->
    "SN_PUBACK";
message_type(16#0e) ->
    "SN_PUBCOMP";
message_type(16#0f) ->
    "SN_PUBREC";
message_type(16#10) ->
    "SN_PUBREL";
message_type(16#12) ->
    "SN_SUBSCRIBE";
message_type(16#13) ->
    "SN_SUBACK";
message_type(16#14) ->
    "SN_UNSUBSCRIBE";
message_type(16#15) ->
    "SN_UNSUBACK";
message_type(16#16) ->
    "SN_PINGREQ";
message_type(16#17) ->
    "SN_PINGRESP";
message_type(16#18) ->
    "SN_DISCONNECT";
message_type(16#1a) ->
    "SN_WILLTOPICUPD";
message_type(16#1b) ->
    "SN_WILLTOPICRESP";
message_type(16#1c) ->
    "SN_WILLMSGUPD";
message_type(16#1d) ->
    "SN_WILLMSGRESP";
message_type(Type) ->
    io_lib:format("Unknown Type ~p", [Type]).






format(?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_PUBLISH, ~p, TopicId=~w, MsgId=~w, Payload=~w",
        [format_flag(Flags), TopicId, MsgId, Data]));
format(?SN_PUBACK_MSG(Flags, MsgId, ReturnCode)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_PUBACK, ~p, MsgId=~w, ReturnCode=~w",
        [format_flag(Flags), MsgId, ReturnCode]));
format(?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_PUBCOMP, MsgId=~w", [MsgId]));
format(?SN_PUBREC_MSG(?SN_PUBREC, MsgId)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_PUBREC, MsgId=~w", [MsgId]));
format(?SN_PUBREC_MSG(?SN_PUBREL, MsgId)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_PUBREL, MsgId=~w", [MsgId]));
format(?SN_SUBSCRIBE_MSG(Flags, Msgid, Topic)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_SUBSCRIBE, ~p, MsgId=~w, TopicId=~w",
        [format_flag(Flags), Msgid, Topic]));
format(?SN_SUBACK_MSG(Flags, TopicId, MsgId, ReturnCode)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_SUBACK, ~p, MsgId=~w, TopicId=~w, ReturnCode=~w",
        [format_flag(Flags), MsgId, TopicId, ReturnCode]));
format(?SN_UNSUBSCRIBE_MSG(Flags, Msgid, Topic)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_UNSUBSCRIBE, ~p, MsgId=~w, TopicId=~w",
        [format_flag(Flags), Msgid, Topic]));
format(?SN_UNSUBACK_MSG(MsgId)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_UNSUBACK, MsgId=~w", [MsgId]));
format(?SN_REGISTER_MSG(TopicId, MsgId, TopicName)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_REGISTER, TopicId=~w, MsgId=~w, TopicName=~w",
        [TopicId, MsgId, TopicName]));
format(?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)) ->
    lists:flatten(io_lib:format("mqtt_sn_message SN_REGACK, TopicId=~w, MsgId=~w, ReturnCode=~w",
        [TopicId, MsgId, ReturnCode]));
format(#mqtt_sn_message{type = Type, variable = Var}) ->
    lists:flatten(io_lib:format("mqtt_sn_message type=~s, Var=~w", [emq_sn_message:message_type(Type), Var])).


format_flag(#mqtt_sn_flags{dup = Dup, qos = Qos, retain = Retain, will = Will, clean_session = CleanSession, topic_id_type = TopicType}) ->
    lists:flatten(io_lib:format("mqtt_sn_flags{dup=~p, qos=~p, retain=~p, will=~p, clean_session=~p, topic_id_type=~p}", [Dup, Qos, Retain, Will, CleanSession, TopicType]));
format_flag(_Flag) ->
    "invalid flag".


