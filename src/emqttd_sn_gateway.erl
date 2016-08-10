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

-module(emqttd_sn_gateway).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_fsm).

-include("emqttd_sn.hrl").

-include_lib("emqttd/include/emqttd_protocol.hrl").

%% API.
-export([start_link/2]).

%% SUB/UNSUB Asynchronously. Called by plugins.
-export([subscribe/2, unsubscribe/2]).

%% gen_fsm.

-export([idle/2, idle/3, wait_for_will_topic/2, wait_for_will_topic/3,
         wait_for_will_msg/2, wait_for_will_msg/3, connected/2, connected/3]).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(state, {gwid, gwinfo = <<>>, sock, peer, protocol, client_id, keepalive, connpkt}).

-define(LOG(Level, Format, Args, State),
            lager:Level("MQTT-SN(~s): " ++ Format,
                        [esockd_net:format(State#state.peer) | Args])).

-spec(start_link(inet:socket(), {inet:ip_address(), inet:port()}) -> {ok, pid()}).
start_link(Sock, Peer) ->
	gen_fsm:start_link(?MODULE, [Sock, Peer], []).

%% TODO:

subscribe(GwPid, TopicTable) ->
    gen_fsm:send_event(GwPid, {subscribe, TopicTable}).

unsubscribe(GwPid, Topics) ->
    gen_fsm:send_event(GwPid, {unsubscribe, Topics}).

%% TODO:

%% gen_fsm.
init([Sock, Peer]) ->
    put(sn_gw, Peer), %%TODO:
    State = #state{gwid = 1, sock = Sock, peer = Peer},
    SendFun = fun(Packet) -> send_message(transform(Packet), State) end,
    PktOpts = [{max_clientid_len, 24}, {max_packet_size, 256}],
    ProtoState = emqttd_protocol:init(Peer, SendFun, PktOpts),
    {ok, idle, State#state{protocol = ProtoState}, 3000}.

idle(timeout, StateData) ->
    {stop, idle_timeout, StateData};

idle(?SN_SEARCHGW_MSG(_Radius), StateData = #state{gwid = GwId, gwinfo = GwInfo}) ->
    send_message(?SN_GWINFO_MSG(GwId, GwInfo), StateData),
    {next_state, idle, StateData};

idle(?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId), StateData = #state{protocol = Proto}) ->
    %%TODO:
    #mqtt_sn_flags{will = Will, clean_session = CleanSession} = Flags,
    ConnPkt = #mqtt_packet_connect{client_id  = ClientId,
                                   clean_sess = CleanSession,
                                   keep_alive = Duration},
    case Will of
        true  ->
            send_message(?SN_WILLTOPICREQ_MSG(), StateData#state{connpkt = ConnPkt}),
            {next_state, wait_for_will_topic, StateData#state{connpkt = ConnPkt}};
        false ->
            case emqttd_protocol:received(?CONNECT_PACKET(ConnPkt), Proto) of
                {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
                {error, Error}         -> shutdown(Error, StateData);
                {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
                {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
            end
    end;

idle(Event, StateData) ->
    %%TODO:...
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
	{next_state, idle, StateData}.

wait_for_will_topic(?SN_WILLTOPIC_MSG(Flags, Topic), StateData = #state{connpkt = ConnPkt}) ->
    #mqtt_sn_flags{qos = Qos, retain = Retain} = Flags,
    ConnPkt1 = ConnPkt#mqtt_packet_connect{will_retain = Retain,
                                           will_qos    = Qos,
                                           will_topic  = Topic},
    send_message(?SN_WILLMSGREQ_MSG(), StateData),
    {next_state, wait_for_will_msg, StateData#state{connpkt = ConnPkt1}};

wait_for_will_topic(_Event, StateData) ->
    %%TODO: LOG error
    {next_state, wait_for_will_topic, StateData}.

wait_for_will_msg(?SN_WILLMSG_MSG(Msg), StateData = #state{protocol = Proto, connpkt = ConnPkt}) ->
    %%TODO: protocol connect
    ConnPkt1 = ConnPkt#mqtt_packet_connect{will_msg = Msg},
    case emqttd_protocol:received(?CONNECT_PACKET(ConnPkt1), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

wait_for_will_msg(Event, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
    {next_state, wait_for_will_msg, StateData}.

connected(?SN_REGISTER_MSG(TopicId, MsgId, TopicName), StateData = #state{client_id = ClientId}) ->
    emqttd_sn_registry:register(ClientId, TopicId, TopicName),
    send_message(?SN_REGACK_MSG(TopicId, MsgId, 0), StateData),
	{next_state, connected, StateData};

connected(?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data), StateData = #state{client_id = ClientId, protocol = Proto}) ->
    #mqtt_sn_flags{dup = Dup, qos = Qos, retain = Retain} = Flags,
    case emqttd_sn_registry:lookup_Topic(ClientId, TopicId) of
        undefined ->
            send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), StateData);
        TopicName -> 
            Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = Qos, retain = Retain},
                                   variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId},
                                   payload  = Data},
            case emqttd_protocol:received(Publish, Proto) of
                {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
                {error, Error}         -> shutdown(Error, StateData);
                {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
                {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
            end
    end;

connected(?SN_PUBACK_MSG(_TopicId, MsgId, _ReturnCode), StateData = #state{protocol = Proto}) ->
    case emqttd_protocol:received(?PUBACK_PACKET(?PUBACK, MsgId), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

connected(?SN_PUBREC_MSG(PubRec, MsgId), StateData = #state{protocol = Proto})
    when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    case emqttd_protocol:received(?PUBACK_PACKET(PubRec, MsgId), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

%%TODO: TopicId -> Name
connected(?SN_SUBSCRIBE_MSG(Flags, MsgId, Topic), StateData = #state{protocol = Proto}) ->
    #mqtt_sn_flags{qos = Qos, topic_id_type = _TopicIdType} = Flags,
    case emqttd_protocol:received(?SUBSCRIBE_PACKET(MsgId, [{Topic, Qos}]), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

%%TODO: TopicId -> Name
connected(?SN_UNSUBSCRIBE_MSG(_Flags, MsgId, Topic), StateData = #state{protocol = Proto}) ->
    case emqttd_protocol:received(?UNSUBSCRIBE_PACKET(MsgId, [Topic]), Proto) of
        {ok, Proto1}           -> next_state(connected, StateData#state{protocol = Proto1});
        {error, Error}         -> shutdown(Error, StateData);
        {error, Error, Proto1} -> shutdown(Error, StateData#state{protocol = Proto1});
        {stop, Reason, Proto1} -> stop(Reason, StateData#state{protocol = Proto1})
    end;

connected(?SN_PINGREQ_MSG(_ClientId), StateData) ->
    send_message(?SN_PINGRESP_MSG(), StateData),
    next_state(connected, StateData);

connected(?SN_DISCONNECT_MSG(_Duration), StateData = #state{protocol = Proto}) ->
    {stop, Reason, Proto1} = emqttd_protocol:received(?PACKET(?DISCONNECT), Proto),
    stop(Reason, StateData#state{protocol = Proto1});

connected(Event, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
	{next_state, connected, StateData}.

handle_event(Event, StateName, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
	{next_state, StateName, StateData}.

idle(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
	{reply, ignored, idle, StateData}.

wait_for_will_topic(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
	{reply, ignored, wait_for_will_topic, StateData}.

wait_for_will_msg(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
	{reply, ignored, wait_for_will_msg, StateData}.

connected(Event, _From, StateData) ->
    ?LOG(error, "UNEXPECTED Event: ~p", [Event], StateData),
	{reply, ignored, state_name, StateData}.

handle_sync_event(Event, _From, StateName, StateData) ->
    ?LOG(error, "UNEXPECTED SYNC Event: ~p", [Event], StateData),
	{reply, ignored, StateName, StateData}.

handle_info({datagram, Data}, StateName, StateData) ->
    {ok, Msg} = emqttd_sn_message:parse(Data),
    ?LOG(info, "RECV ~p", [Msg], StateData),
    ?MODULE:StateName(Msg, StateData); %% cool?

%% Asynchronous SUBACK
handle_info({suback, PacketId, [GrantedQos]}, StateName, StateData) ->
    Flags = #mqtt_sn_flags{qos = GrantedQos},
    send_message(?SN_SUBACK_MSG(Flags, 1, PacketId, 0), StateData),
    next_state(StateName, StateData);

handle_info(Info, StateName, StateData) ->
    ?LOG(error, "UNEXPECTED INFO: ~p", [Info], StateData),
	{next_state, StateName, StateData}.

terminate(_Reason, _StateName, _StateData = #state{client_id = ClientId}) ->
    emqttd_sn_registry:unregister_topic(ClientId).

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.

transform(?CONNACK_PACKET(0)) ->
    ?SN_CONNACK_MSG(0);

transform(?CONNACK_PACKET(_ReturnCode)) ->
    ?SN_CONNACK_MSG(?SN_RC_CONGESTION);

transform(?PUBACK_PACKET(?PUBACK, PacketId)) ->
    ?SN_PUBACK_MSG(1, PacketId, 0);

transform(?PUBACK_PACKET(?PUBREC, PacketId)) ->
    ?SN_PUBREC_MSG(?SN_PUBREC, PacketId);

transform(?PUBACK_PACKET(?PUBREL, PacketId)) ->
    ?SN_PUBREC_MSG(?SN_PUBREL, PacketId);

transform(?PUBACK_PACKET(?PUBCOMP, PacketId)) ->
    ?SN_PUBREC_MSG(?SN_PUBCOMP, PacketId).

send_message(Msg, StateData = #state{sock = Sock, peer = {Host, Port}}) ->
    ?LOG(debug, "SEND ~p~n", [Msg], StateData),
    gen_udp:send(Sock, Host, Port, emqttd_sn_message:serialize(Msg)).

next_state(StateName, StateData) ->
    {next_state, StateName, StateData, hibernate}.

shutdown(Error, StateData) ->
    {stop, {shutdown, Error}, StateData}.

stop(Reason, StateData) ->
    {stop, Reason, StateData}.

