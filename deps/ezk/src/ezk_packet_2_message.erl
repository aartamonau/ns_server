%% -------------------------------------------------------------------
%%
%% ezk_packet_2_message: A module that contains functions to convert
%%                       incoming binary messages into Erlangdata.
%%
%% Copyright (c) 2011 Marco Grebe. All Rights Reserved.
%% Copyright (c) 2011 global infinipool GmbH.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(ezk_packet_2_message).
-export([get_message_typ/1, replymessage_2_reply/2, get_watch_data/1]).
-include("ezk.hrl").

%% First stage of Message Passing.
%% The first part of the Message determines the type (heartbeat, watchevent, reply) and
%% the first part of the Header (which is necessary to find the right entry in
%% open_requests) if it is a reply.
%% Returns {heartbeat, HeartbeatBin} | {watchevent, Payload}
%%       | {normal, MessageId, Zxid, Payload}
get_message_typ(Data) ->
    case Data  of
%%% Heartbeat
        <<255,255,255,254, Heartbeat/binary>> ->
            {heartbeat, Heartbeat};
%%% Watchevents
        <<255,255,255,255, 255,255,255,255, 255,255,255,255 , 0,0,0,0, Payload/binary>> ->
            ?LOG(3, "packet_2_message: A Watchevent arrived"),
            {watchevent, Payload};
        <<255, 255, 255, 252, 0:64, Payload/binary>> ->
            {authreply, Payload};
%%% Normal Replys
        <<MessId:32, Zxid:64, Payload/binary>> ->
            ?LOG(3, "packet_2_message: A normal Message arrived"),
            {normal, MessId, Zxid, Payload}
    end.

%% A message typed as watchevent is processed
%% returns {child, Path, SyncConnected} | {data, Path, SyncConnected}
get_watch_data(Binary) ->
    <<TypInt:32, SyncConnected:32, PackedPath/binary>> = Binary,
    {Path, _Nothing} = unpack(PackedPath),
    case TypInt of
        1 ->
            Typ = node_created;
        2 ->
            Typ = node_deleted;
        3 ->
            Typ = data_changed;
        4 ->
            Typ = child_changed
    end,
    {Typ, binary_to_list(Path), SyncConnected}.

%% Gets a replybinary from the server and returns it as a parsed Erlang tupel.
%% First step is to filter if there was an error and pass it on to the server if there is.
%% If not the interpret_reply_data function is used to interpret the Payload.
replymessage_2_reply(CommId, PayloadWithErrorCode) ->
    ?LOG(1,"packet_2_message: Trying to Interpret payload: ~w", [PayloadWithErrorCode]),
    case PayloadWithErrorCode of
        <<0:32, Payload/binary>> ->
            ?LOG(1
                ,"packet_2_message: Interpreting the payload ~w with commid ~w"
                ,[Payload, CommId]),
            {Reply, <<>>} = interpret_reply_data(CommId, Payload),
            ?LOG(1, "The Reply is ~w",[Reply]),
            Reply;
        <<ErrorCode:32/signed, _/binary>> ->
            {error, map_error(ErrorCode)}
    end.

%% Map server error code to an atom. In reality, not all of these can be
%% returned by server. But for simplicity and convenience we keep them here
%% anyway.
-spec map_error(integer()) -> ezk_err().
map_error(0) -> rolled_back;
map_error(-1) -> system_error;
map_error(-2) -> runtime_inconsistency;
map_error(-3) -> data_inconsistency;
map_error(-4) -> connection_loss;
map_error(-5) -> marshalling_error;
map_error(-6) -> unimplemented;
map_error(-7) -> operation_timeout;
map_error(-8) -> bad_arguments;
map_error(-13) -> new_config_no_quorum;
map_error(-14) -> reconfig_inprogress;
map_error(-100) -> api_error;
map_error(-101) -> no_node;
map_error(-102) -> no_auth;
map_error(-103) -> bad_version;
map_error(-108) -> no_children_for_ephemerals;
map_error(-110) -> node_exists;
map_error(-111) -> not_empty;
map_error(-112) -> session_expired;
map_error(-113) -> invalid_callback;
map_error(-114) -> invalid_acl;
map_error(-115) -> auth_failed;
map_error(-118) -> session_moved;
map_error(-119) -> not_readonly;
map_error(-120) -> ephemeral_on_local_session;
map_error(-121) -> no_watcher;
map_error(Code) -> {unknown_server_error, Code}.


interpret_reply_data(Command, Data) ->
    {Reply0, RestData} = do_interpret_reply_data(Command, Data),
    case Reply0 of
        ok ->
            {ok, RestData};
        _ ->
            {{ok, Reply0}, RestData}
    end.

%% There is a pattern matching on the command id and depending on the command id
%% the Reply is interpreted.
%%% create --> Reply = The new Path
do_interpret_reply_data(1, Reply) ->
    <<LengthOfData:32, ReplyPath:LengthOfData/binary, Rest/binary>> = Reply,
    {binary_to_list(ReplyPath), Rest};
%%% delete --> Reply = Nothing --> use the Path
do_interpret_reply_data(2, Reply) ->
    {ok, Reply};
%%% exists
do_interpret_reply_data(3, Reply) ->
    getbinary_2_stat(Reply);
%%% get --> Reply = The data stored in the node and then all the nodes  parameters
do_interpret_reply_data(4, Reply) ->
    ?LOG(3,"P2M: Got a get reply"),
    <<LengthOfData:32/signed, Data/binary>> = Reply,
    ?LOG(3,"P2M: Length of data is ~w",[LengthOfData]),
    {ReplyData, Left} = case LengthOfData of
                            -1 -> {<<"">>, Data};
                            _ -> split_binary(Data, LengthOfData)
                        end,
    ?LOG(3,"P2M: The Parameterdata is ~w",[Left]),
    ?LOG(3,"P2M: Data is ~w",[ReplyData]),
    {Parameter, Rest} = getbinary_2_stat(Left),
    {{ReplyData, Parameter}, Rest};
%%% set --> Reply = the nodes parameters
do_interpret_reply_data(5, Reply) ->
    getbinary_2_stat(Reply);

%%% get_acl --> A list of the Acls and the nodes parameters
do_interpret_reply_data(6, Reply) ->
    ?LOG(3,"P2M: Got a get acl reply"),
    <<NumberOfAcls:32, Data/binary>> = Reply,
    ?LOG(3,"P2M: There are ~w acls",[NumberOfAcls]),
    {Acls, Data2} = get_n_acls(NumberOfAcls, [],  Data),
    ?LOG(3,"P2M: Acls got parsed: ~w", [Acls]),
    {Parameter, Rest} = getbinary_2_stat(Data2),
    ?LOG(3,"P2M: Data got also parsed."),
    {{Acls, Parameter}, Rest};
%%% set_acl --> Reply = the nodes parameters
do_interpret_reply_data(7, Reply) ->
    getbinary_2_stat(Reply);
%%% ls --> Reply = a list of all children of the node.
do_interpret_reply_data(8, Reply) ->
    ?LOG(4,"packet_2_message: Interpreting a ls"),
    <<NumberOfAnswers:32, Data/binary>> = Reply,
    ?LOG(4,"packet_2_message: Number of Children: ~w",[NumberOfAnswers]),
    ?LOG(4,"packet_2_message: The Binary is: ~w",[Data]),
    {List, Rest} =  get_n_paths(NumberOfAnswers, Data),
    ?LOG(4,"packet_2_message: Paths extracted."),
    ?LOG(4,"packet_2_message: Paths are: ~w",[List]),
    {List, Rest};
%% sync
do_interpret_reply_data(9, Reply) ->
    <<LengthOfData:32, ReplyPath:LengthOfData/binary, Rest/binary>> = Reply,
    {binary_to_list(ReplyPath), Rest};
%%% ls2 --> Reply = a list of the nodes children and the nodes parameters
do_interpret_reply_data(12, Reply) ->
    {<<NumberOfAnswers:32>>, Data} = split_binary(Reply, 4),
    {Children, Left} =  get_n_paths(NumberOfAnswers, Data),
    {Parameter, Rest} = getbinary_2_stat(Left),
    {{Children, Parameter}, Rest};
%%% check
do_interpret_reply_data(13, Reply) ->
    {ok, Reply};
%%% transaction
do_interpret_reply_data(14, Reply) ->
    interpret_transaction_reply(Reply, []).

%%----------------------------------------------------------------
%% Little Helpers (internally neede functions)
%%----------------------------------------------------------------

%% unpacks N paths from a Binary.
%% Returns {ListOfPaths , Leftover}
get_n_paths(0, Binary) ->
    {[],Binary};
get_n_paths(N, Binary) ->
    {ThisPathBin, ToProcessBin} = unpack(Binary),
    {RekResult, Left2} = get_n_paths(N-1, ToProcessBin),
    {[binary_to_list(ThisPathBin) | RekResult ], Left2}.

%% interprets the parameters of a node and returns a List of them.
getbinary_2_stat(Binary) ->
    ?LOG(3,"p2m: Trying to match Parameterdata"),
    <<Czxid:64,                           Mzxid:64,
      Ctime:64,                           Mtime:64,
      DaVer:32,          CVer:32,         AclVer:32,    EpheOwner:64,
      DaLe:32,         NumChi:32,    Pzxid:64,
      Rest/binary>> = Binary,
    ?LOG(3,"p2m: Matching Parameterdata Successfull"),
    {#ezk_stat{czxid          = Czxid,   mzxid     = Mzxid,
               ctime          = Ctime,   mtime     = Mtime,
               dataversion    = DaVer,   datalength= DaLe,
               number_children= NumChi,  pzxid     = Pzxid,
               cversion       = CVer,    aclversion= AclVer,
               ephe_owner     = EpheOwner},
     Rest}.

%% uses the first 4 Byte of a binary to determine the lengths of the data and then
%% returns a pair {Data, Leftover}
unpack(Binary) ->
    <<Length:32, Load/binary>> = Binary,
    split_binary(Load, Length).

%% Looks for the first N Acls in a Binary.
%% Returns {ListOfAclTripels, Leftover}
get_n_acls(0, Acls, Binary) ->
    ?LOG(3,"P2M: Last Acl parsed."),
    {Acls, Binary};
get_n_acls(N, Acls,  Binary) ->
    ?LOG(3,"P2M: Parse next acl from this: ~w.",[Binary]),
    <<0:27, A:1, D:1, C:1, W:1, R:1, Left/binary>>  = Binary,
    {Scheme, Left2}         = unpack(Left),
    ?LOG(3,"P2M: Scheme is: ~w.",[Scheme]),
    {Id,     NowLeft}       = unpack(Left2),
    ?LOG(3,"P2M: Id is: ~w.",[Id]),
    ?LOG(3,"P2M: The Permissiontupel is: ~w.",[{R,W,C,D,A}]),
    Permi = get_perm_from_tupel({R,W,C,D,A}),
    NewAcls = [{Permi, Scheme, Id} | Acls ],
    get_n_acls(N-1, NewAcls, NowLeft).

%% Interprets the Permissions of an Acl.
get_perm_from_tupel({1,W,C,D,A}) ->
    [r | get_perm_from_tupel({0,W,C,D,A})];
get_perm_from_tupel({0,1,C,D,A}) ->
    [w | get_perm_from_tupel({0,0,C,D,A})];
get_perm_from_tupel({0,0,1,D,A}) ->
    [c | get_perm_from_tupel({0,0,0,D,A})];
get_perm_from_tupel({0,0,0,1,A}) ->
    [d | get_perm_from_tupel({0,0,0,0,A})];
get_perm_from_tupel({0,0,0,0,1}) ->
    [a | get_perm_from_tupel({0,0,0,0,0})];
get_perm_from_tupel({0,0,0,0,0}) ->
    [].

interpret_transaction_reply(<<Type:32/signed, Done:8, _:32, Rest/binary>>, R) ->
    case Done =:= 0 of
        true ->
            {Response, Rest1} =
                case Type =:= -1 of
                    true ->
                        <<Error:32/signed, Rest0/binary>> = Rest,
                        {{error, map_error(Error)}, Rest0};
                    false ->
                        interpret_reply_data(Type, Rest)
                end,

            interpret_transaction_reply(Rest1, [Response | R]);
        false ->
            {lists:reverse(R), Rest}
    end.
