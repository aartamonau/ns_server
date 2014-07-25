%% -------------------------------------------------------------------
%%
%% ezk_message_2_packet: A Module which contains functions to convert
%%                       Erlang representations into binarys to send them
%%                       to the zkServer.
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
-module(ezk_message_2_packet).
-include("ezk.hrl").
-export([make_packet/2, make_addauth_packet/1, make_quit_message/1]).
-export([get_permi_int/2]).

%% This function gets a command and the associated data plus the actual Iteration
%% and constructs a corresponding Binary which is used to signal the command to the server.
%% The command is translated to the command id and the payload for the packet is computed.
%% Then the function wrap_packet wraps this all up neatly.
%% Returns {ok, CommandId, PacketBinary}
make_packet(Command, Iteration) ->
    wrap_packet(make_payload(Command), Iteration).

%% create
make_payload({create, Path, Data, Typ, Acls}) ->
    %% e = epheremal , s = sequenced
    case Typ of
        e -> Mode = 1;
        s -> Mode = 2;
        es -> Mode = 3;
        se -> Mode = 3;
        _Else -> Mode = 0
    end,
    %% gets a binary representation of the acls
    AclBin = acls_2_bin(Acls, <<>>, 0),
    Load = <<(pack_it_l2b(Path))/binary,
             (pack_it_b2b(Data))/binary,
             AclBin/binary,
             Mode:32>>,
    Command = 1,
    {Command, Load};
%%delete
make_payload({delete, Path, Version}) ->
    Load = <<(pack_it_l2b(Path))/binary, Version:32/big>>,
    Command = 2,
    {Command, Load};
%% exists
make_payload({exists, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 0:8 >>,
    Command = 3,
    {Command, Load};
make_payload({existsw, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 1:8 >>,
    Command = 3,
    {Command, Load};

%% get
make_payload({get, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 0:8>>,
    Command = 4,
    {Command, Load};
%% getw (the last Bit in the load is 1 if there should be a watch)
make_payload({getw, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 1:8>>,
    Command = 4,
    {Command, Load};
%% set
make_payload({set, Path, Data, Version}) ->
    Load = <<(pack_it_l2b(Path))/binary,
             (pack_it_b2b(Data))/binary,
             Version:32>>,
    Command = 5,
    {Command, Load};
%% get acl
make_payload({get_acl, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary>>,
    Command = 6,
    {Command, Load};
%% set acl
make_payload({set_acl, Path, Acls, Version}) ->
    ?LOG(3,"m2p: trying to set an acl, starting to build package"),
    AclBin = acls_2_bin(Acls,<<>>,0),
    ?LOG(3,"m2p: trying to set an acl, AclBin constructed"),
    Load = <<(pack_it_l2b(Path))/binary,
             AclBin/binary,
             Version:32>>,
    Command = 7,
    ?LOG(3,"m2p: trying to set an acl, Load constructed"),
    {Command, Load};
%% ls
make_payload({ls, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 0:8>>,
    Command = 8,
    {Command, Load};
%% ls with a watch
make_payload({lsw, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 1:8>>,
    Command = 8,
    {Command, Load};
make_payload({sync, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary>>,
    Command = 9,
    {Command, Load};
%% ls2
make_payload({ls2, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 0:8>>,
    Command = 12,
    {Command, Load};
%% ls2 with watch
make_payload({ls2w, Path}) ->
    Load = <<(pack_it_l2b(Path))/binary, 1:8>>,
    Command = 12,
    {Command, Load};
make_payload({check, Path, Version}) ->
    Load = <<(pack_it_l2b(Path))/binary, Version:32>>,
    Command = 13,
    {Command, Load};
make_payload({transaction, Operations}) ->
    Command = 14,
    Load0 = << <<(begin
                      {Id, L} = make_payload(Op),
                      <<Id:32, 0:8, -1:32, L/binary>>
                  end)/binary>> || Op <- Operations >>,
    Load = << Load0/binary, -1:32, 1:8, -1:32>>,
    {Command, Load}.


%% addauth (special case, because iteration is not used, so the standard
%% way to build a packet has to be altered.
make_addauth_packet({add_auth, Scheme, Auth}) ->
    Packet = <<255, 255, 255, 252, 0, 0, 0, 100, 0, 0, 0, 0,
               (pack_it_l2b(Scheme))/binary,
               (pack_it_l2b(Auth))/binary>>,
    {ok, Packet}.

make_quit_message(Iteration) ->
    _QuitMessage  = <<Iteration:32, 255, 255, 255, 245>>.

%%--------------------------------------------------------------------
%% Little Helpers (internal functions)
%%--------------------------------------------------------------------

%% gets a list, determines the length and then puts both together as a binary.
pack_it_l2b(List) ->
    Length = iolist_size(List),
    <<Length:32,(iolist_to_binary(List))/binary>>.

pack_it_b2b(Bin) ->
    Length = size(Bin),
    <<Length:32, Bin/binary>>.

%% Gets the command id, the path, the load and the actual iteration and forms a
%% zookeeper packet from all of them but the path. Instead the Path is passed on
%% to the ezk_server.
%% Returns {ok, CommandId, Path, PacketBinary}
wrap_packet({Command, Load}, Iteration) ->
    ?LOG(3, "message_2_packet: Try send a request {command, Load}: ~w",
         [{Command, Load}]),
    Packet = <<Iteration:32, Command:32, Load/binary>>,
    ?LOG(3, "message_2_packet: Request send"),
    {ok, Command, Packet}.

%% Gets a List of Acl and returns a binary representation of them.
%% To call this function use acls_2_bin(ListOfAcls, <<>>, 0).
%% The empty case represents the end of the parsing.
acls_2_bin([], AclBin, Int) ->
    ?LOG(3,"m2p: Acl finished"),
    <<Int:32, AclBin/binary>>;
%% If an undef is in the list there is no Acl and world:anyone with all permissions is
%% used.
acls_2_bin([undef], _AclBin, Int) ->
    ?LOG(3,"m2p: undef Acl build"),
    NewAclBin = <<31:32,
                  (pack_it_l2b("world"))/binary,
                  (pack_it_l2b("anyone"))/binary>>,
    acls_2_bin([], NewAclBin, Int+1);
%% The case in which the real work is going on.
%% The Permission is translated to a bitcombination, where the last 5 bits are:
%% admin, delete, create, write, read
acls_2_bin([{Permi, Scheme, Id}| Left], AclBin, Int) ->
    ?LOG(3,"m2p: one more element for acl build"),
    ?LOG(3,"m2p: ACL : ~w",[{Scheme, Id, Permi}]),
    PermiInt = get_permi_int(Permi,0),
    ?LOG(3,"m2p: PermiInt : ~w",[PermiInt]),
    SchemeBin = pack_it_l2b(Scheme),
    ?LOG(3,"m2p: SchemeBin : ~w",[SchemeBin]),
    IdBin     = pack_it_l2b(Id),
    ?LOG(3,"m2p: IdBin : ~w",[IdBin]),
    NewAclBin = <<PermiInt:32,
                  SchemeBin/binary,
                  IdBin/binary,
                  AclBin/binary>>,
    acls_2_bin(Left, NewAclBin, Int+1).

%% translates the permissions into an integer.
%% it works with a list of atoms or a list of representations of the chars.
%% also mixtures are no problem.
get_permi_int([], PermiInt) ->
    ?LOG(3,"All rights processed"),
    PermiInt;
get_permi_int([H | T], PermiInt) ->
    ?LOG(3,"Right ~w in process",[H]),
    case H of
        r ->   CommandBit = 1;
        w ->   CommandBit = 2;
        c ->   CommandBit = 4;
        d ->   CommandBit = 8;
        a ->   CommandBit = 16;
        114 -> CommandBit = 1;
        119 -> CommandBit = 2;
        99  -> CommandBit = 4;
        100 -> CommandBit = 8;
        97  -> CommandBit = 16
    end,
    get_permi_int(T, (PermiInt bor CommandBit)).
