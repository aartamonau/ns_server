%% -------------------------------------------------------------------
%%
%% ezk: The Interface Module. No real functions.
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

-module(ezk).
-include("ezk.hrl").

%% functions creating and deleting zkNodes
-export([  create/3,   create/4,   create/5,   delete/2,   delete/3]).
-export([n_create/5, n_create/6, n_create/7, n_delete/4, n_delete/5]).
%% functions dealing with node informations
-export([  set/3,   set/4,   get/2,   ls/2,   ls2/2]).
-export([n_set/5, n_set/6, n_get/4, n_ls/4, n_ls2/4]).
-export([  set_acl/3,   set_acl/4,   get_acl/2]).
-export([n_set_acl/5, n_set_acl/6, n_get_acl/4]).
-export([transaction/2]).
-export([n_transaction/4]).
-export([create_op/2, create_op/3, create_op/4]).
-export([delete_op/1, delete_op/2]).
-export([set_op/2, set_op/3]).
-export([check_op/2]).
%% functions dealing with watches
-export([ls/4, n_ls/5, get/4, n_get/5, ls2/4, n_ls2/5, exists/4, n_exists/5]).
%% macros
-export([delete_all/2, ensure_path/2]).
%% infos
-export([info_get_iterations/1]).
%% Stop commands (forcing Client to choose a new random Server from List)
-export([die/1, die/2, auth/3]).

-export([start_connection/0, start_connection/1, start_connection/2, end_connection/2]).
-export([add_monitors/2, get_connections/0]).
-export([exists/2, n_exists/4]).
-export([sync/2, n_sync/4]).

-export_type([ezk_create_op/0, ezk_delete_op/0, ezk_set_op/0, ezk_check_op/0]).

-opaque ezk_create_op() :: {create, ezk_path(), ezk_data(), ezk_ctype(), ezk_acls()}.
-opaque ezk_delete_op() :: {delete, ezk_path(), ezk_version()}.
-opaque ezk_set_op() :: {set, ezk_path(), ezk_data(), ezk_version()}.
-opaque ezk_check_op() :: {check, ezk_path(), ezk_version()}.


%%--------------------------- Zookeeper Functions ---------------------
%% Return {ok, Reply}.

%% Reply = authed
%% Returns {error, auth_in_progress}  if the authslot is already in use.
%% Returns {error, auth_failed} if server rejected auth
%% Returns {error, unknown, ErrorCodeBin} if something new happened
-spec auth(ezk_conpid(), ezk_acl_scheme(), ezk_acl_id()) -> ezk_authreply().
auth(ConnectionPId, Scheme, Id) ->
   ezk_connection:addauth(ConnectionPId, Scheme, Id).


%% Creates a new ZK_Node
-spec create(ezk_conpid(), ezk_path(), ezk_data()) ->
                    {ok, ezk_path()} | {error, ezk_err()}.
create(ConnectionPId, Path, Data) ->
     ezk_connection:create(ConnectionPId, Path, Data).
n_create(ConnectionPId, Path, Data, Receiver, Tag) ->
     ezk_connection:n_create(ConnectionPId, Path, Data, Receiver, Tag).

%% Typ = e | s | es (stands for etheremal, sequenzed or both)
-spec create(ezk_conpid(), ezk_path(), ezk_data(), ezk_ctype()) ->
                    {ok, ezk_path()} | {error, ezk_err()}.
create(ConnectionPId, Path, Data, Typ) ->
    ezk_connection:create(ConnectionPId, Path, Data, Typ).
n_create(ConnectionPId, Path, Data, Typ, Receiver, Tag) ->
    ezk_connection:n_create(ConnectionPId, Path, Data, Typ, Receiver, Tag).


%% Acls = [Acl] where Acl = {Permissions, Scheme, Id}
%% with Scheme and Id = String
%% and Permission = [Per] | String
%% where Per = r | w | c | d | a
-spec create(ezk_conpid(), ezk_path(), ezk_data(), ezk_ctype(), ezk_acls()) ->
                    {ok, ezk_path()} | {error, ezk_err()}.
create(ConnectionPId, Path, Data, Typ, Acls)  ->
   ezk_connection:create(ConnectionPId, Path, Data, Typ, Acls).
n_create(ConnectionPId, Path, Data, Typ, Acls, Receiver, Tag)  ->
   ezk_connection:n_create(ConnectionPId, Path, Data, Typ, Acls, Receiver, Tag).

-spec ensure_path(ezk_conpid(), ezk_path()) ->
                         {ok, ezk_path()} | {error, ezk_err()}.
ensure_path(ConnectionPId, Path) ->
    ezk_connection:ensure_path(ConnectionPId, Path).

%% Deletes a ZK_Node
%% Only working if Node has no children.
-spec delete(ezk_conpid(), ezk_path()) -> ok | {error, ezk_err()}.
delete(ConnectionPId, Path) ->
    ezk_connection:delete(ConnectionPId, Path).
n_delete(ConnectionPId, Path, Receiver, Tag) ->
    ezk_connection:n_delete(ConnectionPId, Path, Receiver, Tag).

%% Deletes a ZK_Node if its version matches.
%% Only working if Node has no children.
-spec delete(ezk_conpid(), ezk_path(), ezk_version()) -> ok | {error, ezk_err()}.
delete(ConnectionPId, Path, Version) ->
    ezk_connection:delete(ConnectionPId, Path, Version).
n_delete(ConnectionPId, Path, Version, Receiver, Tag) ->
    ezk_connection:n_delete(ConnectionPId, Path, Version, Receiver, Tag).

%% Deletes a ZK_Node and all his childs.
-spec delete_all(ezk_conpid(), ezk_path()) -> ok | {error, ezk_err()}.
delete_all(ConnectionPId, Path) ->
   ezk_connection:delete_all(ConnectionPId, Path).

%% Looks if a Node exists
%% Reply = Parameters like in get (see one function below)
%% Can set a watch to the path
%% which is triggered
%% a) when path is erased if path existed.
%% b) when path is created if path did not exist.
-spec exists(ezk_conpid(), ezk_path()) -> {ok, #ezk_stat{}} | {error, ezk_err()}.
exists(ConnectionPId, Path) ->
    ezk_connection:exists(ConnectionPId, Path).
n_exists(ConnectionPId, Path, Receiver, Tag) ->
    ezk_connection:n_exists(ConnectionPId, Path, Receiver, Tag).

-spec exists(ezk_conpid(), ezk_path(), ezk_watchowner(), ezk_watchmessage()) ->
                    {ok, #ezk_stat{}} | {error, ezk_err()}.
exists(ConnectionPId, Path, WatchOwner, WatchMessage) ->
    ezk_connection:exists(ConnectionPId, Path, WatchOwner, WatchMessage).
n_exists(ConnectionPId, Path, WatchOwner, WatchMessage, ReplyTag) ->
    ezk_connection:n_exists(ConnectionPId, Path,
                            WatchOwner, WatchMessage, ReplyTag).

%% Reply = {Data, Parameters} where Data = The Data stored in the Node
%% and Parameters = #ezk_stat{}
-spec get(ezk_conpid(), ezk_path()) ->
                 {ok, {ezk_data(), #ezk_stat{}}} | {error, ezk_err()}.
get(ConnectionPId, Path) ->
    ezk_connection:get(ConnectionPId, Path).
n_get(ConnectionPId, Path, Receiver, Tag) ->
    ezk_connection:n_get(ConnectionPId, Path, Receiver, Tag).

%% Like the one above but sets a datawatch to Path.
%% If watch is triggered a Message M is send to the PId WatchOwner
%% M = {WatchMessage, {Path, Type, SyncCon}}
%% with Type = child
-spec get(ezk_conpid(), ezk_path(), ezk_watchowner(), ezk_watchmessage()) ->
                 {ok, {ezk_data(), #ezk_stat{}}} | {error, ezk_err()}.
get(ConnectionPId, Path, WatchOwner, WatchMessage) ->
    ezk_connection:get(ConnectionPId, Path, WatchOwner, WatchMessage).
n_get(ConnectionPId, Path, WatchOwner, WatchMessage, ReplyTag) ->
    ezk_connection:n_get(ConnectionPId, Path,
                         WatchOwner, WatchMessage, ReplyTag).

%% Returns the actual Acls of a Node
%% Reply = {[ACL],Parameters} with ACl and Parameters like above
-spec get_acl(ezk_conpid, ezk_path()) ->
                     {ok, {ezk_acls(), #ezk_stat{}}} | {error, ezk_err()}.
get_acl(ConnectionPId, Path) ->
    ezk_connection:get_acl(ConnectionPId, Path).
n_get_acl(ConnectionPId, Path, Receiver, Tag) ->
    ezk_connection:n_get_acl(ConnectionPId, Path, Receiver, Tag).

%% Sets new Data in a Node. Old ones are lost.
%% Reply = Parameters with Data like at get
-spec set(ezk_conpid(), ezk_path(), ezk_data()) ->
                 {ok, #ezk_stat{}} | {error, ezk_err()}.
set(ConnectionPId, Path, Data) ->
   ezk_connection:set(ConnectionPId, Path, Data).
n_set(ConnectionPId, Path, Data, Receiver, Tag) ->
   ezk_connection:n_set(ConnectionPId, Path, Data, Receiver, Tag).

%% Sets new Data in a Node if its verion matches. Old ones are lost.
%% Reply = Parameters with Data like at get
-spec set(ezk_conpid(), ezk_path(), ezk_data(), ezk_version()) ->
                 {ok, #ezk_stat{}} | {error, ezk_err()}.
set(ConnectionPId, Path, Data, Version) ->
   ezk_connection:set(ConnectionPId, Path, Data, Version).
n_set(ConnectionPId, Path, Data, Version, Receiver, Tag) ->
   ezk_connection:n_set(ConnectionPId, Path, Data, Version, Receiver, Tag).

%% Sets new Acls in a Node. Old ones are lost.
%% ACL like above.
%% Reply = Parameters with Data like at get
-spec set_acl(ezk_conpid(), ezk_path(), ezk_acls()) ->
                     {ok, #ezk_stat{}} | {error, ezk_err()}.
set_acl(ConnectionPId, Path, Acls) ->
    ezk_connection:set_acl(ConnectionPId, Path, Acls).
n_set_acl(ConnectionPId, Path, Acls, Receiver, Tag) ->
    ezk_connection:n_set_acl(ConnectionPId, Path, Acls, Receiver, Tag).

%% Sets new Acls in a Node if version matches. Old ones are lost.
%% ACL like above.
%% Reply = Parameters with Data like at get
-spec set_acl(ezk_conpid(), ezk_path(), ezk_acls(), ezk_version()) ->
                     {ok, #ezk_stat{}} | {error, ezk_err()}.
set_acl(ConnectionPId, Path, Acls, Version) ->
    ezk_connection:set_acl(ConnectionPId, Path, Acls, Version).
n_set_acl(ConnectionPId, Path, Acls, Version, Receiver, Tag) ->
    ezk_connection:n_set_acl(ConnectionPId, Path, Acls, Version, Receiver, Tag).

%% Lists all Children of a Node. Paths are given as Binarys!
%% Reply = [ChildName] where ChildName = <<"Name">>
-spec ls(ezk_conpid(), ezk_path()) -> {ok, [ezk_path()]} | {error, ezk_err()}.
ls(ConnectionPId, Path) ->
   ezk_connection:ls(ConnectionPId, Path).
n_ls(ConnectionPId, Path, Receiver, Tag) ->
   ezk_connection:n_ls(ConnectionPId, Path, Receiver, Tag).

%% like above, but a Childwatch is set to the Node.
%% Same Reaktion like at get with watch but Type = child
-spec ls(ezk_conpid(), ezk_path(), ezk_watchowner(), ezk_watchmessage()) ->
                {ok, [ezk_path()]} | {error, ezk_err()}.
ls(ConnectionPId, Path, WatchOwner, WatchMessage) ->
    ezk_connection:ls(ConnectionPId, Path, WatchOwner, WatchMessage).
n_ls(ConnectionPId, Path, WatchOwner, WatchMessage, ReplyTag) ->
    ezk_connection:n_ls(ConnectionPId, Path, WatchOwner, WatchMessage, ReplyTag).

%% Lists all Children of a Node. Paths are given as Binarys!
%% Reply = {[ChildName],Parameters} with Parameters and ChildName like above.
-spec ls2(ezk_conpid(), ezk_path()) -> {ok, ezk_ls2data()} | {error, ezk_err()}.
ls2(ConnectionPId, Path) ->
   ezk_connection:ls2(ConnectionPId, Path).
n_ls2(ConnectionPId, Path, Receiver, Tag) ->
   ezk_connection:n_ls2(ConnectionPId, Path, Receiver, Tag).

%% like above, but a Childwatch is set to the Node.
%% Same Reaktion like at get with watch but Type = child
-spec ls2(ezk_conpid(), ezk_path(), ezk_watchowner(), ezk_watchmessage()) ->
                 {ok, ezk_ls2data()} | {error, ezk_err()}.
ls2(ConnectionPId, Path, WatchOwner, WatchMessage) ->
    ezk_connection:ls2(ConnectionPId, Path, WatchOwner, WatchMessage).
n_ls2(ConnectionPId, Path, WatchOwner, WatchMessage, ReplyTag) ->
    ezk_connection:n_ls2(ConnectionPId, Path,
                         WatchOwner, WatchMessage, ReplyTag).

%% Run a squence of operations atomically. Results returned in as a list in
%% the same order. Valid operations can be created by create_op/2,
%% create_op/3, create_op/4, delete_op/1, delete_op/2, set_op/2, set_op/3,
%% check_op/2.
-spec transaction(ezk_conpid(), Operations) ->
                         {ok, [SuccessResponse] | [FailureResponse]} |
                         {error, ezk_err()} when
      Operations :: [ezk_create_op() | ezk_check_op() |
                     ezk_set_op() | ezk_delete_op()],
      SuccessResponse :: ok | {ok, SetResponse | CreateResponse},
      FailureResponse :: {error, ezk_err()},
      SetResponse :: #ezk_stat{},
      CreateResponse :: ezk_path().
transaction(ConnectionPId, Operations) ->
    ezk_connection:transaction(ConnectionPId, Operations).
n_transaction(ConnectionPId, Operations, Receiver, Tag) ->
    ezk_connection:n_transaction(ConnectionPId, Operations, Receiver, Tag).

-spec create_op(ezk_path(), ezk_data()) -> ezk_create_op().
create_op(Path, Data) ->
    ezk_connection:create_op(Path, Data).
-spec create_op(ezk_path(), ezk_data(), ezk_ctype()) -> ezk_create_op().
create_op(Path, Data, Typ) ->
    ezk_connection:create_op(Path, Data, Typ).
-spec create_op(ezk_path(), ezk_data(), ezk_ctype(), ezk_acls()) -> ezk_create_op().
create_op(Path, Data, Typ, Acls) ->
    ezk_connection:create_op(Path, Data, Typ, Acls).

-spec delete_op(ezk_path()) -> ezk_delete_op().
delete_op(Path) ->
    ezk_connection:delete_op(Path).
-spec delete_op(ezk_path(), ezk_version()) -> ezk_delete_op().
delete_op(Path, Version) ->
    ezk_connection:delete_op(Path, Version).

-spec set_op(ezk_path(), ezk_data()) -> ezk_set_op().
set_op(Path, Data) ->
    ezk_connection:set_op(Path, Data).
-spec set_op(ezk_path(), ezk_data(), ezk_version()) -> ezk_set_op().
set_op(Path, Data, Version) ->
    ezk_connection:set_op(Path, Data, Version).

-spec check_op(ezk_path(), ezk_version()) -> ezk_check_op().
check_op(Path, Version) ->
    ezk_connection:check_op(Path, Version).

-spec sync(ezk_conpid(), ezk_path()) -> {ok, ezk_path()} | {error, ezk_err()}.
sync(ConnectionPId, Path) ->
    ezk_connection:sync(ConnectionPId, Path).
n_sync(ConnectionPId, Path, Receiver, Tag) ->
    ezk_connection:n_sync(ConnectionPId, Path, Receiver, Tag).

%% Returns the Actual Transaction Id of the Client.
%% Reply = Iteration = Int.
-spec info_get_iterations(ezk_conpid()) -> integer().
info_get_iterations(ConnectionPId) ->
    ezk_connection:info_get_iterations(ConnectionPId).

%% Starts a connection to a zookeeper Server
%% Returns {ok, PID} where Pid is the PId of the gen_server
%% which manages the connection
-spec start_connection() -> {ok, ezk_conpid()} | {error, no_server_reached}.
start_connection() ->
    start_connection([]).

%% Starts a connection to a zookeeper Server
%% Returns {ok, PID} where Pid is the PId of the gen_server
%% which manages the connection
-spec start_connection([ezk_server()]) ->
                              {ok, ezk_conpid()} | {error, no_server_reached}.
start_connection(Servers) ->
    ezk_connection_manager:start_connection(Servers).

-spec start_connection([ezk_server()], [pid()]) ->
                              {ok, ezk_conpid()} | {error, no_server_reached}.
start_connection(Servers, MonitorPids) ->
    ezk_connection_manager:start_connection(Servers, MonitorPids).

%% stops a connection. Returns ok.
-spec end_connection(ezk_conpid(), string()) -> ok | {error, no_connection}.
end_connection(ConnectionPId, Reason) ->
    ezk_connection_manager:end_connection(ConnectionPId, Reason).

%% Adds new monitor PIds to bind to one connection. If one
%% of the Monitors dies the connection is closed down.
-spec add_monitors(ezk_conpid(), [pid()])  -> ok.
add_monitors(ConnectionPId, Monitors) ->
    ezk_connection_manager:add_monitors(ConnectionPId, Monitors).

%% Provides a list of all actually active connections.
%% Returns [Connection] where Connection = {PId, [MonitorPId]}
-spec get_connections() -> [{ezk_conpid(), [ezk_monitor()]}].
get_connections() ->
    ezk_connection_manager:get_connections().

die(ConnectionPId) ->
    ezk:die(ConnectionPId, "No offence").

die(ConnectionPId, Reason) when is_pid(ConnectionPId) ->
    ezk_connection:die(ConnectionPId, Reason).
