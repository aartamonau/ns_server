%% @author Couchbase <info@couchbase.com>
%% @copyright 2011 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc This server syncs part of (replicated) ns_config to local
%% couch config.

-module(cb_config_couch_sync).

-behaviour(gen_server).

-export([start_link/0, set_db_and_ix_paths/2, get_db_and_ix_paths/0]).

%% gen_event callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-include("ns_common.hrl").

-record(state, {}).

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    consider_changing_db_path(),

    Self = self(),
    NodeUUID = node_uuid:get(),
    config_pubsub:subscribe_link(
      [announce_initial,
       {path_pred, fun (Path) ->
                           is_notable_key(NodeUUID, Path)
                   end}],
      fun ({Path, {Value, _}}) ->
              Key = case config:path_components(Path) of
                        ["couchdb", K] ->
                            K;
                        ["node", _NodeUUID, "couchdb", K] ->
                            K
                    end,
              Self ! {notable_change, Key, Value}
      end),

    {ok, #state{}}.

-spec get_db_and_ix_paths() -> [{db_path | index_path, string()}].
get_db_and_ix_paths() ->
    DbPath = couch_config:get("couchdb", "database_dir"),
    IxPath = couch_config:get("couchdb", "view_index_dir", DbPath),
    [{db_path, filename:join([DbPath])},
     {index_path, filename:join([IxPath])}].

-spec set_db_and_ix_paths(DbPath :: string(), IxPath :: string()) -> ok.
set_db_and_ix_paths(DbPath0, IxPath0) ->
    DbPath = filename:join([DbPath0]),
    IxPath = filename:join([IxPath0]),

    couch_config:set("couchdb", "database_dir", DbPath),
    couch_config:set("couchdb", "view_index_dir", IxPath).

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _) ->
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({notable_change, Key, Value0}, State) ->
    Value = couch_util:to_list(flush_for_key(Key, Value0)),
    CurrentValue = couch_config:get("couchdb", Key),

    case CurrentValue =:= Value of
        true ->
            ok;
        false ->
            ok = couch_config:set("couchdb", Key, Value)
    end,

    {noreply, State};
handle_info(_Event, State) ->
    {noreply, State}.

%% Auxiliary functions.

is_notable_key(NodeUUID, Path) ->
    case config:path_components(Path) of
        ["couchdb", _Key] ->
            true;
        ["node", NodeUUID, "couchdb", _Key] ->
            true;
        _ ->
            false
    end.

flush_for_key(Key, Value) ->
    receive
        {notable_change, Key, V} ->
            flush_for_key(Key, V)
    after
        0 ->
            Value
    end.

consider_changing_db_path() ->
    {value, MCDPList} = ns_config:search({node, node(), memcached}),
    case proplists:get_value(dbdir, MCDPList) of
        undefined ->
            ok;
        SomeValue ->
            consider_changing_db_path_with_dbdir(filename:absname(SomeValue), MCDPList)
    end.

consider_changing_db_path_with_dbdir(NSConfigDBDir, MCDPList) ->
    [{db_path, DbPath},
     {index_path, IxPath}] = lists:sort(get_db_and_ix_paths()),

    case NSConfigDBDir =:= DbPath andalso NSConfigDBDir =:= IxPath of
        true ->
            ok;
        _ ->
            set_db_and_ix_paths(NSConfigDBDir, NSConfigDBDir),
            cb_couch_sup:restart_couch()
    end,
    NewMCDPList = lists:keydelete(dbdir, 1, MCDPList),
    ale:info(?USER_LOGGER, "Removed db dir from per node memcached config after setting it as couch's db and index directory. Path is: ~s", [NSConfigDBDir]),
    ns_config:set({node, node(), memcached}, NewMCDPList).
