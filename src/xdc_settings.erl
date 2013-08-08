%% @author Couchbase <info@couchbase.com>
%% @copyright 2013 Couchbase, Inc.
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

-module(xdc_settings).

-compile(export_all).

-include("couch_db.hrl").

get_global_setting(Key) ->
    get_global_setting('latest-config-marker', Key).

get_global_setting(Config, Key) ->
    Specs = settings_specs(),
    {Key, _, _, Default} = lists:keyfind(Key, 1, Specs),

    ns_config:search(Config, setting_to_config_key(Key), Default).

get_setting(RepDoc, Key) ->
    true = lists:keymember(Key, 1, per_replication_settings_specs()),

    #doc{body={Props}} = couch_doc:with_ejson_body(RepDoc),
    KeyBinary = couch_util:to_binary(Key),
    case lists:keyfind(KeyBinary, 1, Props) of
        false ->
            get_global_setting(Key);
        {KeyBinary, Value} ->
            Value
    end.

%% internal
settings_specs() ->
    [{max_concurrent_reps,              per_replication, {int, 1, 1024},             32},
     {checkpoint_interval,              per_replication, {int, 60, 14400},           1800},
     {doc_batch_size_kb,                per_replication, {int, 500, 10000},          2048},
     {failure_restart_interval,         per_replication, {int, 1, 300},              30},
     {worker_batch_size,                per_replication, {int, 500, 10000},          500},
     {connection_timeout,               per_replication, {int, 10, 10000},           180},
     {num_worker_process,               per_replication, {int, 1, 32},               4},
     {num_http_connections,             per_replication, {int, 1, 100},              20},
     {num_retries_per_request,          per_replication, {int, 1, 100},              2},
     {optimistic_replication_threshold, per_replication, {int, 0, 20 * 1024 * 1024}, 256},
     {xmem_worker,                      per_replication, {int, 1, 32},               1},
     {enable_pipeline_ops,              per_replication, bool,                       true},
     {local_conflict_resolution,        per_replication, bool,                       false},
     {trace_dump_inverse_prob,          global,          {int, 1, 100000},           1000}].

per_replication_settings_specs() ->
    [{Key, Type} || {Key, per_replication, Type, _Default} <- settings_specs()].

setting_to_config_key(Key) when is_atom(Key) ->
    list_to_atom("xdcr_" ++ atom_to_list(Key)).
