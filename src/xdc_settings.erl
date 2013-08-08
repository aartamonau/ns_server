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

    ns_config:search(Config, key_to_config_key(Key), Default).

update_global_settings(KVList0) ->
    KVList = [{key_to_config_key(K), V} || {K, V} <- KVList0],
    ns_config:set(KVList).

get_all_global_settings() ->
    get_all_global_settings('latest-config-marker').

get_all_global_settings(Config) ->
    Settings = [{Key, Default} || {Key, _, _, Default} <- settings_specs()],
    ConfigKeyToKey = dict:from_list([{key_to_config_key(K), K} ||
                                        {K, _} <- Settings]),

    {Settings1, _} =
        ns_config:fold(
          fun (ConfigKey, Value, {AccSettings, Saw} = Acc) ->
                  case dict:find(ConfigKey, ConfigKeyToKey) of
                      {ok, Key} ->
                          case sets:is_element(Key, Saw) of
                              true ->
                                  Acc;
                              false ->
                                  AccSettings1 = lists:keystore(Key, 1,
                                                                AccSettings, {Key, Value}),
                                  Saw1 = sets:add_element(Key, Saw),
                                  {AccSettings1, Saw1}
                          end;
                      error ->
                          Acc
                  end
          end, {Settings, sets:new()}, Config),
    Settings1.

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

extract_per_replication_settings(#doc{body={Props}}) ->
    InterestingKeys = [couch_util:to_binary(element(1, Spec)) ||
                          Spec <- xdc_settings:per_replication_settings_specs()],

    [{K, V} || {K, V} <- Props, lists:member(K, InterestingKeys)].

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

%% internal
key_to_config_key(Key) ->
    list_to_atom("xdcr_" ++ couch_util:to_list(Key)).
