-module(config_ezk).

-behaviour(config).

-export([init/1, terminate/2]).
-export([handle_get/3, handle_create/4, handle_set/4, handle_set/5,
         handle_delete/3, handle_delete/4, handle_watch/4, handle_unwatch/3]).
-export([handle_msg/2]).

-include_lib("ezk/include/ezk.hrl").
-include("ns_common.hrl").

-define(PREFIX, "/ns_server").

-record(state, { connection :: ezk_conpid(),
                 watches :: ets:tid() }).

-record(pending_watch, { paths_left :: [config:cfg_path()],
                         paths_triggered :: [config:cfg_path()] }).

init(_Args) ->
    application:start(ezk),
    %% TODO: get configuration from Args
    case ezk:start_connection([], [self()]) of
        {ok, Conn} ->
            erlang:monitor(process, Conn),

            State = #state{connection = Conn,
                           watches = ets:new(ok, [set, protected])},

            %% TODO
            case ezk:exists(Conn, ?PREFIX) of
                {error, no_node} ->
                    Transaction = build_init_transaction(),
                    ?log_debug("running initial transaction~n~p", [Transaction]),
                    {ok, RV} = ezk:transaction(Conn, Transaction),
                    ?log_debug("initial transaction return value: ~p", [RV]),
                    true = lists:all(
                             fun (V) ->
                                     case V of
                                         {ok, _} ->
                                             true;
                                         _ ->
                                             false
                                     end
                             end, RV);
                Other ->
                    ?log_debug("not running initial transaction because got ~p", [Other])
            end,

            {ok, State};
            %% ?log_debug("Creating ~p node for all our keys", [?PREFIX]),
            %% case ezk:create(Conn, ?PREFIX, <<>>) of
            %%     {ok, _Path} ->
            %%         {ok, State};
            %%     {error, node_exists} ->
            %%         ?log_debug("~p already exists", [?PREFIX]),
            %%         {ok, State};
            %%     {error, Error} ->
            %%         ?log_error("Couldn't create ~p node: ~p", [?PREFIX, Error]),
            %%         {error, {cant_create_prefix_node, Error}}
            %% end;
        {error, Error} ->
            {error, {cant_create_ezk_connection, Error}}
    end.

terminate(_Reason, #state{connection = Conn}) ->
    case Conn =/= undefined of
        true ->
            ezk:end_connection(Conn, "terminating");
        false ->
            ok
    end.

handle_get(Path, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_sync(Conn, add_prefix(Path), self(), {synced, Path, Tag}),
    {noreply, State}.

handle_create(Path, Value, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_create(Conn, add_prefix(Path), term_to_binary(Value),
                      self(), {reply, create, Tag}),
    {noreply, State}.

handle_set(Path, Value, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_set(Conn, add_prefix(Path), term_to_binary(Value),
                   self(), {reply, set, Tag}),
    {noreply, State}.

handle_set(Path, Value, Version, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_set(Conn, add_prefix(Path), term_to_binary(Value), Version,
                   self(), {reply, set, Tag}),
    {noreply, State}.

handle_delete(Path, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_delete(Conn, add_prefix(Path), self(), {reply, delete, Tag}),
    {noreply, State}.

handle_delete(Path, Version, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_delete(Conn, add_prefix(Path), Version, self(),
                      {reply, delete, Tag}),
    {noreply, State}.

handle_watch(Paths, WatchRef, Tag, #state{connection = Conn,
                                          watches = Watches} = State) ->
    lists:foreach(
      fun (Path) ->
              ok = ezk:n_exists(Conn, add_prefix(Path), self(),
                                {watch, Path, WatchRef},
                                {watch_reply, Path, Tag, WatchRef})
      end, Paths),
    true = ets:insert_new(Watches,
                          {WatchRef, #pending_watch{paths_left = Paths,
                                                    paths_triggered = []}}),
    {noreply, State}.

handle_unwatch(WatchRef, _Tag, #state{watches = Watches} = State) ->
    ets:delete(Watches, WatchRef),
    {reply, ok, State}.

handle_msg({{synced, Path, Tag}, RV}, #state{connection = Conn} = State) ->
    case RV of
        {ok, _Path} ->
            ok = ezk:n_get(Conn, add_prefix(Path), self(), {reply, get, Tag}),
            {noreply, State};
        {error, Error} ->
            {reply, Tag, {error, translate_error(Error)}, State}
    end;
handle_msg({{reply, ReplyType, Tag}, RV}, State) ->
    {reply, Tag, translate_reply(ReplyType, RV), State};
handle_msg({{watch_reply, Path, Tag, WatchRef}, RV} = Msg,
           #state{watches = Watches} = State) ->
    case ets:lookup(Watches, WatchRef) of
        [{WatchRef, #pending_watch{paths_left = Paths,
                                   paths_triggered = Triggered} = Watch}] ->
            case RV of
                {error, Error} when Error =/= no_node ->
                    ets:delete(Watches, WatchRef),
                    {reply, Tag, {error, translate_error(Error)}, State};
                _ ->
                    case lists:delete(Path, Paths) of
                        [] ->
                            ets:insert(Watches, {WatchRef, initialized}),
                            [self() ! {{watch, P, WatchRef}, unused}
                             || P <- Triggered],
                            {reply, Tag, {ok, WatchRef}, State};
                        NewPaths ->
                            ets:insert(Watches,
                                       {WatchRef,
                                        Watch#pending_watch{paths_left = NewPaths}}),
                            {noreply, State}
                    end
            end;
        [] ->
            ?log_debug("Ignoring watch_reply with unknown tag: ~p", [Msg]),
            {noreply, State}
    end;
handle_msg({{watch_rearm_reply, Path, WatchRef}, RV}, State) ->
    case RV of
        {error, Error} when Error =/= no_node ->
            {stop, {couldnt_rearm_watch, Path, translate_error(Error)}, State};
        _ ->
            {notify_watch, WatchRef, Path, State}
    end;
handle_msg({'DOWN', _, process, Conn, Reason},
           #state{connection = Conn} = State) ->
    ?log_error("Lost connection to zookeeper: ~p. Terminating", [Reason]),
    {stop, {ezk_connection_lost, Reason}, State#state{connection = undefined}};
handle_msg({{watch, Path, WatchRef}, _} = Msg,
           #state{connection = Conn,
                  watches = Watches} = State) ->
    case ets:lookup(Watches, WatchRef) of
        [] ->
            ?log_debug("Ignoring notification for non-existent watch ~p", [Msg]),
            {noreply, State};
        [{WatchRef, initialized}] ->
            ok = ezk:n_exists(Conn, add_prefix(Path), self(),
                              {watch, Path, WatchRef},
                              {watch_rearm_reply, Path, WatchRef}),
            {noreply, State};
        [{WatchRef, #pending_watch{paths_triggered = Triggered} = Watch}] ->
            ets:insert(Watches,
                       {WatchRef,
                        Watch#pending_watch{paths_triggered = [Path | Triggered]}}),
            {noreply, State}
    end;
handle_msg(_, _State) ->
    ignore.

%% TODO
translate_error(Error) ->
    Error.

translate_ok_reply(get, {Data, #ezk_stat{dataversion = Version}}) ->
    try
        binary_to_term(Data)
    of
        Term ->
            {ok, {Term, Version}}
    catch
        error:badarg ->
            {error, conversion_error}
    end;
translate_ok_reply(create, _Path) ->
    {ok, 0};
translate_ok_reply(set, #ezk_stat{dataversion = Version}) ->
    {ok, Version}.

translate_reply(ReplyType, RV) ->
    case RV of
        ok ->
            ok;
        {ok, Ok} ->
            translate_ok_reply(ReplyType, Ok);
        {error, Error} ->
            {error, translate_error(Error)}
    end.

add_prefix(Path) ->
    case Path of
        "/" ->
            ?PREFIX;
        _ ->
            ?PREFIX ++ Path
    end.

%% TODO: it shouldn't be here
-define(ISASL_PW, "isasl.pw").
-define(NS_LOG, "ns_log").

ensure_data_dir() ->
    RawDir = path_config:component_path(data),
    filelib:ensure_dir(RawDir),
    file:make_dir(RawDir),
    RawDir.

get_data_dir() ->
    RawDir = path_config:component_path(data),
    case misc:realpath(RawDir, "/") of
        {ok, X} -> X;
        _ -> RawDir
    end.

detect_enterprise_version(NsServerVersion) ->
    case re:run(NsServerVersion, <<"-enterprise$">>) of
        nomatch ->
            false;
        _ ->
            true
    end.

%% dialyzer proves that statically and complains about impossible code
%% path if I use ?assert... Sucker
is_forced_enterprise() ->
    case os:getenv("FORCE_ENTERPRISE") of
        false ->
            false;
        "0" ->
            false;
        _ ->
            true
    end.

init_is_enterprise() ->
    MaybeNsServerVersion =
        [V || {ns_server, _, V} <- application:loaded_applications()],
    case lists:any(fun (V) -> detect_enterprise_version(V) end, MaybeNsServerVersion) of
        true ->
            true;
        _ ->
            is_forced_enterprise()
    end.

default() ->
    ensure_data_dir(),
    DataDir = get_data_dir(),
    InitQuota = case memsup:get_memory_data() of
                    {_, _, _} = MemData ->
                        ns_storage_conf:default_memory_quota(MemData);
                    _ -> undefined
                end,
    CAPIPort = case erlang:get(capi_port_override) of
                   undefined -> list_to_integer(couch_config:get("httpd", "port", "5984"));
                   CAPIVal -> CAPIVal
               end,

    PortMeta = case application:get_env(rest_port) of
                   {ok, _Port} -> local;
                   undefined -> global
               end,

    RawLogDir = path_config:component_path(data, "logs"),
    filelib:ensure_dir(RawLogDir),
    file:make_dir(RawLogDir),

    IsEnterprise = init_is_enterprise(),

    Node = node_uuid:get(),

    [{directory, path_config:component_path(data, "config")},
     {{node, Node, is_enterprise}, IsEnterprise},
     {index_aware_rebalance_disabled, false},
     {max_bucket_count, 10},
     {autocompaction, [{database_fragmentation_threshold, {30, undefined}},
                       {view_fragmentation_threshold, {30, undefined}}]},
     {set_view_update_daemon,
      [{update_interval, 5000},
       {update_min_changes, 5000},
       {replica_update_min_changes, 5000}]},
     {fast_warmup, [{fast_warmup_enabled, true},
                    {min_memory_threshold, 10},
                    {min_items_threshold, 10}]},
     {{node, Node, compaction_daemon}, [{check_interval, 30},
                                          {min_file_size, 131072}]},
     %% TODO
     {nodes_wanted, [node()]},
     {{node, Node, membership}, active},
     %% In general, the value in these key-value pairs are property lists,
     %% like [{prop_atom1, value1}, {prop_atom2, value2}].
     %%
     %% See the proplists erlang module.
     %%
     %% A change to any of these rest properties probably means a restart of
     %% mochiweb is needed.
     %%
     %% Modifiers: menelaus REST API
     %% Listeners: some menelaus module that configures/reconfigures mochiweb
     {rest,
      [{port, 8091}]},

     {{couchdb, max_parallel_indexers}, 4},
     {{couchdb, max_parallel_replica_indexers}, 2},

     {{node, Node, rest},
      [{port, misc:get_env_default(rest_port, 8091)}, % Port number of the REST admin API and UI.
       {port_meta, PortMeta}]},

     {{node, Node, ssl_rest_port},
      case IsEnterprise of
          true -> misc:get_env_default(ssl_rest_port, 18091);
          _ -> undefined
      end},

     {{node, Node, capi_port},
      CAPIPort},

     {{node, Node, ssl_capi_port},
      case IsEnterprise of
          true -> misc:get_env_default(ssl_capi_port, 18092);
          _ -> undefined
      end},

     {{node, Node, ssl_proxy_downstream_port},
      case IsEnterprise of
          true -> misc:get_env_default(ssl_proxy_downstream_port, 11214);
          _ -> undefined
      end},

     {{node, Node, ssl_proxy_upstream_port},
      case IsEnterprise of
          true -> misc:get_env_default(ssl_proxy_upstream_port, 11215);
          _ -> undefined
      end},

     %% pre 3.0 format:
     %% {rest_creds, [{creds, [{"user", [{password, "password"}]},
     %%                        {"admin", [{password, "admin"}]}]}
     %% An empty list means no login/password auth check.

     %% for 3.0 clusters:
     %% {rest_creds, {User, {password, {Salt, Mac}}}}
     %% {rest_creds, null} means no login/password auth check.
     %% read_only_user_creds has the same format
     {rest_creds, [{creds, []}
                  ]},
     {remote_clusters, []},
     {{node, Node, isasl}, [{path, filename:join(DataDir, ?ISASL_PW)}]},

                                                % Memcached config
     {{node, Node, memcached},
      [{port, misc:get_env_default(memcached_port, 11210)},
       {mccouch_port, misc:get_env_default(mccouch_port, 11213)},
       {dedicated_port, misc:get_env_default(memcached_dedicated_port, 11209)},
       {ssl_port, case IsEnterprise of
                      true -> misc:get_env_default(memcached_ssl_port, 11207);
                      _ -> undefined
                  end},
       {admin_user, "_admin"},
       %% Note that this is not actually the password that is being used; as
       %% part of upgrading config from 2.2 to 2.3 version it's replaced by
       %% unique per-node password. I didn't put it here because default()
       %% supposed to be a pure function.
       {admin_pass, ""},
       {bucket_engine, path_config:component_path(lib, "memcached/bucket_engine.so")},
       {engines,
        [{membase,
          [{engine, path_config:component_path(lib, "memcached/ep.so")},
           {static_config_string,
            "vb0=false;waitforwarmup=false;failpartialwarmup=false"}]},
         {memcached,
          [{engine,
            path_config:component_path(lib, "memcached/default_engine.so")},
           {static_config_string, "vb0=true"}]}]},
       {log_path, path_config:component_path(data, "logs")},
       %% Prefix of the log files within the log path that should be rotated.
       {log_prefix, "memcached.log"},
       %% Number of recent log files to retain.
       {log_generations, 20},
       %% how big log file needs to grow before memcached starts using
       %% next file
       {log_cyclesize, 1024*1024*10},
       %% flush interval of memcached's logger in seconds
       {log_sleeptime, 19},
       %% Milliseconds between log rotation runs.
       {log_rotation_period, 39003},
       {verbosity, 0}]},

     {{node, Node, memcached_config},
      {[
        {interfaces,
         {ns_ports_setup, omit_missing_mcd_ports,
          [
           {[{host, <<"*">>},
             {port, port},
            {maxconn, 30000}]},

           {[{host, <<"*">>},
             {port, dedicated_port},
             {maxconn, 5000}]},

           {[{host, <<"*">>},
             {port, ssl_port},
             {maxconn, 30000},
             {ssl, {[{key, list_to_binary(ns_ssl_services_setup:memcached_key_path())},
                     {cert, list_to_binary(ns_ssl_services_setup:memcached_cert_path())}]}}]}
          ]}},

        {extensions,
         [
          {[{module, list_to_binary(
                       path_config:component_path(lib,
                                                  "memcached/stdin_term_handler.so"))},
            {config, <<"">>}]},

          {[{module, list_to_binary(
                       path_config:component_path(lib, "memcached/file_logger.so"))},
            {config, {"cyclesize=~B;sleeptime=~B;filename=~s/~s",
                      [log_cyclesize, log_sleeptime, log_path, log_prefix]}}]}
         ]},

        {engine,
         {[{module, list_to_binary(
                      path_config:component_path(lib, "memcached/bucket_engine.so"))},
           {config, {"admin=~s;default_bucket_name=default;auto_create=false",
                     [admin_user]}}]}},

        {verbosity, verbosity}
       ]}},

     {memory_quota, InitQuota},

     {buckets, [{configs, []}]},

     %% Moxi config. This is
     %% per-node so command
     %% line override
     %% doesn't propagate
     {{node, Node, moxi}, [{port, misc:get_env_default(moxi_port, 11211)},
                             {verbosity, ""}
                            ]},

     %% Note that we currently assume the ports are available
     %% across all servers in the cluster.
     %%
     %% This is a classic "should" key, where ns_port_sup needs
     %% to try to start child processes.  If it fails, it should ns_log errors.
     {{node, Node, port_servers},
      [{moxi, path_config:component_path(bin, "moxi"),
        ["-Z", {"port_listen=~B,default_bucket_name=default,downstream_max=1024,downstream_conn_max=4,"
                "connect_max_errors=5,connect_retry_interval=30000,"
                "connect_timeout=400,"
                "auth_timeout=100,cycle=200,"
                "downstream_conn_queue_timeout=200,"
                "downstream_timeout=5000,wait_queue_timeout=200",
                [port]},
         "-z", {"url=http://127.0.0.1:~B/pools/default/saslBucketsStreaming",
                [{misc, this_node_rest_port, []}]},
         "-p", "0",
         "-Y", "y",
         "-O", "stderr",
         {"~s", [verbosity]}
        ],
        [{env, [{"EVENT_NOSELECT", "1"},
                {"MOXI_SASL_PLAIN_USR", {"~s", [{ns_moxi_sup, rest_user, []}]}},
                {"MOXI_SASL_PLAIN_PWD", {"~s", [{ns_moxi_sup, rest_pass, []}]}}
               ]},
         use_stdio, exit_status,
         port_server_send_eol,
         stderr_to_stdout,
         stream]
       },
       {memcached, path_config:component_path(bin, "memcached"),
        ["-C", ns_ports_setup:memcached_config_path()],
        [{env, [{"EVENT_NOSELECT", "1"},
                %% NOTE: bucket engine keeps this number of top keys
                %% per top-keys-shard. And number of shards is hard-coded to 8
                %%
                %% So with previous setting of 100 we actually got 800
                %% top keys every time. Even if we need just 10.
                %%
                %% See hot_keys_keeper.erl TOP_KEYS_NUMBER constant
                %%
                %% Because of that heavy sharding we cannot ask for
                %% very small number, which would defeat usefulness
                %% LRU-based top-key maintenance in memcached. 5 seems
                %% not too small number which means that we'll deal
                %% with 40 top keys.
                {"MEMCACHED_TOP_KEYS", "5"},
                {"ISASL_PWFILE", {"~s", [{isasl, path}]}}]},
         use_stdio,
         stderr_to_stdout, exit_status,
         port_server_send_eol,
         stream]
       }]
     },

     {{node, Node, ns_log}, [{filename, filename:join(DataDir, ?NS_LOG)}]},

                                                % Modifiers: menelaus
                                                % Listeners: ? possibly ns_log
     {email_alerts,
      [{recipients, ["root@localhost"]},
       {sender, "couchbase@localhost"},
       {enabled, false},
       {email_server, [{user, ""},
                       {pass, ""},
                       {host, "localhost"},
                       {port, 25},
                       {encrypt, false}]},
       {alerts, [auto_failover_node,auto_failover_maximum_reached,
                 auto_failover_other_nodes_down,auto_failover_cluster_too_small,ip,
                 disk,overhead,ep_oom_errors,ep_item_commit_failed]}
      ]},
     {alert_limits, [
       %% Maximum percentage of overhead compared to max bucket size (%)
       {max_overhead_perc, 50},
       %% Maximum disk usage before warning (%)
       {max_disk_used, 90}
      ]},
     {replication, [{enabled, true}]},
     {auto_failover_cfg, [{enabled, false},
                          % timeout is the time (in seconds) a node needs to be
                          % down before it is automatically faileovered
                          {timeout, 120},
                          % max_nodes is the maximum number of nodes that may be
                          % automatically failovered
                          {max_nodes, 1},
                          % count is the number of nodes that were auto-failovered
                          {count, 0}]},

     %% everything is unlimited by default
     {{request_limit, rest}, undefined},
     {{request_limit, capi}, undefined},
     {drop_request_memory_threshold_mib, undefined},
     {replication_topology, star}].

build_init_transaction() ->
    KVs = [{key_to_path(K), V} || {K, V} <- default()],
    Structure = lists:usort(lists:append([ancestors(K) || {K, _} <- KVs])),

    [ezk:create_op(add_prefix(P), <<>>) || P <- Structure] ++
        [ezk:create_op(add_prefix(K), term_to_binary(V)) || {K, V} <- KVs].

key_to_path(Key) ->
    case is_tuple(Key) of
        true ->
            Components = [couch_util:to_list(X) || X <- tuple_to_list(Key)],
            [$/ | string:join(Components, "/")];
        false ->
            [$/ | couch_util:to_list(Key)]
    end.

ancestors(Path) ->
    case Path of
        "/" ->
            [];
        _ ->
            Parent = filename:dirname(Path),
            [Parent | ancestors(Parent)]
    end.
