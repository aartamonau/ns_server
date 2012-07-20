%% @author Couchbase <info@couchbase.com>
%% @copyright 2012 Couchbase, Inc.
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

-module(remote_clusters_info).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-compile(export_all).

-include("ns_common.hrl").
%% -include("couch_db.hrl").
-include("/home/shaleny/dev/membase/repo20/couchdb/src/couchdb/couch_db.hrl").
-include("remote_clusters_info.hrl").


-define(CACHE, ?MODULE).

-define(REMOTE_INFO_REQ_TIMEOUT,
        ns_config_ets_dup:get_timeout(remote_info_req_timeout, 5000)).
%% -define(GC_INTERVAL,
%%         ns_config_ets_dup:get_timeout(remote_clusters_info_gc_interval, 60000)).
-define(GC_INTERVAL, 10000).

-record(state, {cache_path :: string()}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

fetch_remote_cluster(Cluster) ->
    gen_server:call(?MODULE, {fetch_remote_cluster, Cluster}, 60000).

get_remote_bucket(Reference, Through) ->
    {ok, {ClusterName, BucketName}} = parse_remote_bucket_reference(Reference),
    get_remote_bucket(ClusterName, BucketName, Through).

get_remote_bucket(ClusterName, Bucket, Through) ->
    Cluster = find_cluster_by_name(ClusterName),
    gen_server:call(?MODULE,
                    {get_remote_bucket, Cluster, Bucket, Through}, 60000).

%% gen_server callbacks
init([]) ->
    CachePath = path_config:component_path(data, "remote_clusters_cache"),
    ok = read_or_create_table(?CACHE, CachePath),
    ets:insert_new(?CACHE, {clusters, []}),

    schedule_gc(),

    {ok, #state{cache_path=CachePath}}.

handle_call({fetch_remote_cluster, Cluster}, From, State) ->
    proc_lib:spawn_link(
      fun () ->
              R = remote_cluster(Cluster),
              case R of
                  {ok, #remote_cluster{uuid=UUID} = RemoteCluster} ->
                      ?MODULE ! {cache_remote_cluster, UUID, RemoteCluster};
                  _ ->
                      ok
              end,

              gen_server:reply(From, R)
      end),
    {noreply, State};
handle_call({get_remote_bucket, Cluster, Bucket, false}, From, State) ->
    UUID = proplists:get_value(uuid, Cluster),
    true = (UUID =/= undefined),

    case ets:lookup(?CACHE, {bucket, UUID, Bucket}) of
        [] ->
            handle_call({get_remote_bucket, Cluster, Bucket, true}, From, State);
        [{_, Cached}] ->
            {reply, {ok, Cached}, State}
    end;
handle_call({get_remote_bucket, Cluster, Bucket, true}, From, State) ->
    Username = proplists:get_value(username, Cluster),
    Password = proplists:get_value(password, Cluster),
    UUID = proplists:get_value(uuid, Cluster),

    true = (Username =/= undefined),
    true = (Password =/= undefined),
    true = (UUID =/= undefined),

    RemoteCluster =
        case ets:lookup(?CACHE, UUID) of
            [] ->
                Hostname = proplists:get_value(hostname, Cluster),
                true = (Hostname =/= undefined),

                Nodes = [hostname_to_remote_node(Hostname)],
                #remote_cluster{uuid=UUID, nodes=Nodes};
            [{UUID, FoundCluster}] ->
                FoundCluster
        end,

    proc_lib:spawn_link(
      fun () ->
              R = remote_cluster_and_bucket(RemoteCluster, Bucket, Username, Password),

              Reply =
                  case R of
                      {ok, {NewRemoteCluster, RemoteBucket}} ->
                          ?MODULE ! {cache_remote_cluster, UUID, NewRemoteCluster},
                          ?MODULE ! {cache_remote_bucket, {UUID, Bucket}, RemoteBucket},

                          {ok, RemoteBucket};
                      Error ->
                          Error
                  end,
              gen_server:reply(From, Reply)
      end),
    {noreply, State};
handle_call(Request, From, State) ->
    ?log_warning("Got unexpected call request: ~p", [{Request, From}]),
    {reply, unhandled, State}.

handle_cast(Msg, State) ->
    ?log_warning("Got unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({cache_remote_cluster, UUID, RemoteCluster0}, State) ->
    RemoteCluster = last_cache_request(cache_remote_cluster, UUID, RemoteCluster0),
    true = ets:insert(?CACHE, {UUID, RemoteCluster}),

    [{clusters, Clusters}] = ets:lookup(?CACHE, clusters),
    NewClusters = ordsets:add_element(UUID, Clusters),
    true = ets:insert(?CACHE, {clusters, NewClusters}),

    ets:insert_new(?CACHE, {{buckets, UUID}, []}),
    {noreply, State};
handle_info({cache_remote_bucket, {UUID, Bucket} = Id, RemoteBucket0}, State) ->
    [{_, Buckets}] = ets:lookup(?CACHE, {buckets, UUID}),
    NewBuckets = ordsets:add_element(Bucket, Buckets),
    true = ets:insert(?CACHE, {{buckets, UUID}, NewBuckets}),

    RemoteBucket = last_cache_request(cache_remote_bucket, Id, RemoteBucket0),
    true = ets:insert(?CACHE, {{bucket, UUID, Bucket}, RemoteBucket}),

    {noreply, State};
handle_info(gc, #state{cache_path=CachePath} = State) ->
    gc(),
    dump_table(?CACHE, CachePath),
    schedule_gc(),
    {noreply, State};
handle_info(Info, State) ->
    ?log_warning("Got unexpected info: ~p", [Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions
read_or_create_table(TableName, Path) ->
    Read =
        case filelib:is_regular(Path) of
            true ->
                ?log_info("Reading ~p content from ~s", [TableName, Path]),
                case ets:file2tab(Path, [{verify, true}]) of
                    {ok, TableName} ->
                        true;
                    {error, Error} ->
                        ?log_warning("Failed to read ~p content from ~s: ~p",
                                     [TableName, Path, Error]),
                        false
                end;
        false ->
                false
        end,

    case Read of
        true ->
            ok;
        false ->
            TableName = ets:new(TableName, [named_table, set, protected]),
            ok
    end.

dump_table(TableName, Path) ->
    case ets:tab2file(TableName, Path, [{extended_info, [md5sum]}]) of
        ok ->
            ok;
        {error, Error} ->
            ?log_error("Failed to dump table `~s` to file `~s`: ~p",
                       [TableName, Path, Error]),
            ok
    end.

expect(Value, Context, Extract, K) ->
    case Extract(Value, Context) of
        {ok, ExtractedValue} ->
            K(ExtractedValue);
        {bad_value, Msg} ->
            ?log_warning("~s:~n~p", [Msg, Value]),
            {error, bad_value, Msg}
    end.

extract_object(Value, Context) ->
    case Value of
        {struct, ActualValue} ->
            {ok, ActualValue};
        _ ->
            Msg = io_lib:format("(~s) invalid object", [Context]),
            {bad_value, iolist_to_binary(Msg)}
    end.

expect_object(Value, Context, K) ->
    expect(Value, Context, fun extract_object/2, K).

expect_nested(Field, Props, Context, Extract, K) ->
    case proplists:get_value(Field, Props) of
        undefined ->
            Msg = io_lib:format("(~s) missing field `~s`", [Context, Field]),
            ?log_warning("~s:~n~p", [Msg, Props]),
            {error, {missing_field, Field}, iolist_to_binary(Msg)};
        Value ->
            ExtContext = io_lib:format("field `~s` in ~s", [Field, Context]),
            case Extract(Value, ExtContext) of
                {ok, ActualValue} ->
                    K(ActualValue);
                {bad_value, Msg} ->
                    ?log_warning("~s:~n~p", [Msg, Value]),
                    {error, {bad_value, Field}, Msg}
            end
    end.

expect_nested_object(Field, Props, Context, K) ->
    expect_nested(Field, Props, Context, fun extract_object/2, K).

extract_string(MaybeBinary, Context) ->
    case is_binary(MaybeBinary) of
        true ->
            {ok, MaybeBinary};
        false ->
            Msg = io_lib:format("(~s) got invalid string", [Context]),
            {bad_value, iolist_to_binary(Msg)}
    end.

expect_string(Value, Context, K) ->
    expect(Value, Context, fun extract_string/2, K).

expect_nested_string(Field, Props, Context, K) ->
    expect_nested(Field, Props, Context, fun extract_string/2, K).

extract_array(MaybeArray, Context) ->
    case is_list(MaybeArray) of
        true ->
            {ok, MaybeArray};
        false ->
            Msg = io_lib:format("(~s) got invalid array", [Context]),
            {bad_value, iolist_to_binary(Msg)}
    end.

expect_array(Value, Context, K) ->
    expect(Value, Context, fun extract_array/2, K).

expect_nested_array(Field, Props, Context, K) ->
    expect_nested(Field, Props, Context, fun extract_array/2, K).

extract_number(MaybeNumber, Context) ->
    case is_integer(MaybeNumber) of
        true ->
            {ok, MaybeNumber};
        false ->
            Msg = io_lib:format("(~s) got invalid number", [Context]),
            {bad_value, iolist_to_binary(Msg)}
    end.

expect_number(Value, Context, K) ->
    expect(Value, Context, fun extract_number/2, K).

expect_nested_number(Field, Props, Context, K) ->
    expect_nested(Field, Props, Context, fun extract_number/2, K).

with_pools(JsonGet, K) ->
    Context = <<"/pools response">>,

    JsonGet(
      "/pools",
      fun (PoolsRaw) ->
              expect_object(
                PoolsRaw, Context,
                fun (Pools) ->
                        expect_nested_string(
                          <<"uuid">>, Pools, Context,
                          fun (UUID) ->
                                  K(Pools, UUID)
                          end)
                end)
      end).

with_default_pool_details(Pools, JsonGet, K) ->
    expect_nested_array(
      <<"pools">>, Pools, <<"/pools response">>,
      fun ([DefaultPool | _]) ->
              expect_object(
                DefaultPool,
                <<"pools list in /pools response">>,
                fun (DefaultPoolObject) ->
                        expect_nested_string(
                          <<"uri">>, DefaultPoolObject,
                          <<"default pool object">>,
                          fun (URI) ->
                                  JsonGet(
                                    binary_to_list(URI),
                                    fun (PoolDetails) ->
                                            expect_object(
                                              PoolDetails,
                                              <<"default pool details">>, K)
                                    end)
                          end)
                end)
      end).

with_buckets(PoolDetails, JsonGet, K) ->
    expect_nested_object(
      <<"buckets">>, PoolDetails, <<"default pool details">>,
      fun (BucketsObject) ->
              expect_nested_string(
                <<"uri">>, BucketsObject,
                <<"buckets object in default pool details">>,
                fun (URI) ->
                        JsonGet(
                          binary_to_list(URI),
                          fun (Buckets) ->
                                  expect_array(
                                    Buckets,
                                    <<"buckets details">>, K)
                          end)
                end)
      end).

with_bucket([], BucketName, _K) ->
    Msg = io_lib:format("Bucket `~s` not found.", [BucketName]),
    {error, not_present, iolist_to_binary(Msg)};
with_bucket([Bucket | Rest], BucketName, K) ->
    expect_object(
      Bucket,
      <<"bucket details">>,
      fun (BucketObject) ->
              expect_nested_string(
                <<"name">>, BucketObject, <<"bucket details">>,
                fun (Name) ->
                        case Name =:= BucketName of
                            true ->
                                with_bucket_tail(Name, BucketObject, K);
                            false ->
                                with_bucket(Rest, BucketName, K)
                        end
                end)
      end).

with_bucket_tail(Name, BucketObject, K) ->
    %% server prior to 2.0 don't have bucketCapabilities at all
    CapsRaw = proplists:get_value(<<"bucketCapabilities">>, BucketObject, []),

    expect_array(
      CapsRaw, <<"bucket capabilities">>,
      fun (Caps) ->
              case lists:member(<<"couchapi">>, Caps) of
                  true ->
                      expect_nested_string(
                        <<"uuid">>, BucketObject, <<"bucket details">>,
                        fun (BucketUUID) ->
                                K(BucketObject, BucketUUID)
                        end);
                  false ->
                      Msg = io_lib:format("Bucket `~s` does not support "
                                          "`couchapi` capability", [Name]),
                      {error, not_capable, iolist_to_binary(Msg)}
              end
      end).

remote_cluster(Cluster) ->
    Username = proplists:get_value(username, Cluster),
    Password = proplists:get_value(password, Cluster),
    Hostname = proplists:get_value(hostname, Cluster),

    true = (Username =/= undefined),
    true = (Password =/= undefined),
    true = (Hostname =/= undefined),

    {Host, Port} = host_and_port(Hostname),

    JsonGet = mk_json_get(Host, Port, Username, Password),
    do_remote_cluster(JsonGet).

do_remote_cluster(JsonGet) ->
    with_pools(
      JsonGet,
      fun (Pools, UUID) ->
              with_default_pool_details(
                Pools, JsonGet,
                fun (PoolDetails) ->
                        with_nodes(
                          PoolDetails, <<"default pool details">>,
                          [{<<"hostname">>, fun extract_string/2}],
                          fun (NodeProps) ->
                                  Nodes = lists:map(fun props_to_remote_node/1,
                                                    NodeProps),

                                  {ok, #remote_cluster{uuid=UUID,
                                                       nodes=Nodes}}
                          end)
                end)
      end).

with_nodes(Object, Context, Props, K) ->
    expect_nested_array(
      <<"nodes">>, Object, Context,
      fun (NodeList) ->
              Nodes =
                  lists:flatmap(
                    fun (Node) ->
                            extract_node_props(Props, Context, Node)
                    end, NodeList),
              case Nodes of
                  [] ->
                      Msg = io_lib:format("(~s) unable to extract any nodes",
                                          [Context]),
                      {error, {bad_value, <<"nodes">>}, iolist_to_binary(Msg)};
                  _ ->
                      K(Nodes)
              end
      end).

extract_node_props(Props, Context, {struct, Node}) ->
    R =
        lists:foldl(
          fun ({Prop, Extract}, Acc) ->
                  case proplists:get_value(Prop, Node) of
                      undefined ->
                          ?log_warning("Missing `~s` field in node info:~n~p",
                                       [Prop, Node]),
                          Acc;
                      Value ->
                          case Extract(Value, Context) of
                              {ok, FinalValue} ->
                                  [{Prop, FinalValue} | Acc];
                              {bad_value, Msg} ->
                                  ?log_warning("~s:~n~p", [Msg, Value]),
                                  Acc
                          end
                  end
          end, [], Props),
    [R];
extract_node_props(_Props, Context, Node) ->
    ?log_warning("(~s) got invalid node info value:~n~p", [Context, Node]),
    [].

props_to_remote_node(Props) ->
    Hostname = proplists:get_value(<<"hostname">>, Props),
    true = (Hostname =/= undefined),

    hostname_to_remote_node(binary_to_list(Hostname)).

hostname_to_remote_node(Hostname) ->
    {Host, Port} = host_and_port(Hostname),
    #remote_node{host=Host, port=Port}.

host_and_port(Hostname) ->
    case re:run(Hostname, <<"^(.*):([0-9]*)$">>,
                [anchored, {capture, all_but_first, list}]) of
        nomatch ->
            {Hostname, 8091};
        {match, [Host, PortStr]} ->
            try
                {Host, list_to_integer(PortStr)}
            catch
                error:badarg ->
                    {Hostname, 8091}
            end
    end.

remote_cluster_and_bucket(#remote_cluster{nodes=Nodes,
                                          uuid=UUID},
                          BucketStr, Username, Password) ->
    Bucket = list_to_binary(BucketStr),

    %% TODO
    Node = hd(Nodes),
    remote_bucket(Node, Bucket, Username, Password, UUID).

remote_bucket(#remote_node{host=Host, port=Port},
              Bucket, Username, Password, UUID) ->
    Creds = {Username, Password},

    JsonGet = mk_json_get(Host, Port, Username, Password),

    with_pools(
      JsonGet,
      fun (Pools, ActualUUID) ->
              case ActualUUID =:= UUID of
                  true ->
                      with_default_pool_details(
                        Pools, JsonGet,
                        fun (PoolDetails) ->
                                remote_bucket_with_pool_details(PoolDetails,
                                                                UUID,
                                                                Bucket,
                                                                Creds, JsonGet)

                        end);
                  false ->
                      ?log_info("Attempted to get vbucket map for bucket `~s` "
                                "from remote node ~s:~b. But cluster's "
                                "uuid (~s) does not match expected one (~s)",
                                [Bucket, Host, Port, ActualUUID, UUID]),
                      {error, other_cluster,
                       <<"Remote cluster uuid doesn't match expected one.">>}
              end
      end).

remote_bucket_with_pool_details(PoolDetails, UUID, Bucket, Creds, JsonGet) ->
    with_nodes(
      PoolDetails, <<"default pool details">>,
      [{<<"hostname">>, fun extract_string/2}],
      fun (PoolNodeProps) ->
              with_buckets(
                PoolDetails, JsonGet,
                fun (Buckets) ->
                        with_bucket(
                          Buckets, Bucket,
                          fun (BucketObject, BucketUUID) ->
                                  with_nodes(
                                    BucketObject, <<"bucket details">>,
                                    [{<<"hostname">>, fun extract_string/2},
                                     {<<"couchApiBase">>, fun extract_string/2},
                                     {<<"ports">>, fun extract_object/2}],
                                    fun (BucketNodeProps) ->
                                            remote_bucket_with_bucket(BucketObject,
                                                                      UUID,
                                                                      BucketUUID,
                                                                      PoolNodeProps,
                                                                      BucketNodeProps,
                                                                      Creds)
                                    end)
                          end)
                end)
      end).

remote_bucket_with_bucket(BucketObject, UUID,
                          BucketUUID, PoolNodeProps, BucketNodeProps, Creds) ->
    PoolNodes = lists:map(fun props_to_remote_node/1, PoolNodeProps),
    BucketNodes = lists:map(fun props_to_remote_node/1, BucketNodeProps),

    RemoteNodes = lists:usort(PoolNodes ++ BucketNodes),
    RemoteCluster = #remote_cluster{uuid=UUID, nodes=RemoteNodes},

    with_mcd_to_couch_uri_dict(
      BucketNodeProps, Creds,
      fun (McdToCouchDict) ->
              expect_nested_object(
                <<"vBucketServerMap">>, BucketObject, <<"bucket details">>,
                fun (VBucketServerMap) ->
                        remote_bucket_with_server_map(VBucketServerMap, BucketUUID,
                                                      RemoteCluster, McdToCouchDict)
                end)
      end).

remote_bucket_with_server_map(ServerMap, BucketUUID, RemoteCluster, McdToCouchDict) ->
    with_server_list(
      ServerMap,
      fun (ServerList) ->
              case build_ix_to_couch_uri_dict(ServerList,
                                              McdToCouchDict) of
                  {ok, IxToCouchDict} ->
                      expect_nested_array(
                        <<"vBucketMap">>, ServerMap, <<"vbucket server map">>,
                        fun (VBucketMap) ->
                                VBucketMapDict =
                                    build_vbmap(VBucketMap,
                                                BucketUUID, IxToCouchDict),
                                RemoteBucket = #remote_bucket{uuid=BucketUUID,
                                                              vbucket_map=VBucketMapDict},

                                {ok, {RemoteCluster, RemoteBucket}}
                        end);
                  Error ->
                      Error
              end
      end).

build_vbmap(RawVBucketMap, BucketUUID, IxToCouchDict) ->
    do_build_vbmap(RawVBucketMap, BucketUUID, IxToCouchDict, 0, dict:new()).

do_build_vbmap([], _, _, _, D) ->
    D;
do_build_vbmap([ChainRaw | Rest], BucketUUID, IxToCouchDict, VBucket, D) ->
    expect_array(
      ChainRaw, <<"vbucket map chain">>,
      fun (Chain) ->
              case build_vbmap_chain(Chain, BucketUUID, IxToCouchDict, VBucket) of
                  {ok, FinalChain} ->
                      do_build_vbmap(Rest, BucketUUID, IxToCouchDict,
                                     VBucket + 1,
                                     dict:store(VBucket, FinalChain, D));
                  Error ->
                      Error
              end
      end).

build_vbmap_chain(Chain, BucketUUID, IxToCouchDict, VBucket) ->
    do_build_vbmap_chain(Chain, BucketUUID, IxToCouchDict, VBucket, []).

do_build_vbmap_chain([], _, _, _, R) ->
    {ok, lists:reverse(R)};
do_build_vbmap_chain([NodeIxRaw | Rest], BucketUUID, IxToCouchDict, VBucket, R) ->
    expect_number(
      NodeIxRaw, <<"Vbucket map chain">>,
      fun (NodeIx) ->
              case NodeIx of
                  -1 ->
                      do_build_vbmap_chain(Rest, BucketUUID, IxToCouchDict,
                                           VBucket, [undefined | R]);
                  _ ->
                      case dict:find(NodeIx, IxToCouchDict) of
                          error ->
                              Msg = io_lib:format("Invalid node reference in "
                                                  "vbucket map chain: ~p", [NodeIx]),
                              ?log_error("~s", [Msg]),
                              {error, bad_value, iolist_to_binary(Msg)};
                          {ok, URL} ->
                              VBucketURL0 = [URL, "%2f", integer_to_list(VBucket),
                                             "%3b", BucketUUID],
                              VBucketURL = iolist_to_binary(VBucketURL0),
                              do_build_vbmap_chain(Rest, BucketUUID, IxToCouchDict,
                                                   VBucket, [VBucketURL | R])
                      end
              end
      end).

build_ix_to_couch_uri_dict(ServerList, McdToCouchDict) ->
    do_build_ix_to_couch_uri_dict(ServerList, McdToCouchDict, 0, dict:new()).

do_build_ix_to_couch_uri_dict([], _McdToCouchDict, _Ix, D) ->
    {ok, D};
do_build_ix_to_couch_uri_dict([S | Rest], McdToCouchDict, Ix, D) ->
    case dict:find(S, McdToCouchDict) of
        error ->
            ?log_error("Was not able to find node corresponding to server ~s",
                       [S]),

            Msg = io_lib:format("(bucket server list) got bad server value: ~s",
                                [S]),
            {error, bad_value, iolist_to_binary(Msg)};
        {ok, Value} ->
            D1 = dict:store(Ix, Value, D),
            do_build_ix_to_couch_uri_dict(Rest, McdToCouchDict, Ix + 1, D1)
    end.

with_mcd_to_couch_uri_dict(NodeProps, Creds, K) ->
    do_with_mcd_to_couch_uri_dict(NodeProps, dict:new(), Creds, K).

do_with_mcd_to_couch_uri_dict([], Dict, _Creds, K) ->
    K(Dict);
do_with_mcd_to_couch_uri_dict([Props | Rest], Dict, Creds, K) ->
    Hostname = proplists:get_value(<<"hostname">>, Props),
    CouchApiBase0 = proplists:get_value(<<"couchApiBase">>, Props),
    Ports = proplists:get_value(<<"ports">>, Props),

    %% this is ensured by `extract_node_props' function
    true = (Hostname =/= undefined),
    true = (CouchApiBase0 =/= undefined),
    true = (Ports =/= undefined),

    {Host, _Port} = host_and_port(Hostname),
    CouchApiBase = add_credentials(CouchApiBase0, Creds),

    expect_nested_number(
      <<"direct">>, Ports, <<"node ports object">>,
      fun (DirectPort) ->
              McdUri = iolist_to_binary([Host, $:, integer_to_list(DirectPort)]),
              NewDict = dict:store(McdUri, CouchApiBase, Dict),

              do_with_mcd_to_couch_uri_dict(Rest, NewDict, Creds, K)
      end).

add_credentials(URLBinary, {Username, Password}) ->
    URL = binary_to_list(URLBinary),
    {Scheme, Netloc, Path, Query, Fragment} = mochiweb_util:urlsplit(URL),
    Netloc1 = mochiweb_util:quote_plus(Username) ++ ":" ++
        mochiweb_util:quote_plus(Password) ++ "@" ++ Netloc,
    URL1 = mochiweb_util:urlunsplit({Scheme, Netloc1, Path, Query, Fragment}),
    list_to_binary(URL1).

with_server_list(VBucketServerMap, K) ->
    expect_nested_array(
      <<"serverList">>, VBucketServerMap, <<"bucket details">>,
      fun (Servers) ->
              case validate_server_list(Servers) of
                  ok ->
                      K(Servers);
                  Error ->
                      Error
              end
      end).

validate_server_list([]) ->
    ok;
validate_server_list([Server | Rest]) ->
    expect_string(
      Server, <<"bucket server list">>,
      fun (_Value) ->
              validate_server_list(Rest)
      end).

mk_json_get(Host, Port, Username, Password) ->
    fun (Path, K) ->
            R = menelaus_rest:json_request_hilevel(get,
                                                   {Host, Port, Path},
                                                   {Username, Password}),

            case R of
                {ok, Value} ->
                    K(Value);
                {client_error, Body} ->
                    ?log_error("Request to http://~s:~s@~s:~b~s returned "
                               "400 status code:~n~p",
                               [mochiweb_util:quote_plus(Username),
                                mochiweb_util:quote_plus(Password),
                                Host, Port, Path, Body]),

                    %% convert it to the same form as all other errors
                    {error, client_error,
                     <<"Remote cluster returned 400 status code unexpectedly">>};
                Error ->
                    ?log_error("Request to http://~s:~s@~s:~b~s failed:~n~p",
                               [mochiweb_util:quote_plus(Username),
                                mochiweb_util:quote_plus(Password),
                                Host, Port, Path, Error]),
                    Error
            end
    end.

remote_bucket_reference(ClusterName, BucketName) ->
    iolist_to_binary(
      [<<"/remoteClusters/">>,
       mochiweb_util:quote_plus(ClusterName),
       <<"/buckets/">>,
       mochiweb_util:quote_plus(BucketName)]).

parse_remote_bucket_reference(Reference) ->
    case binary:split(Reference, <<"/">>, [global]) of
        [<<>>, <<"remoteClusters">>, ClusterName, <<"buckets">>, BucketName] ->
            {ok, {mochiweb_util:unquote(ClusterName),
                  mochiweb_util:unquote(BucketName)}};
        _ ->
            {error, bad_reference}
    end.

find_cluster_by_name(ClusterName) ->
    Clusters = get_remote_clusters(),

    case lists:dropwhile(fun (Cluster) ->
                                 Name = proplists:get_value(name, Cluster),
                                 true = (Name =/= undefined),

                                 Name =/= ClusterName
                         end, Clusters) of
        [] ->
            {error, cluster_not_found,
             <<"Requested cluster not found">>};
        [Cluster | _] ->
            Cluster
    end.

last_cache_request(Type, Id, Value) ->
    receive
        {Type, Id, OtherValue} ->
            last_cache_request(Type, Id, OtherValue)
    after
        0 ->
            Value
    end.

get_remote_clusters() ->
    case ns_config:search(remote_clusters) of
        {value, Clusters} ->
            Clusters;
        false ->
            []
    end.

get_remote_clusters_ids() ->
    Clusters = get_remote_clusters(),
    lists:sort(
      lists:map(
        fun (Cluster) ->
                UUID = proplists:get_value(uuid, Cluster),
                true = (UUID =/= undefined),

                UUID
        end, Clusters)).

schedule_gc() ->
    erlang:send_after(?GC_INTERVAL, self(), gc).

get_cached_remote_clusters_ids() ->
    [{clusters, Clusters}] = ets:lookup(?CACHE, clusters),
    Clusters.

get_cached_remote_buckets(ClusterId) ->
    [{_, Buckets}] = ets:lookup(?CACHE, {buckets, ClusterId}),
    [{bucket, ClusterId, Bucket} || Bucket <- Buckets].

gc() ->
    Clusters = get_remote_clusters_ids(),
    CachedClusters = get_cached_remote_clusters_ids(),

    RemovedClusters = ordsets:subtract(CachedClusters, Clusters),
    lists:foreach(fun gc_cluster/1, RemovedClusters),

    gc_buckets().

gc_cluster(Cluster) ->
    CachedBuckets = get_cached_remote_buckets(Cluster),
    lists:foreach(
      fun (Bucket) ->
              true = ets:delete(?CACHE, Bucket)
      end, CachedBuckets).

gc_buckets() ->
    PresentReplications = build_present_replications_set(),
    lists:foreach(
      fun ([UUID, Bucket]) ->
              case sets:is_element({UUID, Bucket}, PresentReplications) of
                  true ->
                      ok;
                  false ->
                      true = ets:delete(?CACHE, {bucket, UUID, Bucket})
              end
      end, ets:match(?CACHE, {{bucket, '$1', '$2'}, '_'})).

build_present_replications_set() ->
    with_replicator_db(
      fun (Db) ->
              {ok, _, Set} =
                  couch_db:enum_docs(
                    Db,
                    fun (DocInfo, _, S) ->
                            case get_rdoc_info(DocInfo, Db) of
                                next ->
                                    {ok, S};
                                {UUID, BucketName} ->
                                    {ok, sets:add_element({UUID, BucketName}, S)}
                            end
                    end, sets:new(), []),
              Set
      end).

get_rdoc_info(#doc_info{deleted=true}, _Db) ->
    next;
get_rdoc_info(#doc_info{id= <<"_design", _/binary>>}, _Db) ->
    next;
get_rdoc_info(#doc_info{id=Id} = DocInfo, Db) ->
    case binary:match(Id, <<"_info_">>) of
        nomatch ->
            next;
        _ ->
            {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, [ejson_body]),
            case Doc#doc.body of
                {Props} ->
                    Target = proplists:get_value(<<"target">>, Props),
                    UUID = proplists:get_value(<<"targetUUID">>, Props),

                    case Target =:= undefined orelse UUID =:= undefined of
                        true ->
                            next;
                        false ->
                            case parse_remote_bucket_reference(Target) of
                                {ok, {_ClusterName, BucketName}} ->
                                    {UUID, BucketName};
                                {error, bad_reference} ->
                                    next
                            end
                    end;
                _ ->
                    next
            end
    end.

with_replicator_db(Fn) ->
    case couch_db:open_int(<<"_replicator">>, []) of
        {ok, Db} ->
            try
                Fn(Db)
            after
                couch_db:close(Db)
            end;
        Error ->
            ?log_error("Failed to open replicator database: ~p", [Error]),
            exit({open_replicator_db_failed, Error})
    end.
