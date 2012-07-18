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
-include("remote_clusters_info.hrl").

-define(REMOTE_INFO_REQ_TIMEOUT,
        ns_config_ets_dup:get_timeout(remote_info_req_timeout, 5000)).

-define(REMOTES, remotes_cache).
-define(VBMAPS, vbmaps_cache).

-record(state, {remotes_file_cache :: string(),
                vbmaps_file_cache :: string()}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_remote_cluster(Cluster) ->
    gen_server:call(?MODULE, {get_remote_cluster, Cluster}).

get_remote_cluster_vbucket_map(ClusterName, Bucket) ->
    Cluster = find_cluster_by_name(ClusterName),
    gen_server:call(?MODULE, {get_remote_vbucket_map, Cluster, Bucket}).

%% gen_server callbacks
init([]) ->
    RemotesPath = path_config:component_path(data, "remotes"),
    VBMapsPath = path_config:component_path(data, "remotes_vbmaps"),

    ok = read_or_create_table(?REMOTES, RemotesPath),
    ok = read_or_create_table(?VBMAPS, VBMapsPath),

    {ok, #state{remotes_file_cache=RemotesPath,
                vbmaps_file_cache=VBMapsPath}}.

handle_call({get_remote_cluster, Cluster}, From, State) ->
    proc_lib:spawn_link(
      fun () ->
              R = remote_cluster(Cluster),
              gen_server:reply(From, R)%% ,

              %% case R of
              %%     {ok, RemoteCluster} ->
              %%         gen_server:cast(?MODULE,
              %%                         {cache_remote_cluster,
              %%                          Cluster, RemoteCluster});
              %%     _ ->
              %%         ok
              %% end
      end),
    {noreply, State};
handle_call({get_remote_vbucket_map, Cluster, Bucket}, From, State) ->
    proc_lib:spawn_link(
      fun () ->
              R = case remote_vbmap(Cluster, Bucket) of
                      {ok, {_Nodes, VBucketMap}} ->
                          {ok, VBucketMap};
                      Error ->
                          Error
                  end,
              gen_server:reply(From, R)
      end),
    {noreply, State};
handle_call(Request, From, State) ->
    ?log_warning("Got unexpected call request: ~p", {Request, From}),
    {reply, unhandled, State}.

handle_cast({cache_remote_cluster, _Cluster, _RemoteCluster}, State) ->
    {noreply, State};
handle_cast(Msg, State) ->
    ?log_warning("Got unexpected cast: ~p", [Msg]),
    {noreply, State}.

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
            TableName = ets:new(TableName, [named_table, bag, protected]),
            ok
    end.

%% Internal functions
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
    Msg = io_lib:format("Bucket ~s not found.", [BucketName]),
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
    Hostname0 = proplists:get_value(<<"hostname">>, Props),
    true = (Hostname0 =/= undefined),
    Hostname = binary_to_list(Hostname0),

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

remote_vbmap(Cluster, BucketStr) ->
    Username = proplists:get_value(username, Cluster),
    Password = proplists:get_value(password, Cluster),
    Hostname = proplists:get_value(hostname, Cluster),
    UUID = proplists:get_value(uuid, Cluster),

    true = (Username =/= undefined),
    true = (Password =/= undefined),
    true = (Hostname =/= undefined),

    {Host, Port} = host_and_port(Hostname),

    Bucket = list_to_binary(BucketStr),

    JsonGet = mk_json_get(Host, Port, Username, Password),

    with_pools(
      JsonGet,
      fun (Pools, ActualUUID) ->
              case ActualUUID =:= UUID of
                  true ->
                      with_default_pool_details(
                        Pools, JsonGet,
                        fun (PoolDetails) ->
                                remote_vbmap_with_pool_details(PoolDetails,
                                                               Bucket, JsonGet)

                        end);
                  false ->
                      ?log_info("Attempted to get vbucket map for bucket `~s` "
                                "from remote node ~s:~b. But cluster's "
                                "uuid (~s) does not match expected one (~s)",
                                [Bucket, Host, Port, ActualUUID, UUID]),
                      {error, other_cluster}
              end
      end).

remote_vbmap_with_pool_details(PoolDetails, Bucket, JsonGet) ->
    with_nodes(
      PoolDetails, <<"default pool details">>,
      [{<<"hostname">>, fun extract_string/2}],
      fun (PoolNodeProps) ->
              with_buckets(
                PoolDetails, JsonGet,
                fun (Buckets) ->
                        with_bucket(
                          Buckets, Bucket,
                          fun (BucketObject, UUID) ->
                                  with_nodes(
                                    BucketObject, <<"bucket details">>,
                                    [{<<"hostname">>, fun extract_string/2},
                                     {<<"couchApiBase">>, fun extract_string/2},
                                     {<<"ports">>, fun extract_object/2}],
                                    fun (BucketNodeProps) ->
                                            remote_vbmap_with_bucket(BucketObject,
                                                                     UUID,
                                                                     PoolNodeProps,
                                                                     BucketNodeProps)
                                    end)
                          end)
                end)
      end).

remote_vbmap_with_bucket(BucketObject, UUID, PoolNodeProps, BucketNodeProps) ->
    PoolNodes = lists:map(fun props_to_remote_node/1, PoolNodeProps),
    BucketNodes = lists:map(fun props_to_remote_node/1, BucketNodeProps),

    RemoteNodes = lists:usort(PoolNodes ++ BucketNodes),

    with_mcd_to_couch_uri_dict(
      BucketNodeProps,
      fun (McdToCouchDict) ->
              expect_nested_object(
                <<"vBucketServerMap">>, BucketObject, <<"bucket details">>,
                fun (VBucketServerMap) ->
                        remote_vbmap_with_server_map(VBucketServerMap, UUID,
                                                     RemoteNodes, McdToCouchDict)
                end)
      end).

remote_vbmap_with_server_map(ServerMap, UUID, RemoteNodes, McdToCouchDict) ->
    with_server_list(
      ServerMap,
      fun (ServerList) ->
              case build_ix_to_couch_uri_dict(ServerList,
                                              McdToCouchDict) of
                  {ok, IxToCouchDict} ->
                      expect_nested_array(
                        <<"vBucketMap">>, ServerMap, <<"vbucket server map">>,
                        fun (VBucketMap) ->
                                {ok,
                                 {RemoteNodes,
                                  build_vbmap(VBucketMap, UUID, IxToCouchDict)}}
                        end);
                  Error ->
                      Error
              end
      end).

build_vbmap(RawVBucketMap, UUID, IxToCouchDict) ->
    do_build_vbmap(RawVBucketMap, UUID, IxToCouchDict, 0, dict:new()).

do_build_vbmap([], _, _, _, D) ->
    D;
do_build_vbmap([ChainRaw | Rest], UUID, IxToCouchDict, VBucket, D) ->
    expect_array(
      ChainRaw, <<"vbucket map chain">>,
      fun (Chain) ->
              case build_vbmap_chain(Chain, UUID, IxToCouchDict, VBucket) of
                  {ok, FinalChain} ->
                      do_build_vbmap(Rest, UUID, IxToCouchDict,
                                     VBucket + 1,
                                     dict:store(VBucket, FinalChain, D));
                  Error ->
                      Error
              end
      end).

build_vbmap_chain(Chain, UUID, IxToCouchDict, VBucket) ->
    do_build_vbmap_chain(Chain, UUID, IxToCouchDict, VBucket, []).

do_build_vbmap_chain([], _, _, _, R) ->
    {ok, lists:reverse(R)};
do_build_vbmap_chain([NodeIxRaw | Rest], UUID, IxToCouchDict, VBucket, R) ->
    expect_number(
      NodeIxRaw, <<"Vbucket map chain">>,
      fun (NodeIx) ->
              case NodeIx of
                  -1 ->
                      do_build_vbmap_chain(Rest, UUID, IxToCouchDict,
                                           VBucket, [undefined | R]);
                  _ ->
                      case dict:find(NodeIx, IxToCouchDict) of
                          error ->
                              Msg = io_lib:format("Invalid node reference in "
                                                  "vbucket map chain: ~p", [NodeIx]),
                              ?log_error("~s", Msg),
                              {error, bad_value, iolist_to_binary(Msg)};
                          {ok, URL} ->
                              VBucketURL0 = [URL, "%2f", integer_to_list(VBucket),
                                             "?bucket_uuid=", UUID],
                              VBucketURL = iolist_to_binary(VBucketURL0),
                              do_build_vbmap_chain(Rest, UUID, IxToCouchDict,
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

with_mcd_to_couch_uri_dict(NodeProps, K) ->
    do_with_mcd_to_couch_uri_dict(NodeProps, dict:new(), K).

do_with_mcd_to_couch_uri_dict([], Dict, K) ->
    K(Dict);
do_with_mcd_to_couch_uri_dict([Props | Rest], Dict, K) ->
    Hostname = proplists:get_value(<<"hostname">>, Props),
    CouchApiBase = proplists:get_value(<<"couchApiBase">>, Props),
    Ports = proplists:get_value(<<"ports">>, Props),

    %% this is ensured by `extract_node_props' function
    true = (Hostname =/= undefined),
    true = (CouchApiBase =/= undefined),
    true = (Ports =/= undefined),

    {Host, _Port} = host_and_port(Hostname),

    expect_nested_number(
      <<"direct">>, Ports, <<"node ports object">>,
      fun (DirectPort) ->
              McdUri = iolist_to_binary([Host, $:, integer_to_list(DirectPort)]),
              NewDict = dict:store(McdUri, CouchApiBase, Dict),

              do_with_mcd_to_couch_uri_dict(Rest, NewDict, K)
      end).

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
    case binary:split(Reference, $/, [global]) of
        [<<>>, <<"remoteClusters">>, ClusterName, <<"buckets">>, BucketName] ->
            {ok, {mochiweb_util:unquote(ClusterName),
                  mochiweb_util:unquote(BucketName)}};
        _ ->
            {error, bad_reference}
    end.

find_cluster_by_name(ClusterName) ->
    Config = ns_config:get(),
    Clusters = ns_config:search(Config, remote_clusters, []),

    case lists:dropwhile(Clusters,
                         fun (Cluster) ->
                                 Name = proplists:get_value(name, Cluster),
                                 true = (Name =/= undefined),

                                 Name =/= ClusterName
                         end) of
        [] ->
            {error, cluster_not_found,
             <<"Requested cluster not found">>};
        [Cluster | _] ->
            Cluster
    end.
