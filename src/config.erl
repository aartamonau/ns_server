-module(config).

-behaviour(gen_server).

-export([start_link/2]).
-export([get/1, get/2, dirty_get/1, get_snapshot/0]).
-export([get_value/1, get_value/2, get_value/3]).
-export([dirty_get_value/1, dirty_get_value/2]).
-export([get_node_value/2, get_node_value/3, get_node_value/4]).
-export([dirty_get_node_value/2, dirty_get_node_value/3]).
-export([create/2, update/2, update/3, delete/1, delete/2]).
-export([set/1, set/2, must_set/1, must_set/2]).
-export([must_create/2, must_update/2, must_update/3]).
-export([must_delete/1, must_delete/2]).
-export([map_into/2]).
-export([watch/0, watch/1, unwatch/1]).
-export([reply/2, notify_watch/2]).
-export([multi/1]).
-export([exists_op/2, missing_op/1, create_op/2, update_op/2, update_op/3]).
-export([delete_op/1, delete_op/2]).
-export([path_components/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-type path() :: string().
-type value() :: any().
-type version() :: any().
-type state() :: any().
-type error() :: any().
-type reason() :: any().
-type watch_ref() :: any().
-type reply_ref() :: reference().
-type path_pred() :: fun ((path()) -> boolean()).
-type watch_opt() :: announce_initial | {announce_initial, boolean()} |
                     {path_pred, path_pred()}.
-type watch_opts() :: [watch_opt()].

-callback init(Args :: any()) ->
    {ok, state()} | {error, error()}.

-callback terminate(reason(), state()) ->
    ok.

-callback handle_get(path(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_create(path(), value(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_update(path(), value(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_update(path(), value(), version(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_delete(path(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_delete(path(), version(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_watch(boolean(), path_pred(), watch_ref(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_unwatch(watch_ref(), reply_ref(), state()) ->
    {noreply, state()} | {reply, any(), state()}.

-callback handle_msg(any(), state()) ->
    {noreply, state()} |
    {reply, reference(), any(), state()} |
    {stop, any(), state()} |
    ignore.

-include("ns_common.hrl").

-record(state, { backend :: module(),
                 backend_state :: any(),
                 watches :: ets:tid() }).

start_link(Backend, Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Backend, Args}, []).

-spec get(path()) ->
                 {ok, {value(), version()}} | {error, error()}.
get(Path) ->
    gen_server:call(?MODULE, {get, Path}, infinity).

dirty_get(Path) ->
    config:get(Path).

get(_Snapshot, Path) ->
    config:get(Path).

get_value(Path) ->
    case config:get(Path) of
        {ok, {V, _}} ->
            {ok, V};
        Error ->
            Error
    end.

dirty_get_value(Path) ->
    config:get_value(Path).

get_value(_Snapshot, Path) ->
    config:get_value(Path).

get_value(Snapshot, Path, Default) ->
    case config:get_value(Snapshot, Path) of
        {ok, V} ->
            V;
        {error, no_node} ->
            Default
    end.

dirty_get_value(Path, Default) ->
    config:get_value(unused, Path, Default).

get_node_value(Node, Path) ->
    NodePath = filename:join(["/node", Node, Path]),

    case config:get_value(NodePath) of
        {error, no_node} ->
            config:get_value(Path);
        Other ->
            Other
    end.

dirty_get_node_value(Node, Path) ->
    config:get_node_value(Node, Path).

get_node_value(_Snapshot, Node, Path) ->
    config:get_node_value(Node, Path).

get_node_value(Snapshot, Node, Path, Default) ->
    case config:get_node_value(Snapshot, Node, Path) of
        {ok, V} ->
            V;
        {error, no_node} ->
            Default
    end.

dirty_get_node_value(Node, Path, Default) ->
    config:get_node_value(unused, Node, Path, Default).

get_snapshot() ->
    ok.

-spec create(path(), value()) ->
                    {ok, version()} | {error, error()}.
create(Path, Value) ->
    gen_server:call(?MODULE, {create, Path, Value}, infinity).

must_create(Path, Value) ->
    {ok, _} = config:create(Path, Value).

-spec update(path(), value()) ->
                    {ok, version()} | {error, error()}.
update(Path, Value) ->
    gen_server:call(?MODULE, {update, Path, Value}, infinity).

must_update(Path, Value) ->
    {ok, _} = config:update(Path, Value).

-spec update(path(), value(), version()) ->
                    {ok, version()} | {error, error()}.
update(Path, Value, Version) ->
    gen_server:call(?MODULE, {update, Path, Value, Version}, infinity).

must_update(Path, Value, Version) ->
    {ok, _} = config:update(Path, Value, Version).

-spec set(path(), value()) -> {ok, version()} | {error, error()}.
set(Path, Value) ->
    RV = update(Path, Value),
    case RV of
        {ok, _} ->
            RV;
        {error, no_node} ->
            RV1 = create(Path, Value),
            case RV1 of
                {ok, _} ->
                    RV1;
                {error, node_exists} ->
                    set(Path, Value);
                {error, _} ->
                    RV1
            end;
        {error, _} ->
            RV
    end.

must_set(Path, Value) ->
    {ok, _} = config:set(Path, Value).

set(KVs) ->
    ExtKVs = [{K, V,
               case config:get(K) of
                   {ok, _} ->
                       true;
                   {error, no_node} ->
                       false
               end} || {K, V} <- KVs],
    try
        do_multi_set(ExtKVs)
    catch
        throw:retry ->
            set(KVs)
    end.

do_multi_set(KVs) ->
    Txn = [case Exists of
               true ->
                   config:update_op(Key, Value);
               false ->
                   config:create_op(Key, Value)
           end || {Key, Value, Exists} <- KVs],

    case config:multi(Txn) of
        {ok, Results} ->
            Retry =
                lists:any(
                  fun ({ok, _}) ->
                          false;
                      ({error, Type}) ->
                          case Type of
                              rolled_back ->
                                  true;
                              runtime_inconsistency ->
                                  true;
                              node_exists ->
                                  true;
                              no_node ->
                                  true;
                              _ ->
                                  false
                          end
                  end, Results),

            case Retry of
                true ->
                    throw(retry);
                false ->
                    {ok, Results}
            end;
        {error, _} = Error ->
            Error
    end.

must_set(KVs) ->
    {ok, Results} = set(KVs),
    [] = [R || {error, _} = R <- Results],
    ok.

-spec delete(path()) -> ok | {error, error()}.
delete(Path) ->
    gen_server:call(?MODULE, {delete, Path}, infinity).

must_delete(Path) ->
    ok = config:delete(Path).

-spec delete(path(), version()) -> ok | {error, error()}.
delete(Path, Version) ->
    gen_server:call(?MODULE, {delete, Path, Version}, infinity).

must_delete(Path, Version) ->
    ok = config:delete(Path, Version).

-spec watch() -> watch_ref().
watch() ->
    watch([]).

-spec watch(watch_opts()) -> watch_ref().
watch(Opts) ->
    AnnounceInitial = proplists:get_bool(announce_initial, Opts),
    PathPred = proplists:get_value(path_pred, Opts,
                                   fun (_) -> true end),

    gen_server:call(?MODULE, {watch, AnnounceInitial, PathPred}, infinity).

-spec unwatch(watch_ref()) -> ok.
unwatch(WatchRef) ->
    gen_server:call(?MODULE, {unwatch, WatchRef}, infinity).

multi(Operations) ->
    gen_server:call(?MODULE, {multi, Operations}, infinity).

exists_op(Path, Version) ->
    {exists, Path, Version}.

missing_op(Path) ->
    {missing, Path}.

create_op(Path, Data) ->
    {create, Path, Data}.

update_op(Path, Data) ->
    {update, Path, Data}.

update_op(Path, Data, Version) ->
    {update, Path, Data, Version}.

delete_op(Path) ->
    {delete, Path}.

delete_op(Path, Version) ->
    {delete, Path, Version}.

map_into(Fun, Paths) ->
    Config = config:get_snapshot(),
    Txn = lists:flatmap(
            fun (Path) ->
                    case config:get(Config, Path) of
                        {ok, {Value, Version}} ->
                            [config:update_op(Path, Fun(Path, Value), Version)];
                        {error, no_node} ->
                            []
                    end
            end, Paths),

    {ok, Results} = config:multi(Txn),

    Retry = lists:any(
              fun ({ok, _}) ->
                      false;
                  ({error, Type}) when Type =:= bad_version;
                                       Type =:= runtime_inconsistency;
                                       Type =:= rolled_back ->
                      true
              end, Results),

    case Retry of
        true ->
            config:map_into(Paths, Fun);
        false ->
            ok
    end.

%% utility functions
path_components(Path) ->
    ["/" | Components] = filename:split(Path),
    Components.

%% for use by backends only
reply(Tag, RV) ->
    gen_server:reply(Tag, RV).

notify_watch({_, Pid} = WatchRef, Msg) ->
    Pid ! {watch, WatchRef, Msg},
    ok.

%% gen_server_callbacks
init({Backend, Args}) ->
    case Backend:init(Args) of
        {ok, BackendState} ->
            {ok, #state{backend = Backend,
                        backend_state = BackendState,
                        watches = ets:new(ok, [set, protected])}};
        {error, _} = Error ->
            Error
    end.

handle_call({get, Path}, From, State) ->
    delegate_call(handle_get, [Path], From, State);
handle_call({create, Path, Value}, From, State) ->
    delegate_call(handle_create, [Path, Value], From, State);
handle_call({update, Path, Value}, From, State) ->
    delegate_call(handle_update, [Path, Value], From, State);
handle_call({update, Path, Value, Version}, From, State) ->
    delegate_call(handle_update, [Path, Value, Version], From, State);
handle_call({delete, Path}, From, State) ->
    delegate_call(handle_delete, [Path], From, State);
handle_call({delete, Path, Version}, From, State) ->
    delegate_call(handle_delete, [Path, Version], From, State);
handle_call({watch, Announce, PathPred}, {FromPid, _} = From,
            #state{watches = Watches} = State) ->
    WatchRef = erlang:monitor(process, FromPid),
    true = ets:insert_new(Watches, {WatchRef, FromPid}),
    delegate_call(handle_watch,
                  [Announce, PathPred, {WatchRef, FromPid}], From, State);
handle_call({unwatch, WatchRef}, From, #state{watches = Watches} = State) ->
    ets:delete(Watches, WatchRef),
    delegate_call(handle_unwatch, [WatchRef], From, State);
handle_call({multi, Operations}, From, State) ->
    delegate_call(handle_multi, [Operations], From, State);
handle_call(Request, From, State) ->
    ?log_warning("Got unknown call ~p from ~p", [Request, From]),
    {reply, unknown_call, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _, Reason} = Msg,
            #state{watches = Watches} = State) ->
    case ets:lookup(Watches, Ref) of
        [] ->
            handle_other_msg(Msg, State);
        [{Ref, Pid} = FullRef] ->
            ?log_debug("Removing watch ~p because ~p died with reason ~p",
                       [Ref, Pid, Reason]),
            %% TODO: don't expect this to reply immediately
            {reply, ok, State} = handle_call({unwatch, FullRef}, unused, State),
            {noreply, State}
    end;
handle_info(Msg, State) ->
    handle_other_msg(Msg, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, #state{backend = Backend,
                         backend_state = BackendState,
                         watches = Watches}) ->
    ok = Backend:terminate(Reason, BackendState),

    lists:foreach(
      fun ({_, Pid} = WatchRef) ->
              Pid ! {watch_lost, WatchRef, Reason}
      end, ets:tab2list(Watches)).

delegate_call(Call, Args, From, #state{backend = Backend,
                                       backend_state = BackendState} = State) ->
    case erlang:apply(Backend, Call, Args ++ [From, BackendState]) of
        {noreply, NewBackendState} ->
            {noreply, State#state{backend_state = NewBackendState}};
        {reply, Reply, NewBackendState} ->
            {reply, Reply, State#state{backend_state = NewBackendState}};
        {stop, Reason, NewBackendState} ->
            {stop, Reason, State#state{backend_state = NewBackendState}}
    end.

handle_other_msg(Msg, #state{backend = Backend,
                             backend_state = BackendState} = State) ->
    case Backend:handle_msg(Msg, BackendState) of
        {noreply, NewBackendState} ->
            {noreply, State#state{backend_state = NewBackendState}};
        ignore ->
            ?log_warning("Got unexpected message ~p", [Msg]),
            {noreply, State};
        {stop, Reason, NewBackendState} ->
            {stop, Reason, State#state{backend_state = NewBackendState}}
    end.
