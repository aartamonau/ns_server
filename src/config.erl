-module(config).

-behaviour(gen_server).

-export([start_link/2]).
-export([get/1, create/2, set/2, set/3, delete/1, delete/2]).
-export([watch/1, unwatch/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-type cfg_path() :: string().
-type cfg_value() :: any().
-type cfg_version() :: any().
-type cfg_state() :: any().
-type cfg_error() :: any().
-type cfg_reason() :: any().

-callback init(Args :: any()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback terminate(cfg_reason(), cfg_state()) ->
    ok.

-callback handle_get(cfg_path(), reference(), cfg_state()) ->
    {noreply, cfg_state()} | {reply, any(), cfg_state()}.

-callback handle_create(cfg_path(), cfg_value(), reference(), cfg_state()) ->
    {noreply, cfg_state()} | {reply, any(), cfg_state()}.

-callback handle_set(cfg_path(), cfg_value(), reference(), cfg_state()) ->
    {noreply, cfg_state()} | {reply, any(), cfg_state()}.

-callback handle_set(cfg_path(), cfg_value(), cfg_version(),
                     reference(), cfg_state()) ->
    {noreply, cfg_state()} | {reply, any(), cfg_state()}.

-callback handle_delete(cfg_path(), reference(), cfg_state()) ->
    {noreply, cfg_state()} | {reply, any(), cfg_state()}.

-callback handle_delete(cfg_path(), cfg_version(), reference(), cfg_state()) ->
    {noreply, cfg_state()} | {reply, any(), cfg_state()}.

-callback handle_msg(any(), cfg_state()) ->
    {noreply, cfg_state()} |
    {reply, reference(), any(), cfg_state()} |
    {stop, any(), cfg_state()} |
    ignore.

-include("ns_common.hrl").

-record(state, { backend :: module(),
                 backend_state :: any(),
                 requests :: ets:tid(),
                 watches :: ets:tid() }).

start_link(Backend, Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Backend, Args}, []).

-spec get(cfg_path()) ->
                 {ok, {cfg_value(), cfg_version()}} | {error, cfg_error()}.
get(Path) ->
    gen_server:call(?MODULE, {get, Path}, infinity).

-spec create(cfg_path(), cfg_value()) ->
                    {ok, cfg_version()} | {error, cfg_error()}.
create(Path, Value) ->
    gen_server:call(?MODULE, {create, Path, Value}, infinity).

-spec set(cfg_path(), cfg_value()) ->
                 {ok, cfg_version()} | {error, cfg_error()}.
set(Path, Value) ->
    gen_server:call(?MODULE, {set, Path, Value}, infinity).

-spec set(cfg_path(), cfg_value(), cfg_version()) ->
                 {ok, cfg_version()} | {error, cfg_error()}.
set(Path, Value, Version) ->
    gen_server:call(?MODULE, {set, Path, Value, Version}, infinity).

-spec delete(cfg_path()) -> ok | {error, cfg_error()}.
delete(Path) ->
    gen_server:call(?MODULE, {delete, Path}, infinity).

-spec delete(cfg_path(), cfg_version()) -> ok | {error, cfg_error()}.
delete(Path, Version) ->
    gen_server:call(?MODULE, {delete, Path, Version}, infinity).

watch(Paths) ->
    gen_server:call(?MODULE, {watch, Paths}, infinity).

unwatch(WatchRef) ->
    gen_server:call(?MODULE, {unwatch, WatchRef}, infinity).

init({Backend, Args}) ->
    case Backend:init(Args) of
        {ok, BackendState} ->
            {ok, #state{backend = Backend,
                        backend_state = BackendState,
                        requests = ets:new(ok, [set, protected]),
                        watches = ets:new(ok, [set, protected])}};
        {error, _} = Error ->
            Error
    end.

handle_call({get, Path}, From, State) ->
    delegate_call(handle_get, [Path], From, State);
handle_call({create, Path, Value}, From, State) ->
    delegate_call(handle_create, [Path, Value], From, State);
handle_call({set, Path, Value}, From, State) ->
    delegate_call(handle_set, [Path, Value], From, State);
handle_call({set, Path, Value, Version}, From, State) ->
    delegate_call(handle_set, [Path, Value, Version], From, State);
handle_call({delete, Path}, From, State) ->
    delegate_call(handle_delete, [Path], From, State);
handle_call({delete, Path, Version}, From, State) ->
    delegate_call(handle_delete, [Path, Version], From, State);
handle_call({watch, Paths}, {FromPid, _} = From,
            #state{watches = Watches} = State) ->
    WatchRef = erlang:monitor(process, FromPid),
    true = ets:insert_new(Watches, {WatchRef, FromPid}),
    delegate_call(handle_watch, [Paths, WatchRef], From, State);
handle_call({unwatch, WatchRef}, From, #state{watches = Watches} = State) ->
    ets:delete(Watches, WatchRef),
    delegate_call(handle_unwatch, [WatchRef], From, State);
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
        [{Ref, Pid}] ->
            ?log_debug("Removing watch ~p because ~p died with reason ~p",
                       [Ref, Pid, Reason]),
            %% TODO: don't expect this to reply immediately
            {reply, _, _} = handle_call({unwatch, Ref}, unused, State)
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
      fun ({WatchRef, Pid}) ->
              Pid ! {watch_lost, WatchRef}
      end, ets:tab2list(Watches)).

reply(Tag, RV, #state{requests = Requests}) ->
    [{Tag, From}] = ets:lookup(Requests, Tag),
    gen_server:reply(From, RV),
    ets:delete(Requests, Tag).

delegate_call(Call, Args, From, #state{backend = Backend,
                                       backend_state = BackendState,
                                       requests = Requests} = State) ->
    Tag = make_ref(),
    case erlang:apply(Backend, Call, Args ++ [Tag, BackendState]) of
        {noreply, NewBackendState} ->
            true = ets:insert_new(Requests, {Tag, From}),
            {noreply, State#state{backend_state = NewBackendState}};
        {reply, Reply, NewBackendState} ->
            {reply, Reply, State#state{backend_state = NewBackendState}}
    end.

notify_watch(WatchRef, Path, #state{watches = Watches}) ->
    [{WatchRef, Pid}] = ets:lookup(Watches, WatchRef),
    Pid ! {watch, WatchRef, Path},
    ok.

handle_other_msg(Msg, #state{backend = Backend,
                             backend_state = BackendState} = State) ->
    case Backend:handle_msg(Msg, BackendState) of
        {noreply, NewBackendState} ->
            {noreply, State#state{backend_state = NewBackendState}};
        {reply, Tag, RV, NewBackendState} ->
            reply(Tag, RV, State),
            {noreply, State#state{backend_state = NewBackendState}};
        {notify_watch, WatchRef, Path, NewBackendState} ->
            notify_watch(WatchRef, Path, State),
            {noreply, State#state{backend_state = NewBackendState}};
        ignore ->
            ?log_warning("Got unexpected message ~p", [Msg]),
            {noreply, State};
        {stop, Reason, State} ->
            {stop, Reason, State}
    end.
