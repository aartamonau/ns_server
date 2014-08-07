-module(config).

-behaviour(gen_server).

-export([start_link/2]).
-export([get/1, create/2, set/2, set/3, delete/1, delete/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-type cfg_path() :: string().
-type cfg_value() :: any().
-type cfg_version() :: any().
-type cfg_state() :: any().
-type cfg_error() :: any().

-callback init(Args :: any()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback terminate(cfg_state()) ->
    ok.

-callback handle_get(cfg_path(), reference(), cfg_state()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback handle_create(cfg_path(), cfg_value(), reference(), cfg_state()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback handle_set(cfg_path(), cfg_value(), reference(), cfg_state()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback handle_set(cfg_path(), cfg_value(), cfg_version(),
                     reference(), cfg_state()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback handle_delete(cfg_path(), reference(), cfg_state()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback handle_delete(cfg_path(), cfg_version(), reference(), cfg_state()) ->
    {ok, cfg_state()} | {error, cfg_error()}.

-callback handle_msg(any(), cfg_state()) ->
    {noreply, cfg_state()} |
    {reply, reference(), any(), cfg_state()} |
    {stop, any(), cfg_state()} |
    ignore.

-include("ns_common.hrl").

-record(state, { backend :: module(),
                 backend_state :: any(),
                 requests :: ets:tid() }).

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

init({Backend, Args}) ->
    case Backend:init(Args) of
        {ok, BackendState} ->
            {ok, #state{backend = Backend,
                        backend_state = BackendState,
                        requests = ets:new(ok, [set, protected])}};
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
    delegate_call(handle_delete, [Path, Version], From, State).

handle_cast(_, State) ->
    {noreply, State}.

handle_info(Msg, #state{backend = Backend,
                        backend_state = BackendState} = State) ->
    case Backend:handle_msg(Msg, BackendState) of
        {noreply, NewBackendState} ->
            {noreply, State#state{backend_state = NewBackendState}};
        {reply, Tag, RV, NewBackendState} ->
            reply(Tag, RV, State),
            {noreply, State#state{backend_state = NewBackendState}};
        ignore ->
            ?log_warning("got unexpected message ~p", [Msg]),
            {noreply, State};
        {stop, Reason, State} ->
            {stop, Reason, State}
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{backend = Backend,
                          backend_state = BackendState}) ->
    ok = Backend:terminate(BackendState).

reply(Tag, RV, #state{requests = Requests}) ->
    [{Tag, From}] = ets:lookup(Requests, Tag),
    gen_server:reply(From, RV),
    ets:delete(Requests, Tag).

delegate_call(Call, Args, From, #state{backend = Backend,
                                       backend_state = BackendState,
                                       requests = Requests} = State) ->

    Tag = make_ref(),
    case erlang:apply(Backend, Call, Args ++ [Tag, BackendState]) of
        {ok, NewBackendState} ->
            true = ets:insert_new(Requests, {Tag, From}),
            {noreply, State#state{backend_state = NewBackendState}};
        {error, _} = Error ->
            {reply, Error, State}
    end.
