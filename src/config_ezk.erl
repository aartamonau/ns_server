-module(config_ezk).

-behaviour(config).

-export([init/1, terminate/2]).
-export([handle_get/3, handle_create/4, handle_set/4, handle_set/5,
         handle_delete/3, handle_delete/4, handle_watch/4, handle_unwatch/3]).
-export([handle_msg/2]).

-include_lib("ezk/include/ezk.hrl").
-include("ns_common.hrl").

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
            {ok, #state{connection = Conn,
                        watches = ets:new(ok, [set, protected])}};
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
    ok = ezk:n_sync(Conn, Path, self(), {synced, Path, Tag}),
    {noreply, State}.

handle_create(Path, Value, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_create(Conn, Path, term_to_binary(Value),
                      self(), {reply, create, Tag}),
    {noreply, State}.

handle_set(Path, Value, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_set(Conn, Path, term_to_binary(Value),
                   self(), {reply, set, Tag}),
    {noreply, State}.

handle_set(Path, Value, Version, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_set(Conn, Path, term_to_binary(Value), Version,
                   self(), {reply, set, Tag}),
    {noreply, State}.

handle_delete(Path, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_delete(Conn, Path, self(), {reply, delete, Tag}),
    {noreply, State}.

handle_delete(Path, Version, Tag, #state{connection = Conn} = State) ->
    ok = ezk:n_delete(Conn, Path, Version, self(), {reply, delete, Tag}),
    {noreply, State}.

handle_watch(Paths, WatchRef, Tag, #state{connection = Conn,
                                          watches = Watches} = State) ->
    lists:foreach(
      fun (Path) ->
              ok = ezk:n_exists(Conn, Path, self(),
                                {watch, Path, WatchRef},
                                {watch_reply, Path, Tag, WatchRef})
      end, Paths),
    true = ets:insert_new(Watches,
                          {WatchRef, #pending_watch{paths_left = Paths,
                                                    paths_triggered = []}}),
    {noreply, State}.

handle_unwatch(WatchRef, Tag, #state{watches = Watches} = State) ->
    ets:delete(WatchRef, Watches),
    {reply, Tag, ok, State}.

handle_msg({{synced, Path, Tag}, RV}, #state{connection = Conn} = State) ->
    case RV of
        {ok, _Path} ->
            ok = ezk:n_get(Conn, Path, self(), {reply, get, Tag}),
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
                                   paths_triggered = Triggered}}] ->
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
                            {reply, Tag, ok, State};
                        NewPaths ->
                            ets:insert(Watches,
                                       {WatchRef,
                                        #pending_watch{paths_left = NewPaths}}),
                            {noreply, State}
                    end
            end;
        [] ->
            ?log_debug("Ignoring watch_reply with unknown tag: ~p", [Msg]),
            {noreply, State}
    end;
handle_msg({{watch_rearm_reply, Path}, RV}, State) ->
    case RV of
        {error, Error} when Error =/= no_node ->
            {stop, {couldnt_rearm_watch, Path, translate_error(Error)}, State};
        _ ->
            {noreply, State}
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
            ok = ezk:n_exists(Conn, Path, self(),
                              {watch, Path, WatchRef},
                              {watch_rearm_reply, Path}),
            {notify_watch, WatchRef, Path, State};
        [{WatchRef, #pending_watch{paths_triggered = Triggered}}] ->
            ets:insert(Watches,
                       {WatchRef,
                        #pending_watch{paths_triggered = [Path | Triggered]}}),
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
