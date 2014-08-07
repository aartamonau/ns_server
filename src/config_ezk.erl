-module(config_ezk).

-behaviour(config).

-export([init/1, terminate/1]).
-export([handle_get/3, handle_create/4, handle_set/4, handle_set/5,
         handle_delete/3, handle_delete/4]).
-export([handle_msg/2]).

-include_lib("ezk/include/ezk.hrl").

init(_Args) ->
    application:start(ezk),
    %% TODO: get configuration from Args
    case ezk:start_connection([], [self()]) of
        {ok, Conn} ->
            {ok, Conn};
        {error, Error} ->
            {error, {cant_create_ezk_connection, Error}}
    end.

terminate(Conn) ->
    ok = ezk:end_connection(Conn, "terminating").

handle_get(Path, Tag, Conn) ->
    ok = ezk:n_sync(Conn, Path, self(), {synced, Path, Tag}),
    {ok, Conn}.

handle_create(Path, Value, Tag, Conn) ->
    ok = ezk:n_create(Conn, Path, term_to_binary(Value),
                      self(), {reply, create, Tag}),
    {ok, Conn}.

handle_set(Path, Value, Tag, Conn) ->
    ok = ezk:n_set(Conn, Path, term_to_binary(Value),
                   self(), {reply, set, Tag}),
    {ok, Conn}.

handle_set(Path, Value, Version, Tag, Conn) ->
    ok = ezk:n_set(Conn, Path, term_to_binary(Value), Version,
                   self(), {reply, set, Tag}),
    {ok, Conn}.

handle_delete(Path, Tag, Conn) ->
    ok = ezk:n_delete(Conn, Path, self(), {reply, delete, Tag}),
    {ok, Conn}.

handle_delete(Path, Version, Tag, Conn) ->
    ok = ezk:n_delete(Conn, Path, Version, self(), {reply, delete, Tag}),
    {ok, Conn}.

handle_msg({{synced, Path, Tag}, RV}, Conn) ->
    case RV of
        {ok, _Path} ->
            ok = ezk:n_get(Conn, Path, self(), {reply, get, Tag}),
            {noreply, Conn};
        {error, Error} ->
            {error, translate_error(Error)}
    end;
handle_msg({{reply, ReplyType, Tag}, RV}, Conn) ->
    {reply, Tag, translate_reply(ReplyType, RV), Conn};
handle_msg(_, _Conn) ->
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
