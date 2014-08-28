-module(node_uuid).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/0, get/0, reset/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {path :: string()}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get() ->
    [{uuid, UUID}] = ets:lookup(?MODULE, uuid),
    UUID.

reset() ->
    gen_server:call(?MODULE, reset, infinity).

%% gen_server callbacks
init([]) ->
    ets:new(?MODULE, [protected, named_table, set]),
    Path = path_config:component_path(data, "uuid"),

    case misc:raw_read_file(Path) of
        {ok, UUID} ->
            ?log_debug("Read uuid ~p from ~p", [UUID, Path]),
            true = ets:insert_new(?MODULE, {uuid, UUID}),
            {ok, #state{path = Path}, hibernate};
        {error, enoent} ->
            ?log_debug("Couldn't find uuid file ~p", [Path]),
            case reset_uuid(Path) of
                {ok, _UUID} ->
                    {ok, #state{path = Path}, hibernate};
                {error, Error} ->
                    {stop, Error}
            end;
        {error, Error} ->
            ?log_error("Couldn't read ~p: ~p", [Path, Error]),
            {stop, Error}
    end.

handle_call(reset, _From, #state{path = Path} = State) ->
    case reset_uuid(Path) of
        {ok, UUID} ->
            {reply, UUID, State, hibernate};
        {error, Error}  ->
            {stop, Error, State}
    end;
handle_call(Request, From, State) ->
    ?log_warning("Received unexpected call request: ~p", [{Request, From}]),
    {reply, unhandled, State, hibernate}.

handle_cast(Msg, State) ->
    ?log_warning("Received unexpected cast: ~p", [Msg]),
    {noreply, State, hibernate}.

handle_info(Info, State) ->
    ?log_warning("Received unexpected message: ~p", [Info]),
    {noreply, State, hibernate}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% internal
reset_uuid(Path) ->
    UUID = couch_uuids:random(),
    ?log_debug("Generated new uuid ~p", [UUID]),
    case misc:atomic_write_file(Path, UUID) of
        ok ->
            ?log_debug("Saved uuid to ~p", [Path]),
            ets:insert(?MODULE, {uuid, UUID}),
            {ok, UUID};
        Error ->
            ?log_error("Couldn't save uuid file to ~p: ~p", [Path, Error]),
            {error, {uuid_save_failed, Error}}
    end.
