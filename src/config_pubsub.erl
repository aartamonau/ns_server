-module(config_pubsub).

-include("ns_common.hrl").

%% API
-export([subscribe_link/1, subscribe_link/2, subscribe_link/3, unsubscribe/1]).

%% called by proc_lib:start from subscribe_link/3
-export([do_subscribe_link/4]).

subscribe_link(Paths) ->
    subscribe_link(Paths, msg_fun(self()), ignored).

-spec subscribe_link([config:path()], fun((term()) -> Ignored :: any())) -> pid().
subscribe_link(Paths, Fun) ->
    subscribe_link(
      Paths,
      fun (Event, State) ->
              Fun(Event),
              State
      end, ignored).

-spec subscribe_link([config:path()],
                     fun((Event :: term(), State :: any()) -> NewState :: any()),
                     InitState :: any()) -> pid().
subscribe_link(Paths, Fun, InitState) ->
    proc_lib:start(?MODULE, do_subscribe_link, [Paths, Fun, InitState, self()]).

unsubscribe(Pid) ->
    Pid ! unsubscribe,
    misc:wait_for_process(Pid, infinity),

    %% consume exit message in case trap_exit is true
    receive
        %% we expect the process to die normally; if it's not the case then
        %% this should be handled explicitly by parent process;
        {'EXIT', Pid, normal} ->
            ok
    after 0 ->
            ok
    end.

%%
%% Internal functions
%%
do_subscribe_link(Paths, Fun, State, Parent) ->
    process_flag(trap_exit, true),
    erlang:link(Parent),

    case config:watch(Paths) of
        {ok, WatchRef} ->
            ?log_debug("Subscribed to watch ~p for process ~p. WatchRef: ~p",
                       [Paths, Parent, WatchRef]),
            proc_lib:init_ack(Parent, {ok, self()}),
            do_subscribe_link_loop(WatchRef, Fun, State, Parent);
        {error, Error} ->
            ?log_error("Couldn't subscribe to watch ~p for process ~p: ~p",
                       [Paths, Parent, Error]),
            proc_lib:init_ack(Parent, {error, Error})
    end.

do_subscribe_link_loop(WatchRef, Fun, State, Parent) ->
    receive
        unsubscribe ->
            exit(normal);
        {watch_lost, WatchRef, Reason}->
            case Reason =:= normal orelse Reason =:= shutdown of
                true ->
                    exit(normal);
                false ->
                    exit({config_crashed, Reason})
            end;
        {watch, WatchRef, Path} ->
            NewState = Fun({changed, Path}, State),
            do_subscribe_link_loop(WatchRef, Fun, NewState, Parent);
        {'EXIT', Parent, Reason} ->
            ?log_debug("Parent process ~p of subscription ~p"
                       "exited with reason ~p", [Parent, WatchRef, Reason]),
            exit(normal);
        {'EXIT', Pid, Reason} ->
            ?log_debug("Linked process ~p of subscription ~p "
                       "died unexpectedly with reason ~p",
                       [Pid, WatchRef, Reason]),
            exit({linked_process_died, Pid, Reason});
        X ->
            ?log_error("Subscription ~p got unexpected message: ~p",
                       [WatchRef, X]),
            exit({unexpected_message, X})
    end.

msg_fun(Pid) ->
    fun (Event, ignored) ->
            Pid ! Event,
            ignored
    end.
