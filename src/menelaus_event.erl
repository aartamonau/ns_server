%% @author Northscale <info@northscale.com>
%% @copyright 2009 NorthScale, Inc.
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
%%
-module(menelaus_event).

-behaviour(gen_server).

% Allows menelaus erlang processes (especially long-running HTTP /
% REST streaming processes) to register for messages when there
% are configuration changes.

-export([start_link/0]).

-export([register_watcher/1,
         unregister_watcher/1]).

%% gen_event callbacks

-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {webconfig, watchers = []}).

-include("ns_common.hrl").

% Noop process to get initialized in the supervision tree.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_watcher(Pid) ->
    gen_server:call(?MODULE, {register_watcher, Pid}).

unregister_watcher(Pid) ->
    gen_server:call(?MODULE, {unregister_watcher, Pid}).

%% Implementation

init(_) ->
    Self = self(),
    MsgFun = fun (Event) -> Self ! {event, Event} end,

    ns_pubsub:subscribe_link(ns_config_events, MsgFun),
    ns_pubsub:subscribe_link(ns_node_disco_events, MsgFun),
    ns_pubsub:subscribe_link(buckets_events, MsgFun),

    {ok, #state{watchers = [],
                webconfig = menelaus_web:webconfig()}}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_call({register_watcher, Pid}, _From,
            #state{watchers = Watchers} = State) ->
    Watchers2 = case lists:keysearch(Pid, 1, Watchers) of
                    false -> MonitorRef = erlang:monitor(process, Pid),
                             [{Pid, MonitorRef} | Watchers];
                    _     -> Watchers
                end,
    {reply, ok, State#state{watchers = Watchers2}};

handle_call({unregister_watcher, Pid}, _From,
            #state{watchers = Watchers} = State) ->
    Watchers2 = case lists:keytake(Pid, 1, Watchers) of
                    false -> Watchers;
                    {value, {Pid, MonitorRef}, WatchersRest} ->
                        erlang:demonitor(MonitorRef, [flush]),
                        WatchersRest
                end,
    {reply, ok, State#state{watchers = Watchers2}};

handle_call(Request, From, State) ->
    ?log_warning("Unexpected handle_call(~p, ~p, ~p)", [Request, From, State]),
    {reply, unhandled, State}.

handle_cast(Msg, State) ->
    ?log_warning("Unexpected handle_cast(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_info({event, {{node, Node, rest}, _}}, State) when Node =:= node() ->
    NewState = maybe_restart(State),
    {noreply, NewState};

handle_info({event, {rest, _}}, State) ->
    NewState = maybe_restart(State),
    {noreply, NewState};

handle_info({event, Event}, State) ->
    case is_interesting_to_watchers(Event) of
        true ->
            ok = notify_watchers(State);
        _ ->
            ok
    end,
    {noreply, State};

handle_info({'DOWN', MonitorRef, _, _, _},
            #state{watchers = Watchers} = State) ->
    Watchers2 = case lists:keytake(MonitorRef, 2, Watchers) of
                    false -> Watchers;
                    {value, {_Pid, MonitorRef}, WatchersRest} ->
                        erlang:demonitor(MonitorRef, [flush]),
                        WatchersRest
                end,
    {noreply, State#state{watchers = Watchers2}};

handle_info(_Info, State) ->
    {noreply, State}.

% ------------------------------------------------------------
is_interesting_to_watchers({significant_buckets_change, _}) -> true;
is_interesting_to_watchers({memcached, _}) -> true;
is_interesting_to_watchers({{node, _, memcached}, _}) -> true;
is_interesting_to_watchers({rebalance_status, _}) -> true;
is_interesting_to_watchers({recovery_status, _}) -> true;
is_interesting_to_watchers({buckets, _}) -> true;
is_interesting_to_watchers({nodes_wanted, _}) -> true;
is_interesting_to_watchers({server_groups, _}) -> true;
is_interesting_to_watchers({ns_node_disco_events, _NodesBefore, _NodesAfter}) -> true;
is_interesting_to_watchers({autocompaction, _}) -> true;
is_interesting_to_watchers({cluster_compat_version, _}) -> true;
is_interesting_to_watchers({internal_visual_settings, _}) -> true;
is_interesting_to_watchers(_) -> false.

notify_watchers(#state{watchers = Watchers}) ->
    lists:foreach(fun({Pid, _}) ->
                          Pid ! notify_watcher
                  end,
                  Watchers),
    ok.

maybe_restart(#state{webconfig=WebConfigOld} = State) ->
    WebConfigNew = menelaus_web:webconfig(),
    case WebConfigNew =:= WebConfigOld of
        true -> State;
        false -> spawn(fun menelaus_web:restart/0),
                 State#state{webconfig=WebConfigNew}
    end.
