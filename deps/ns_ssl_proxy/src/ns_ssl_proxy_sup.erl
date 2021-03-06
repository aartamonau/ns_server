%% @author Couchbase <info@couchbase.com>
%% @copyright 2013 Couchbase, Inc.
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

-module(ns_ssl_proxy_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one,
            misc:get_env_default(max_r, 3),
            misc:get_env_default(max_t, 10)}, child_specs()} }.

child_specs() ->
    [
     {ns_ssl_proxy_server_sup, {ns_ssl_proxy_server_sup, start_link, []},
      permanent, infinity, supervisor,
      [ns_ssl_proxy_server_sup]},

     {ns_ssl_downstream_proxy_listener, {ns_ssl_downstream_proxy_listener, start_link, []},
      permanent, brutal_kill, worker,
      []},

     {ns_ssl_upstream_proxy_listener, {ns_ssl_upstream_proxy_listener, start_link, []},
      permanent, brutal_kill, worker,
      []}
    ].
