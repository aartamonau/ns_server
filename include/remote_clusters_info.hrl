-record(remote_node, {host :: string(),
                      port :: integer()}).

-record(remote_cluster, {uuid :: binary(),
                         nodes :: [#remote_node{}]}).
