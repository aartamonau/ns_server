-ifndef(ezk_ezk_HRL).
-define(ezk_ezk_HRL,1).
-define(LOG, ezk_log:put).

-record(ezk_stat, {czxid,
                   mzxid,
                   pzxid,
                   ctime,
                   mtime,
                   dataversion,
                   datalength,
                   number_children,
                   cversion,
                   aclversion,
                   ephe_owner}).

-type ezk_err()          :: rolled_back | system_error | runtime_inconsistency |
                            data_inconsistency | connection_loss |
                            marshalling_error | unimplemented |
                            operation_timeout | bad_arguments |
                            new_config_no_quorum | reconfig_inprogress |
                            api_error | no_node | no_auth | bad_version |
                            no_children_for_ephemerals | node_exists |
                            not_empty | session_expired | invalid_callback |
                            invalid_acl | auth_failed | session_moved |
                            not_readonly | ephemeral_on_local_session |
                            no_watcher | {unknown_server_error, neg_integer()}.
-type ezk_path()         :: string().
-type ezk_conpid()       :: pid().
-type ezk_data()         :: binary().
-type ezk_ctype()        :: e | es | s | se.
-type ezk_acl_perms()    :: [ezk_acl_perm()].
-type ezk_acl_perm()     :: r | w | c | d | a.
-type ezk_acl_scheme()   :: string().
-type ezk_acl_id()       :: string().
-type ezk_acl()          :: {ezk_acl_perms(), ezk_acl_scheme(), ezk_acl_id()}.
-type ezk_acls()         :: [ezk_acl()].
-type ezk_watchowner()   :: pid().
-type ezk_watchmessage() :: term().
-type ezk_ls2data()      :: {[ezk_path()], #ezk_stat{}}.
-type ezk_server()       :: {}.
-type ezk_monitor()      :: pid().
-type ezk_authreply()    :: {ok, authed} | {error, auth_failed} |
                            {error, unknown, binary()} | {error,  auth_in_progress}.
-type ezk_version()      :: integer().

-endif.
