%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

%% couchdb headers
-include("couch_db.hrl").
-include("couch_js_functions.hrl").
-include("couch_api_wrap.hrl").
-include("../lhttpc/lhttpc.hrl").

%% ns_server headers
-include("ns_common.hrl").
-include("replication_infos_ddoc.hrl").

%% imported functions
-import(couch_util, [
                     get_value/2,
                     get_value/3,
                     to_binary/1
                    ]).

%% constants used by XDCR
-define(REP_ID_VERSION, 2).

%% Maximum number of concurrent vbucket replications allowed per doc
-define(MAX_CONCURRENT_REPS_PER_DOC, 8).

%% Number of seconds after which the scheduler will periodically wakeup
-define(XDCR_SCHEDULING_INTERVAL, 5).

%% Interval of checkpointing in ms
-define(XDCR_CHECKPOINT_INTERVAL, 60000).

%% Internal ETS tables used by XDCR
-define(XSTORE, xdc_rep_info_store).
-define(X2CSTORE, xdc_docid_to_couch_rep_pid_store).
-define(CSTORE, couch_rep_info_store).
-define(XSTATS, xdc_rep_stats_store).

%% Max number of replicaiton Pids to store in stat table
-define(XSTATS_MAX_NUM_REP_PIDS, 1024).

%% TODO: maybe make both buffer max sizes configurable
-define(DOC_BUFFER_BYTE_SIZE, 512 * 1024).   %% for remote targets
-define(DOC_BUFFER_LEN, 10).                 %% for local targets, # of documents
-define(MAX_BULK_ATT_SIZE, 64 * 1024).
-define(MAX_BULK_ATTS_PER_DOC, 8).
-define(STATS_DELAY, 10000000).              %% 10 seconds (in microseconds)

-define(inc_stat(StatPos, Stats, Inc),
        setelement(StatPos, Stats, element(StatPos, Stats) + Inc)).

%% data structures
-record(rep, {
          id,
          source,
          target,
          options,
          user_ctx,
          doc_id,
          vb_id,
          stat_table
         }).

-record(rep_stats, {
          missing_checked = 0,
          missing_found = 0,
          docs_read = 0,
          docs_written = 0,
          doc_write_failures = 0
         }).

-record(rep_state_record, {
          rep,
          starting,
          retries_left,
          max_retries
         }).

-record(rep_state, {
          rep_details,
          source_name,
          target_name,
          source,
          target,
          src_master_db,
          tgt_master_db,
          history,
          checkpoint_history,
          start_seq,
          committed_seq,
          current_through_seq,
          seqs_in_progress = [],
          highest_seq_done = {0, ?LOWEST_SEQ},
          source_log,
          target_log,
          rep_starttime,
          src_starttime,
          tgt_starttime,
          timer, %% checkpoint timer
          changes_queue,
          changes_manager,
          changes_reader,
          workers,
          stats = #rep_stats{},
          session_id,
          source_db_compaction_notifier = nil,
          target_db_compaction_notifier = nil,
          source_monitor = nil,
          target_monitor = nil,
          src_master_db_monitor = nil,
          tgt_master_db_monitor = nil,
          source_seq = nil
         }).

%% Record to store and track changes to the _replicator db
-record(rep_db_state, {
          changes_feed_loop = nil,
          rep_db_name = nil
         }).

-record(batch, {
          docs = [],
          size = 0
         }).

-record(rep_worker_state, {
          cp,
          loop,
          max_parallel_conns,
          source,
          target,
          readers = [],
          writer = nil,
          pending_fetch = nil,
          flush_waiter = nil,
          stats = #rep_stats{},
          source_db_compaction_notifier = nil,
          target_db_compaction_notifier = nil,
          batch = #batch{}
         }).


