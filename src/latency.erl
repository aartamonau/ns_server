-module(latency).

-compile(export_all).

start_link() ->
    {ok, proc_lib:spawn_link(
           fun () ->
                   [spawn_worker(I) || I <- lists:seq(0, 9)],
                   self() ! timeout,
                   sleep()
           end)}.

sleep() ->
    receive
        timeout ->
            erlang:send_after(5000, self(), timeout);
        _ ->
            ok
    end,

    sleep().

spawn_worker(I) ->
    proc_lib:spawn_link(
      fun () ->
              FileName = "/tmp/ram/latency" ++ integer_to_list(I),

              case file:delete(FileName) of
                  ok ->
                      ok;
                  {error, enoent} ->
                      ok
              end,

              P = open_port({spawn, "fallocate -l 104857600 -n " ++ FileName}, [exit_status]),
              receive
                  {P, {exit_status, 0}} ->
                      ok
              after
                  5000 ->
                      exit({could_not_fallocate, FileName})
              end,

              Writer = spawn_writer(FileName),
              worker_loop(Writer, undefined)
      end).

worker_loop(W, undefined) ->
    Now = erlang:now(),
    timer:sleep(1000),
    worker_loop(W, ts(Now));
worker_loop(W, OldTs) ->
    Now = ts(erlang:now()),
    Diff = Now - OldTs,
    W ! {write, <<(i2b(Now))/binary, " ", (i2b(Diff))/binary, "\n">>},
    timer:sleep(1000),
    worker_loop(W, Now).

spawn_writer(FileName) ->
    proc_lib:spawn_link(
      fun () ->
              {ok, F} = file:open(FileName, [raw, binary, append]),
              writer(F)
      end).

writer(F) ->
    receive
        {write, Msg} ->
            ok = file:write(F, Msg),
            writer(F)
    end.

ts({A, B, C}) ->
    A * 1000000000 + B * 1000 + C div 1000.

i2b(I) ->
    list_to_binary(integer_to_list(I)).
