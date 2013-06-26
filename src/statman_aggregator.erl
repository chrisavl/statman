%% @doc  Aggregate statman samples
%%
%% statman_aggregator receives metrics from statman_servers running in
%% your cluster, picks them apart and keeps a moving window of the raw
%% values. On demand, the samples are aggregated together over time or
%% over time and over nodes.
-module(statman_aggregator).
-behaviour(gen_server).

-export([start_link/0, get_window/1, get_merged_window/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          subscribers = [],
          last_sample = [],
          metrics = dict:new()
         }).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MAX_WINDOW, 300). %% 5 min

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc: aggregate samples over time
get_window(Size) ->
    gen_server:call(?MODULE, {get_window, Size, false}).

%% @doc: aggregate samples over time and nodes
get_merged_window(Size) ->
    gen_server:call(?MODULE, {get_window, Size, true}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    timer:send_interval(10000, push),
    {ok, #state{subscribers = [], metrics = dict:new()}}.


handle_call({add_subscriber, Ref}, _From, #state{subscribers = Sub} = State) ->
    {reply, ok, State#state{subscribers = [Ref | Sub]}};
handle_call({remove_subscriber, Ref}, _From, #state{subscribers = Sub} = State) ->
    {reply, ok, State#state{subscribers = lists:delete(Ref, Sub)}};

handle_call({get_window, Size, MergeNodes}, From, #state{metrics = Metrics} = State) ->
    PurgedMetrics = purge(now_to_seconds() - ?MAX_WINDOW, Metrics),
    do_reply(From, Size, PurgedMetrics, MergeNodes),
    {noreply, State#state{metrics = PurgedMetrics}}.


handle_cast({statman_update, NewSamples}, #state{metrics = Metrics} = State) ->
    NewMetrics = insert(NewSamples, Metrics),
    {noreply, State#state{metrics = NewMetrics}}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_reply(From, Size, Metrics, MergeNodes) ->
    spawn(fun () ->
                  Aggregated = aggregate(Size, Metrics),
                  Reply = case MergeNodes of
                              false ->
                                  lists:map(unformater(Size), Aggregated);
                              true ->
                                  lists:map(unformater(Size), Aggregated)
                                      ++ lists:map(unformater(Size), merge_nodes(Aggregated))
                          end,
                  gen_server:reply(From, {ok, Reply})
          end).

insert(Samples, Metrics) ->
    lists:foldl(fun (Sample, Acc) ->
                        {Key, Value} = format(Sample),
                        dict:update(Key, fun (Vs) -> [Value | Vs] end, [Value], Acc)
                end, Metrics, Samples).

aggregate(Size, Metrics) ->
    dict:to_list(
        dict:map(fun ({_Key, Type, _Node}, Samples) ->
                         merge_samples(Type, window(Size, Samples))
                 end, Metrics)).

window(_, []) ->
    [];
window(1, [{_, Sample} | _]) ->
    [Sample];

window(Size, Samples) ->
    element(2, lists:unzip(samples_after(now_to_seconds() - Size, Samples))).

purge(ExpiryTime, Metrics) ->
    dict:filter(
        fun (_Key, Samples) -> Samples =/= [] end,
        dict:map(fun (_Key, Samples) ->
                         samples_after(ExpiryTime, Samples)
                 end, Metrics)).

samples_after(Threshold, Samples) ->
    lists:takewhile(fun ({Ts, _}) -> Ts >= Threshold end, Samples).


merge_samples(histogram, Samples) ->
    lists:foldl(fun (Sample, Agg) ->
                        orddict:merge(fun (_, A, B) ->
                                              A + B
                                      end,
                                      orddict:from_list(Sample),
                                      Agg)
                end, orddict:new(), Samples);

merge_samples(counter, Samples) ->
    lists:sum(Samples);

merge_samples(gauge, []) ->
    0;
merge_samples(gauge, Samples) ->
    hd(Samples).

format(Metric) ->
    {internal_key(Metric),
     {now_to_seconds(), proplists:get_value(value, Metric)}}.

unformat(Key, Type, Node, Value, Size) ->
    [{key, Key},
     {type, Type},
     {value, Value},
     {node, Node},
     {window, Size * 1000}].

unformater(Size) ->
    fun ({{Key, Type, Node}, Value}) -> unformat(Key, Type, Node, Value, Size);
        ({Nodes, Key, Type, Value}) -> unformat(Key, Type, Nodes, Value, Size)
    end.

internal_key(Metric) ->
    {proplists:get_value(key, Metric),
     proplists:get_value(type, Metric),
     proplists:get_value(node, Metric)}.

now_to_seconds() ->
    {MegaSeconds, Seconds, _} = os:timestamp(),
    MegaSeconds * 1000000 + Seconds.

merge_nodes(Aggregated) ->
    {_, Merged} =
        lists:unzip(
          orddict:to_list(
            lists:foldl(
              fun ({{_Key, gauge, _Node}, _Sample}, Acc) ->
                      Acc;

                  ({{Key, counter, Node}, Sample}, Acc) ->
                      case orddict:find(Key, Acc) of
                          {ok, {Nodes, Key, counter, OtherSample}} ->
                              orddict:store(Key, {[Node | Nodes], Key, counter,
                                                  Sample + OtherSample}, Acc);
                          error ->
                              orddict:store(Key, {[Node], Key, counter, Sample}, Acc)
                      end;

                  ({{Key, Type, Node}, Samples}, Acc) ->
                      case orddict:find(Key, Acc) of
                          {ok, {Nodes, Key, Type, OtherSamples}} ->
                              Merged = merge_samples(Type, [Samples, OtherSamples]),
                              orddict:store(Key, {[Node | Nodes], Key, Type, Merged}, Acc);
                          error ->
                              orddict:store(Key, {[Node], Key, Type, Samples}, Acc)
                      end
              end, orddict:new(), Aggregated))),

    lists:filter(fun ({Nodes, _, _, _}) -> length(Nodes) > 1 end, Merged).


%%
%% TESTS
%%

-ifdef(TEST).
aggregator_test_() ->
    {foreach,
     fun setup/0, fun teardown/1,
     [?_test(expire()),
      ?_test(window()),
      ?_test(merged_window())
     ]
    }.

setup() ->
    {ok, Pid} = start_link(),
    true = unlink(Pid),
    Pid.

teardown(Pid) ->
    exit(Pid, kill),
    timer:sleep(1000),
    false = is_process_alive(Pid).

expire() ->
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_gauge('a@knutin', 1)]}),
    gen_server:cast(?MODULE, {statman_update, [sample_gauge('a@knutin', 3)]}),

    ?assert(lists:all(fun (M) ->
                              V = proplists:get_value(value, M, 0),
                              V =/= 0 andalso V =/= []
                      end, element(2, get_window(2)))),

    timer:sleep(3000),

    ?assert(lists:all(fun (M) ->
                              V = proplists:get_value(value, M),
                              V == 0 orelse V =:= []
                      end, element(2, get_window(2)))).

window() ->
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_gauge('a@knutin', 1)]}),
    gen_server:cast(?MODULE, {statman_update, [sample_gauge('a@knutin', 3)]}),
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('b@knutin')]}),

    timer:sleep(1000),

    gen_server:cast(?MODULE, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_gauge('a@knutin', 2)]}),
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('b@knutin')]}),

    {ok, Aggregated} = get_window(60),

    [MergedCounter, MergedGauge, MergedHistogramB, MergedHistogramA] = lists:sort(Aggregated),


    ?assertEqual([{key, {<<"/highscores">>,db_a_latency}},
                  {type, histogram},
                  {value, [{1, 3}, {2, 6}, {3, 9}]},
                  {node, 'a@knutin'},
                  {window, 60000}], MergedHistogramA),

    ?assertEqual([{key, {<<"/highscores">>,db_a_latency}},
                  {type, histogram},
                  {value, [{1, 2}, {2, 4}, {3, 6}]},
                  {node, 'b@knutin'},
                  {window, 60000}], MergedHistogramB),

    ?assertEqual([{key, {foo, bar}},
                  {type, counter},
                  {value, 90},
                  {node, 'a@knutin'},
                  {window, 60000}], MergedCounter),

    ?assertEqual([{key, {foo, bar}},
                  {type, gauge},
                  {value, 2},
                  {node, 'a@knutin'},
                  {window, 60000}], MergedGauge).

merged_window() ->
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('b@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('b@knutin')]}),

    timer:sleep(1000),

    gen_server:cast(?MODULE, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_histogram('b@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(?MODULE, {statman_update, [sample_counter('b@knutin')]}),

    {ok, Aggregated} = statman_aggregator:get_merged_window(60),

    [_CounterA, _CounterB, MergedCounter,
     _HistogramA, _HistogramB, MergedHistogram] = lists:sort(Aggregated),


    ?assertEqual([{key, {<<"/highscores">>,db_a_latency}},
                  {type, histogram},
                  {value, [{1, 4}, {2, 8}, {3, 12}]},
                  {node, ['a@knutin', 'b@knutin']},
                  {window, 60000}], MergedHistogram),

    ?assertEqual([{key, {foo, bar}},
                  {type, counter},
                  {value, 120},
                  {node, ['a@knutin', 'b@knutin']},
                  {window, 60000}], MergedCounter).



sample_histogram(Node) ->
    [{key,{<<"/highscores">>,db_a_latency}},
     {node,Node},
     {type,histogram},
     {value,[{1,1},
             {2,2},
             {3,3}]},
     {window,1000}].

sample_counter(Node) ->
    [{key,{foo, bar}},
     {node,Node},
     {type,counter},
     {value,30},
     {window,1000}].

sample_gauge(Node, Value) ->
    [{key,{foo, bar}},
     {node,Node},
     {type,gauge},
     {value,Value},
     {window,1000}].
-endif.
