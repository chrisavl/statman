%% @doc  Aggregate statman samples
%%
%% statman_aggregator receives metrics from statman_servers running in
%% your cluster, picks them apart and keeps a moving window of the raw
%% values. On demand, the samples are aggregated together.
-module(statman_aggregator).
-behaviour(gen_server).

-export([start_link/0, get_window/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([merge_samples/2]). %% used for histograms in node aggregator

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

get_window(Size) ->
    gen_server:call(?MODULE, {get_window, Size}).

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

handle_call({get_window, Size}, From, #state{metrics = Metrics} = State) ->
    PurgedMetrics = purge(now_to_seconds() - ?MAX_WINDOW, Metrics),
    spawn(fun () ->
                  Merged = merge_window(Size, PurgedMetrics),
                  gen_server:reply(From, {ok, Merged})
          end),
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

insert(Samples, Metrics) ->
    lists:foldl(fun (Sample, Acc) ->
                        {Key, Value} = format(Sample),
                        dict:update(Key, fun (Vs) -> [Value | Vs] end, [Value], Acc)
                end, Metrics, Samples).

merge_window(Size, Metrics) ->
    lists:map(
        fun ({Key, Value}) -> unformat(Key, Value, Size) end,
        dict:to_list(
            dict:map(fun ({_Key, Type, _Node}, Samples) ->
                             merge_samples(Type, window(Size, Samples))
                     end, Metrics))).

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

unformat({Key, Type, Node}, Value, Size) ->
    [{key, Key},
     {type, Type},
     {value, Value},
     {node, Node},
     {window, Size * 1000}].

internal_key(Metric) ->
    {proplists:get_value(key, Metric),
     proplists:get_value(type, Metric),
     proplists:get_value(node, Metric)}.

now_to_seconds() ->
    {MegaSeconds, Seconds, _} = os:timestamp(),
    MegaSeconds * 1000000 + Seconds.


%%
%% TESTS
%%

-ifdef(TEST).
aggregator_test_() ->
    {foreach,
     fun setup/0, fun teardown/1,
     [?_test(expire()),
      ?_test(window())
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
