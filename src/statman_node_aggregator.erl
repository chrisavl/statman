%% @doc  Aggregate statman samples over nodes
%%
%% Given a concated list of multiple time aggregated samples from
%% statman_aggregator aggregates those samples over nodes, so that
%% samples with the same key but from different nodes are merged.
-module(statman_node_aggregator).

-export([aggregate/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


aggregate(Metrics, Size) ->
    lists:map(fun (M) -> unformat(M, Size) end, merge(lists:map(fun format/1, Metrics))).


%%
%% INTERNAL
%%

format(Metric) ->
    {proplists:get_value(node, Metric),
     proplists:get_value(key, Metric),
     proplists:get_value(type, Metric),
     proplists:get_value(value, Metric)}.

unformat({NodeOrNodes, Key, Type, Value}, Size) ->
    [{key, Key},
     {node, NodeOrNodes},
     {type, Type},
     {value, Value},
     {window, Size * 1000}].


merge(Metrics) ->
    {_, Merged} =
        lists:unzip(
          orddict:to_list(
            lists:foldl(
              fun ({_, _, gauge, _}, Acc) ->
                      Acc;

                  ({Node, Key, counter, Sample}, Acc) ->
                      case orddict:find(Key, Acc) of
                          {ok, {Nodes, Key, counter, OtherSample}} ->
                              orddict:store(Key, {[Node | Nodes], Key, counter,
                                                  Sample + OtherSample}, Acc);
                          error ->
                              orddict:store(Key, {[Node], Key, counter, Sample}, Acc)
                      end;

                  ({Node, Key, Type, Samples}, Acc) ->
                      case orddict:find(Key, Acc) of
                          {ok, {Nodes, Key, Type, OtherSamples}} ->
                              Merged = statman_aggregator:merge_samples(
                                           Type, [Samples, OtherSamples]),
                              orddict:store(Key, {[Node | Nodes], Key, Type, Merged}, Acc);
                          error ->
                              orddict:store(Key, {[Node], Key, Type, Samples}, Acc)
                      end
              end, orddict:new(), Metrics))),

    lists:filter(fun ({Nodes, _, _, _}) -> length(Nodes) > 1 end, Merged).



-ifdef(TEST).
aggregate_test() ->
    {ok, P} = statman_aggregator:start_link(),

    gen_server:cast(P, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(P, {statman_update, [sample_histogram('b@knutin')]}),
    gen_server:cast(P, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(P, {statman_update, [sample_counter('b@knutin')]}),

    timer:sleep(1000),

    gen_server:cast(P, {statman_update, [sample_histogram('a@knutin')]}),
    gen_server:cast(P, {statman_update, [sample_histogram('b@knutin')]}),
    gen_server:cast(P, {statman_update, [sample_counter('a@knutin')]}),
    gen_server:cast(P, {statman_update, [sample_counter('b@knutin')]}),

    {ok, TimeAggregated} = statman_aggregator:get_window(60),
    NodeAggregated = aggregate(TimeAggregated, 60),

    [MergedCounter, MergedHistogram] = lists:sort(NodeAggregated),


    ?assertEqual([{key, {<<"/highscores">>,db_a_latency}},
                  {node, ['a@knutin', 'b@knutin']},
                  {type, histogram},
                  {value, [{1, 4}, {2, 8}, {3, 12}]},
                  {window, 60000}], MergedHistogram),

    ?assertEqual([{key, {foo, bar}},
                  {node, ['a@knutin', 'b@knutin']},
                  {type, counter},
                  {value, 120},
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
-endif.
