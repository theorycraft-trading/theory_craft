defmodule TheoryCraft.MarketSource.BarAggregatorStageTest do
  use ExUnit.Case, async: true

  alias TheoryCraft.MarketSource.{Bar, BarAggregatorStage, MarketEvent}
  alias TheoryCraft.TestHelpers.TestEventProducer

  @moduletag :capture_log

  ## Tests

  describe "start_link/1" do
    test "successfully starts with bar_names and subscribe_to" do
      producer = start_test_producer([])

      assert {:ok, aggregator} =
               BarAggregatorStage.start_link(
                 bar_names: ["m5"],
                 subscribe_to: [producer]
               )

      assert is_pid(aggregator)
      assert Process.alive?(aggregator)
    end

    test "raises error when bar_names is missing" do
      producer = start_test_producer([])
      Process.flag(:trap_exit, true)

      assert {:error, {%KeyError{key: :bar_names}, _stacktrace}} =
               BarAggregatorStage.start_link(subscribe_to: [producer])
    end

    test "raises error when subscribe_to is missing" do
      Process.flag(:trap_exit, true)

      assert {:error, {%KeyError{key: :subscribe_to}, _stacktrace}} =
               BarAggregatorStage.start_link(bar_names: ["m5"])
    end
  end

  describe "single bar aggregation" do
    test "emits previous bar on second new_bar? event" do
      events = [
        build_event("m5", 0, new_bar?: true, new_market?: true, close: 100.0),
        build_event("m5", 5, new_bar?: true, new_market?: false, close: 101.0)
      ]

      result = run_aggregation(events, ["m5"])

      # Emits first bar (when second starts) + second bar (on stream end)
      assert length(result) == 2

      assert %MarketEvent{data: %{"m5" => bar1}} = Enum.at(result, 0)
      assert %Bar{close: 100.0, new_bar?: true, new_market?: true, time: time} = bar1
      assert %DateTime{minute: 0} = time

      assert %MarketEvent{data: %{"m5" => bar2}} = Enum.at(result, 1)
      assert %Bar{close: 101.0, new_bar?: true, new_market?: false, time: time} = bar2
      assert %DateTime{minute: 5} = time
    end

    test "preserves original flags from first tick of bar" do
      events = [
        build_event("m5", 0, new_bar?: true, new_market?: true, close: 100.0),
        build_event("m5", 0, new_bar?: false, new_market?: false, close: 100.5),
        build_event("m5", 0, new_bar?: false, new_market?: false, close: 101.0),
        build_event("m5", 5, new_bar?: true, new_market?: false, close: 102.0)
      ]

      result = run_aggregation(events, ["m5"])

      # Emits first bar with last updated close (101.0) but original flags (true, true)
      # + second bar on stream end
      assert length(result) == 2

      assert %MarketEvent{data: %{"m5" => bar1}} = Enum.at(result, 0)
      assert %Bar{close: 101.0, new_bar?: true, new_market?: true, time: time} = bar1
      assert %DateTime{minute: 0} = time

      assert %MarketEvent{data: %{"m5" => bar2}} = Enum.at(result, 1)
      assert %Bar{close: 102.0, new_bar?: true, new_market?: false, time: time} = bar2
      assert %DateTime{minute: 5} = time
    end

    test "emits updated bar on stream end" do
      events = [
        build_event("m5", 0, new_bar?: true, new_market?: true, close: 100.0),
        build_event("m5", 0, new_bar?: false, new_market?: false, close: 100.5)
      ]

      result = run_aggregation(events, ["m5"])

      # Emits the updated bar when stream terminates with original flags from first tick
      assert length(result) == 1
      assert %MarketEvent{data: %{"m5" => bar}} = hd(result)
      assert %Bar{close: 100.5, new_bar?: true, new_market?: true} = bar
    end

    test "emits multiple completed bars in sequence" do
      events = [
        build_event("m5", 0, new_bar?: true, new_market?: true, close: 100.0),
        build_event("m5", 0, new_bar?: false, new_market?: false, close: 100.5),
        build_event("m5", 5, new_bar?: true, new_market?: false, close: 101.0),
        build_event("m5", 5, new_bar?: false, new_market?: false, close: 101.5),
        build_event("m5", 10, new_bar?: true, new_market?: false, close: 102.0)
      ]

      result = run_aggregation(events, ["m5"])

      # Emits 3 bars: first (when second starts), second (when third starts), third (on stream end)
      # All bars have new_bar?=true (original flags from their first tick)
      assert length(result) == 3

      assert %MarketEvent{data: %{"m5" => bar1}} = Enum.at(result, 0)
      assert %Bar{close: 100.5, new_bar?: true, new_market?: true, time: time} = bar1
      assert %DateTime{minute: 0} = time

      assert %MarketEvent{data: %{"m5" => bar2}} = Enum.at(result, 1)
      assert %Bar{close: 101.5, new_bar?: true, new_market?: false, time: time} = bar2
      assert %DateTime{minute: 5} = time

      assert %MarketEvent{data: %{"m5" => bar3}} = Enum.at(result, 2)
      assert %Bar{close: 102.0, new_bar?: true, new_market?: false, time: time} = bar3
      assert %DateTime{minute: 10} = time
    end

    test "preserves new_market? flag correctly" do
      events = [
        build_event("m5", 0, new_bar?: true, new_market?: true, close: 100.0),
        build_event("m5", 5, new_bar?: true, new_market?: false, close: 101.0),
        build_event("m5", 10, new_bar?: true, new_market?: true, close: 102.0)
      ]

      result = run_aggregation(events, ["m5"])

      # Emits all 3 bars with correct flags
      assert length(result) == 3

      assert %MarketEvent{data: %{"m5" => bar1}} = Enum.at(result, 0)
      assert %Bar{new_market?: true, time: time} = bar1
      assert %DateTime{minute: 0} = time

      assert %MarketEvent{data: %{"m5" => bar2}} = Enum.at(result, 1)
      assert %Bar{new_market?: false, time: time} = bar2
      assert %DateTime{minute: 5} = time

      assert %MarketEvent{data: %{"m5" => bar3}} = Enum.at(result, 2)
      assert %Bar{new_market?: true, time: time} = bar3
      assert %DateTime{minute: 10} = time
    end
  end

  describe "multiple bar aggregation (OR logic)" do
    test "tracks first emission vs updates for each bar (m2/m5 example)" do
      events = [
        # Event 0: time=0, both bars new
        build_multi_event([
          {"m2", 0, new_bar?: true, new_market?: true, close: 100.0},
          {"m5", 0, new_bar?: true, new_market?: true, close: 200.0}
        ]),
        # Event 1: time=1, both bars continue
        build_multi_event([
          {"m2", 0, new_bar?: false, new_market?: false, close: 100.5},
          {"m5", 0, new_bar?: false, new_market?: false, close: 200.5}
        ]),
        # Event 2: time=2, m2 new bar (triggers emission of Event 1)
        build_multi_event([
          {"m2", 2, new_bar?: true, new_market?: false, close: 101.0},
          {"m5", 0, new_bar?: false, new_market?: false, close: 201.0}
        ]),
        # Event 3: time=3, both bars continue
        build_multi_event([
          {"m2", 2, new_bar?: false, new_market?: false, close: 101.5},
          {"m5", 0, new_bar?: false, new_market?: false, close: 201.5}
        ]),
        # Event 4: time=4, m2 new bar (triggers emission of Event 3)
        build_multi_event([
          {"m2", 4, new_bar?: true, new_market?: false, close: 102.0},
          {"m5", 0, new_bar?: false, new_market?: false, close: 202.0}
        ]),
        # Event 5: time=5, m5 new bar (triggers emission of Event 4)
        build_multi_event([
          {"m2", 4, new_bar?: false, new_market?: false, close: 102.5},
          {"m5", 5, new_bar?: true, new_market?: false, close: 203.0}
        ]),
        # Event 6: time=6, m2 new bar (triggers emission of Event 5)
        build_multi_event([
          {"m2", 6, new_bar?: true, new_market?: false, close: 103.0},
          {"m5", 5, new_bar?: false, new_market?: false, close: 203.5}
        ])
      ]

      result = run_aggregation(events, ["m2", "m5"])

      # Should emit: Event 1, Event 3, Event 4, Event 5, and Event 6 (on stream end)
      assert length(result) == 5

      # Emission 1: Event 1 when Event 2 arrives
      # First emission of both bars → both new_bar?=true
      assert %MarketEvent{data: data1} = Enum.at(result, 0)
      assert %Bar{close: 100.5, new_bar?: true, time: m2_time} = data1["m2"]
      assert %DateTime{minute: 0} = m2_time
      assert %Bar{close: 200.5, new_bar?: true, time: m5_time} = data1["m5"]
      assert %DateTime{minute: 0} = m5_time

      # Emission 2: Event 3 when Event 4 arrives
      # First emission of m2 bar2, but m5 bar0 already emitted → m2=true, m5=false
      assert %MarketEvent{data: data2} = Enum.at(result, 1)
      assert %Bar{close: 101.5, new_bar?: true, time: m2_time} = data2["m2"]
      assert %DateTime{minute: 2} = m2_time
      assert %Bar{close: 201.5, new_bar?: false, time: m5_time} = data2["m5"]
      assert %DateTime{minute: 0} = m5_time

      # Emission 3: Event 4 when Event 5 arrives
      # First emission of m2 bar4, but m5 bar0 already emitted → m2=true, m5=false
      assert %MarketEvent{data: data3} = Enum.at(result, 2)
      assert %Bar{close: 102.0, new_bar?: true, time: m2_time} = data3["m2"]
      assert %DateTime{minute: 4} = m2_time
      assert %Bar{close: 202.0, new_bar?: false, time: m5_time} = data3["m5"]
      assert %DateTime{minute: 0} = m5_time

      # Emission 4: Event 5 when Event 6 arrives
      # m2 bar4 already emitted, first emission of m5 bar5 → m2=false, m5=true
      assert %MarketEvent{data: data4} = Enum.at(result, 3)
      assert %Bar{close: 102.5, new_bar?: false, time: m2_time} = data4["m2"]
      assert %DateTime{minute: 4} = m2_time
      assert %Bar{close: 203.0, new_bar?: true, time: m5_time} = data4["m5"]
      assert %DateTime{minute: 5} = m5_time

      # Emission 5: Event 6 on stream end
      # First emission of m2 bar6, but m5 bar5 already emitted → m2=true, m5=false
      assert %MarketEvent{data: data5} = Enum.at(result, 4)
      assert %Bar{close: 103.0, new_bar?: true, time: m2_time} = data5["m2"]
      assert %DateTime{minute: 6} = m2_time
      assert %Bar{close: 203.5, new_bar?: false, time: m5_time} = data5["m5"]
      assert %DateTime{minute: 5} = m5_time
    end

    test "emits when any tracked bar is complete" do
      events = [
        build_multi_event([
          {"m5", 0, new_bar?: true, new_market?: true, close: 100.0},
          {"h1", 0, new_bar?: true, new_market?: true, close: 200.0}
        ]),
        build_multi_event([
          {"m5", 5, new_bar?: true, new_market?: false, close: 101.0},
          {"h1", 0, new_bar?: false, new_market?: false, close: 200.5}
        ])
      ]

      result = run_aggregation(events, ["m5", "h1"])

      # Emits when m5 completes + final event with both bars
      assert length(result) == 2

      # First emission: emits event 1 (pending) when event 2 arrives
      assert %MarketEvent{data: data1} = Enum.at(result, 0)
      assert %Bar{close: 100.0, new_bar?: true} = data1["m5"]
      assert %Bar{close: 200.0, new_bar?: true} = data1["h1"]

      # Final emission: emits event 2 on stream end
      # h1 has new_bar?=false because it was already emitted in data1
      assert %MarketEvent{data: data2} = Enum.at(result, 1)
      assert %Bar{close: 101.0, new_bar?: true} = data2["m5"]
      assert %Bar{close: 200.5, new_bar?: false} = data2["h1"]
    end

    test "emits when second bar completes, even if first is not complete" do
      events = [
        build_multi_event([
          {"m5", 0, new_bar?: true, new_market?: true, close: 100.0},
          {"h1", 0, new_bar?: true, new_market?: true, close: 200.0}
        ]),
        build_multi_event([
          {"m5", 0, new_bar?: false, new_market?: false, close: 100.5},
          {"h1", 60, new_bar?: true, new_market?: false, close: 201.0}
        ])
      ]

      result = run_aggregation(events, ["m5", "h1"])

      # Emits when h1 completes + final event
      assert length(result) == 2

      # Emission 1: emits event 1 when event 2 arrives (1-tick lag)
      assert %MarketEvent{data: data1} = Enum.at(result, 0)
      # From pending event, not updated to 100.5
      assert data1["m5"].close == 100.0
      assert data1["h1"].close == 200.0

      # Emission 2: emits event 2 on stream end
      assert %MarketEvent{data: data2} = Enum.at(result, 1)
      assert data2["m5"].close == 100.5
      assert data2["h1"].close == 201.0
    end
  end

  describe "edge cases" do
    test "raises error when bar_names is empty" do
      producer = start_test_producer([])
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: "bar_names cannot be empty"}, _stacktrace}} =
               BarAggregatorStage.start_link(
                 bar_names: [],
                 subscribe_to: [producer]
               )
    end

    test "does not track bars not in bar_names" do
      events = [
        # Event 0: Both bars start
        build_multi_event([
          {"m5", 0, new_bar?: true, new_market?: true, close: 100.0},
          {"h1", 0, new_bar?: true, new_market?: true, close: 200.0}
        ]),
        # Event 1: Both bars continue (update)
        build_multi_event([
          {"m5", 0, new_bar?: false, new_market?: false, close: 100.5},
          {"h1", 0, new_bar?: false, new_market?: false, close: 200.5}
        ]),
        # Event 2: m5 starts new bar, h1 continues
        build_multi_event([
          {"m5", 5, new_bar?: true, new_market?: false, close: 101.0},
          {"h1", 0, new_bar?: false, new_market?: false, close: 201.0}
        ])
      ]

      result = run_aggregation(events, ["m5"])

      # Emits event 1 when event 2 arrives + event 2 on stream end
      assert length(result) == 2

      # Emission 1: event 1 when event 2 arrives
      # m5 is tracked → flags modified (first emission, so new_bar?=true)
      # h1 is NOT tracked → flags unchanged from event 1 (new_bar?=false)
      assert %MarketEvent{data: data1} = Enum.at(result, 0)
      assert %Bar{close: 100.5, new_bar?: true, new_market?: true} = data1["m5"]
      assert %Bar{close: 200.5, new_bar?: false, new_market?: false} = data1["h1"]

      # Emission 2: event 2 on stream end
      # m5 is tracked → flags modified (first emission of new bar, so new_bar?=true)
      # h1 is NOT tracked → flags unchanged from event 2 (new_bar?=false)
      assert %MarketEvent{data: data2} = Enum.at(result, 1)
      assert %Bar{close: 101.0, new_bar?: true, new_market?: false} = data2["m5"]
      assert %Bar{close: 201.0, new_bar?: false, new_market?: false} = data2["h1"]
    end

    test "handles events with non-bar data" do
      base_time = ~U[2025-09-04 10:00:00.000Z]

      events = [
        %MarketEvent{
          time: base_time,
          source: "test",
          data: %{
            "m5" => build_bar(0, new_bar?: true, new_market?: true, close: 100.0),
            "other_data" => "some_value"
          }
        },
        %MarketEvent{
          time: DateTime.add(base_time, 5, :minute),
          source: "test",
          data: %{
            "m5" => build_bar(5, new_bar?: true, new_market?: false, close: 101.0),
            "other_data" => "another_value"
          }
        }
      ]

      result = run_aggregation(events, ["m5"])

      # Emits first bar + final bar (both preserve other_data)
      assert length(result) == 2

      # Emission 1: emits event 1 when event 2 arrives (1-tick lag)
      assert %MarketEvent{data: data1} = Enum.at(result, 0)
      assert %Bar{close: 100.0} = data1["m5"]
      # From pending event
      assert data1["other_data"] == "some_value"

      # Emission 2: emits event 2 on stream end
      assert %MarketEvent{data: data2} = Enum.at(result, 1)
      assert %Bar{close: 101.0} = data2["m5"]
      assert data2["other_data"] == "another_value"
    end
  end

  ## Private helper functions

  defp build_event(bar_name, offset_minutes, opts) do
    base_time = ~U[2025-09-04 10:00:00.000Z]
    time = DateTime.add(base_time, offset_minutes, :minute)

    %MarketEvent{
      time: time,
      source: "test",
      data: %{bar_name => build_bar(offset_minutes, opts)}
    }
  end

  defp build_multi_event(bar_specs) do
    base_time = ~U[2025-09-04 10:00:00.000Z]

    data =
      Enum.reduce(bar_specs, %{}, fn {bar_name, offset_minutes, opts}, acc ->
        Map.put(acc, bar_name, build_bar(offset_minutes, opts))
      end)

    %MarketEvent{
      time: base_time,
      source: "test",
      data: data
    }
  end

  defp build_bar(offset_minutes, opts) do
    base_time = ~U[2025-09-04 10:00:00.000Z]
    time = DateTime.add(base_time, offset_minutes, :minute)

    %Bar{
      time: time,
      open: Keyword.get(opts, :open, 100.0),
      high: Keyword.get(opts, :high, 105.0),
      low: Keyword.get(opts, :low, 95.0),
      close: Keyword.get(opts, :close, 100.0),
      volume: Keyword.get(opts, :volume, 1000.0),
      new_bar?: Keyword.fetch!(opts, :new_bar?),
      new_market?: Keyword.fetch!(opts, :new_market?)
    }
  end

  defp start_test_producer(events) do
    {:ok, producer} = TestEventProducer.start_link(events: events)
    producer
  end

  defp run_aggregation(events, bar_names) do
    producer = start_test_producer(events)

    {:ok, aggregator} =
      BarAggregatorStage.start_link(
        bar_names: bar_names,
        subscribe_to: [{producer, cancel: :transient}]
      )

    stream = GenStage.stream([{aggregator, cancel: :transient}], producers: [producer])
    Enum.to_list(stream)
  end
end
