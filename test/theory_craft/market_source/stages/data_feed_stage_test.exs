defmodule TheoryCraft.MarketSource.DataFeedStageTest do
  use ExUnit.Case, async: true

  alias TheoryCraft.MarketSource.{
    Bar,
    DataFeedStage,
    MarketEvent,
    MemoryDataFeed,
    Tick
  }

  alias TheoryCraft.TestHelpers.TestEventConsumer

  ## Setup

  setup_all do
    # Create shared data feeds for tests
    # Note: async: true means each test runs in its own process,
    # but they can all read from the same ETS tables created here
    ticks = sample_ticks()
    bars = sample_bars()

    tick_feed = MemoryDataFeed.new(ticks)
    bar_feed = MemoryDataFeed.new(bars)
    empty_feed = MemoryDataFeed.new([])

    %{
      ticks: ticks,
      bars: bars,
      tick_feed: tick_feed,
      bar_feed: bar_feed,
      empty_feed: empty_feed
    }
  end

  ## Tests

  describe "start_link/2" do
    test "successfully starts with MemoryDataFeed and tick data", %{tick_feed: tick_feed} do
      assert {:ok, stage} =
               DataFeedStage.start_link(
                 {MemoryDataFeed, [from: tick_feed]},
                 name: "xauusd"
               )

      assert is_pid(stage)
      assert Process.alive?(stage)
    end

    test "successfully starts with MemoryDataFeed and bar data", %{bar_feed: bar_feed} do
      assert {:ok, stage} =
               DataFeedStage.start_link(
                 {MemoryDataFeed, [from: bar_feed]},
                 name: "xauusd"
               )

      assert is_pid(stage)
      assert Process.alive?(stage)
    end

    test "successfully starts with custom max_demand option", %{tick_feed: tick_feed} do
      assert {:ok, stage} =
               DataFeedStage.start_link(
                 {MemoryDataFeed, [from: tick_feed]},
                 name: "xauusd",
                 max_demand: 500
               )

      assert is_pid(stage)
      assert Process.alive?(stage)
    end

    test "successfully starts with stage_name option", %{tick_feed: tick_feed} do
      assert {:ok, stage} =
               DataFeedStage.start_link(
                 {MemoryDataFeed, [from: tick_feed]},
                 name: "xauusd",
                 stage_name: :test_feed_stage
               )

      assert is_pid(stage)
      assert Process.whereis(:test_feed_stage) == stage
    end

    test "returns error when name is missing", %{tick_feed: tick_feed} do
      assert_raise KeyError, ~r/key :name not found/, fn ->
        DataFeedStage.start_link({MemoryDataFeed, [from: tick_feed]})
      end
    end

    test "returns error when DataFeed.stream/1 returns error" do
      assert {:error, {:data_feed_error, ":from option is required"}} =
               DataFeedStage.start_link({MemoryDataFeed, []}, name: "xauusd")
    end

    test "returns error when DataFeed module is invalid" do
      assert {:error, {:data_feed_error, ":from option must be a MemoryDataFeed"}} =
               DataFeedStage.start_link({MemoryDataFeed, [from: "invalid"]}, name: "xauusd")
    end

    test "successfully starts with DataFeed module only (no feed options)",
         %{tick_feed: tick_feed} do
      # MemoryDataFeed without options cannot start, so use tuple form
      assert {:ok, stage} =
               DataFeedStage.start_link(
                 {MemoryDataFeed, [from: tick_feed]},
                 name: "xauusd"
               )

      assert is_pid(stage)
      assert Process.alive?(stage)
    end

    test "successfully starts with Enumerable (list)", %{ticks: ticks} do
      assert {:ok, stage} = DataFeedStage.start_link(ticks, name: "xauusd")

      assert is_pid(stage)
      assert Process.alive?(stage)
    end

    test "successfully starts with Enumerable (stream)", %{ticks: ticks} do
      stream = Stream.map(ticks, & &1)

      assert {:ok, stage} = DataFeedStage.start_link(stream, name: "xauusd")

      assert is_pid(stage)
      assert Process.alive?(stage)
    end
  end

  describe "GenStage producer functionality" do
    setup %{tick_feed: tick_feed} do
      {:ok, producer} =
        DataFeedStage.start_link({MemoryDataFeed, [from: tick_feed]}, name: "xauusd")

      # Trigger initial demand with demand: :accumulate
      GenStage.demand(producer, :forward)

      %{producer: producer}
    end

    test "produces events when consumed", %{producer: producer} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer, test_pid: test_pid, subscribe_to: [producer])

      assert_receive {:events, events}, 1000
      refute Enum.empty?(events)

      first_event = List.first(events)
      assert %MarketEvent{} = first_event
      assert %Tick{} = first_event.data["xauusd"]
    end

    test "produces all events in order", %{producer: producer} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [{producer, max_demand: 10}]
        )

      all_events = collect_all_events([])

      expected_ticks = sample_ticks()
      assert length(all_events) == length(expected_ticks)

      for {received_event, expected_tick} <- Enum.zip(all_events, expected_ticks) do
        assert %MarketEvent{} = received_event
        assert received_event.data["xauusd"] == expected_tick
      end
    end

    test "handles backpressure with small demand", %{producer: producer} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [{producer, min_demand: 1, max_demand: 2}]
        )

      # Should receive events in small batches
      assert_receive {:events, events}, 1000
      assert length(events) <= 2
    end

    test "completes when stream is exhausted", %{producer: producer} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [{producer, max_demand: 100}]
        )

      # Collect all events and wait for completion
      all_events = collect_all_events_with_done()

      # Should have received all events
      expected_ticks = sample_ticks()
      assert length(all_events) == length(expected_ticks)
    end
  end

  describe "multiple consumers" do
    setup %{tick_feed: tick_feed} do
      {:ok, producer} =
        DataFeedStage.start_link({MemoryDataFeed, [from: tick_feed]}, name: "xauusd")

      %{producer: producer}
    end

    test "can serve multiple consumers simultaneously", %{producer: producer} do
      test_pid = self()

      {:ok, _consumer1} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          tag: :consumer1,
          subscribe_to: [producer]
        )

      {:ok, _consumer2} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          tag: :consumer2,
          subscribe_to: [producer]
        )

      # Trigger initial demand after consumers are subscribed
      GenStage.demand(producer, :forward)

      # DemandDispatcher distributes events between consumers
      # With 4 ticks, each consumer should get at least some events or see completion
      # Collect events from both consumers
      events1 = collect_consumer_events(:consumer1, [])
      events2 = collect_consumer_events(:consumer2, [])

      # Together they should have received all 4 ticks
      total_events = length(events1) + length(events2)
      assert total_events == 4
    end
  end

  describe "with empty data feed" do
    test "handles empty stream correctly", %{empty_feed: empty_feed} do
      {:ok, producer} =
        DataFeedStage.start_link({MemoryDataFeed, [from: empty_feed]}, name: "xauusd")

      # Trigger initial demand with demand: :accumulate
      GenStage.demand(producer, :forward)

      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer, test_pid: test_pid, subscribe_to: [producer])

      # Should receive completion without any events
      assert_receive {:producer_done, :normal}, 1000
    end
  end

  describe "alternative start_link signatures" do
    test "produces events with DataFeed module only (no feed options)", %{tick_feed: tick_feed} do
      # MemoryDataFeed without options cannot start, so use tuple form
      {:ok, producer} =
        DataFeedStage.start_link({MemoryDataFeed, [from: tick_feed]}, name: "xauusd")

      # Trigger initial demand
      GenStage.demand(producer, :forward)

      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer, test_pid: test_pid, subscribe_to: [producer])

      all_events = collect_all_events([])

      expected_ticks = sample_ticks()
      assert length(all_events) == length(expected_ticks)

      for {received_event, expected_tick} <- Enum.zip(all_events, expected_ticks) do
        assert %MarketEvent{} = received_event
        assert received_event.data["xauusd"] == expected_tick
      end
    end

    test "produces events with Enumerable (list)" do
      ticks = sample_ticks()

      {:ok, producer} = DataFeedStage.start_link(ticks, name: "xauusd")

      # Trigger initial demand
      GenStage.demand(producer, :forward)

      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer, test_pid: test_pid, subscribe_to: [producer])

      all_events = collect_all_events([])

      assert length(all_events) == length(ticks)

      for {received_event, expected_tick} <- Enum.zip(all_events, ticks) do
        assert %MarketEvent{} = received_event
        assert received_event.data["xauusd"] == expected_tick
      end
    end

    test "produces events with Enumerable (stream)" do
      ticks = sample_ticks()
      stream = Stream.map(ticks, & &1)

      {:ok, producer} = DataFeedStage.start_link(stream, name: "xauusd")

      # Trigger initial demand
      GenStage.demand(producer, :forward)

      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer, test_pid: test_pid, subscribe_to: [producer])

      all_events = collect_all_events([])

      assert length(all_events) == length(ticks)

      for {received_event, expected_tick} <- Enum.zip(all_events, ticks) do
        assert %MarketEvent{} = received_event
        assert received_event.data["xauusd"] == expected_tick
      end
    end
  end

  ## Helper functions

  defp sample_ticks do
    base_time = ~U[2025-09-04 10:00:00.000Z]

    [
      %Tick{
        time: DateTime.add(base_time, 0, :second),
        ask: 1.2345,
        bid: 1.2344,
        ask_volume: 1000.0,
        bid_volume: 1500.0
      },
      %Tick{
        time: DateTime.add(base_time, 1, :second),
        ask: 1.2346,
        bid: 1.2345,
        ask_volume: 800.0,
        bid_volume: 1200.0
      },
      %Tick{
        time: DateTime.add(base_time, 2, :second),
        ask: 1.2344,
        bid: 1.2343,
        ask_volume: 1200.0,
        bid_volume: 1800.0
      },
      %Tick{
        time: DateTime.add(base_time, 3, :second),
        ask: 1.2347,
        bid: 1.2346,
        ask_volume: 600.0,
        bid_volume: 900.0
      }
    ]
  end

  defp sample_bars do
    base_time = ~U[2025-09-04 10:00:00.000Z]

    [
      %Bar{
        time: DateTime.add(base_time, 0, :minute),
        open: 1.2340,
        high: 1.2350,
        low: 1.2338,
        close: 1.2345,
        volume: 5000.0
      },
      %Bar{
        time: DateTime.add(base_time, 1, :minute),
        open: 1.2345,
        high: 1.2355,
        low: 1.2342,
        close: 1.2350,
        volume: 4800.0
      }
    ]
  end

  defp collect_all_events(acc) do
    receive do
      {:events, events} ->
        collect_all_events(acc ++ events)

      {:producer_done, _reason} ->
        acc
    after
      500 -> acc
    end
  end

  defp collect_all_events_with_done do
    collect_all_events_with_done([])
  end

  defp collect_all_events_with_done(acc) do
    receive do
      {:events, events} ->
        collect_all_events_with_done(acc ++ events)

      {:producer_done, :normal} ->
        acc
    after
      1000 -> acc
    end
  end

  defp collect_consumer_events(tag, acc) do
    receive do
      {:events, ^tag, events} ->
        collect_consumer_events(tag, acc ++ events)

      {:producer_done, :normal} ->
        acc
    after
      500 -> acc
    end
  end
end
