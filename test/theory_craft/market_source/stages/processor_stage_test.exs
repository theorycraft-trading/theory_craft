defmodule TheoryCraft.MarketSource.ProcessorStageTest do
  use ExUnit.Case, async: true

  alias TheoryCraft.MarketSource.{
    DataFeedStage,
    MarketEvent,
    MemoryDataFeed,
    ProcessorStage,
    Tick
  }

  alias TheoryCraft.TestHelpers.TestEventConsumer

  ## Mocks

  # Mock processor that sends messages to test process to verify calls
  defmodule MockProcessor do
    @behaviour TheoryCraft.MarketSource.Processor

    @impl true
    def init(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      send(test_pid, {:init_called, opts})

      state = %{test_pid: test_pid, call_count: 0}
      {:ok, state}
    end

    @impl true
    def next(event, state) do
      %{test_pid: test_pid, call_count: call_count} = state
      send(test_pid, {:next_called, event, state})

      # Add a simple output to the event
      updated_event = put_in(event.data["mock_output"], call_count)
      new_state = %{state | call_count: call_count + 1}

      {:ok, updated_event, new_state}
    end
  end

  # Mock processor that fails on init
  defmodule FailingInitProcessor do
    @behaviour TheoryCraft.MarketSource.Processor

    @impl true
    def init(_opts) do
      {:error, :init_failed}
    end

    @impl true
    def next(_event, _state), do: raise("Should not be called")
  end

  # Mock processor that fails on next
  defmodule FailingNextProcessor do
    @behaviour TheoryCraft.MarketSource.Processor

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def next(_event, _state) do
      {:error, :next_failed}
    end
  end

  ## Setup

  setup_all do
    tick_feed = MemoryDataFeed.new(sample_ticks())

    %{tick_feed: tick_feed}
  end

  ## Tests

  describe "start_link/2" do
    test "successfully starts with processor spec", %{tick_feed: tick_feed} do
      producer = start_producer(tick_feed)

      assert {:ok, stage} =
               ProcessorStage.start_link(
                 {MockProcessor, [test_pid: self()]},
                 subscribe_to: [producer]
               )

      assert is_pid(stage)
      assert Process.alive?(stage)

      # Verify init was called
      assert_received {:init_called, _opts}
    end

    test "successfully starts with custom stage name", %{tick_feed: tick_feed} do
      producer = start_producer(tick_feed)

      assert {:ok, stage} =
               ProcessorStage.start_link(
                 {MockProcessor, [test_pid: self()]},
                 subscribe_to: [producer],
                 name: :test_processor_stage
               )

      assert is_pid(stage)
      assert Process.whereis(:test_processor_stage) == stage
    end

    test "successfully starts with module-only spec (no options)", %{tick_feed: tick_feed} do
      producer = start_producer(tick_feed)

      # MockProcessor without test_pid option should fail
      Process.flag(:trap_exit, true)

      {:error, {:processor_init_error, reason}} =
        ProcessorStage.start_link(
          MockProcessor,
          subscribe_to: [producer]
        )

      # Should fail during init due to missing :test_pid option
      assert %KeyError{} = reason
    end

    test "returns error when processor init fails", %{tick_feed: tick_feed} do
      producer = start_producer(tick_feed)

      Process.flag(:trap_exit, true)

      {:error, {:processor_init_error, {:error, :init_failed}}} =
        ProcessorStage.start_link(
          FailingInitProcessor,
          subscribe_to: [producer]
        )
    end
  end

  describe "GenStage producer_consumer functionality" do
    setup %{tick_feed: tick_feed} do
      producer = start_producer(tick_feed)
      GenStage.demand(producer, :forward)

      {:ok, processor} =
        ProcessorStage.start_link(
          {MockProcessor, [test_pid: self()]},
          subscribe_to: [producer]
        )

      %{producer: producer, processor: processor}
    end

    test "calls processor next for each event", %{processor: processor} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [processor]
        )

      # Should receive events and verify next was called
      assert_receive {:events, events}, 100
      refute Enum.empty?(events)

      # Verify next was called for each event
      assert_received {:next_called, _event, _state}
    end

    test "forwards processed events to consumers", %{processor: processor} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [processor]
        )

      assert_receive {:events, events}, 1000
      first_event = List.first(events)

      # Event should have mock_output added by processor
      assert Map.has_key?(first_event.data, "mock_output")
    end

    test "maintains processor state across events", %{processor: processor} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [{processor, max_demand: 10}]
        )

      all_events = collect_all_events([])

      # Should have processed all ticks
      assert length(all_events) == length(sample_ticks())

      # Verify call_count increased for each event
      assert Enum.at(all_events, 0).data["mock_output"] == 0
      assert Enum.at(all_events, 1).data["mock_output"] == 1
      assert Enum.at(all_events, 2).data["mock_output"] == 2
      assert Enum.at(all_events, 3).data["mock_output"] == 3
    end

    test "handles backpressure correctly", %{processor: processor} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [{processor, min_demand: 1, max_demand: 2}]
        )

      # Should receive events in small batches
      assert_receive {:events, events}, 1000
      assert length(events) <= 2
    end

    test "completes when upstream producer completes", %{processor: processor} do
      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [{processor, max_demand: 100}]
        )

      all_events = collect_all_events_with_done()

      # Should have processed all ticks
      assert length(all_events) == length(sample_ticks())
    end
  end

  describe "error handling" do
    test "passes through original event when processor returns error", %{tick_feed: tick_feed} do
      producer = start_producer(tick_feed)
      GenStage.demand(producer, :forward)

      {:ok, processor} =
        ProcessorStage.start_link(
          FailingNextProcessor,
          subscribe_to: [producer]
        )

      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [{processor, max_demand: 10}]
        )

      # Should receive all events despite processor errors
      all_events = collect_all_events([])
      assert length(all_events) == length(sample_ticks())

      # All events should be original MarketEvents (unchanged)
      for event <- all_events do
        assert %MarketEvent{} = event
        assert %Tick{} = event.data["xauusd"]
      end
    end
  end

  describe "multiple consumers" do
    setup %{tick_feed: tick_feed} do
      producer = start_producer(tick_feed)

      {:ok, processor} =
        ProcessorStage.start_link(
          {MockProcessor, [test_pid: self()]},
          subscribe_to: [producer]
        )

      %{producer: producer, processor: processor}
    end

    test "can serve multiple consumers simultaneously", %{
      producer: producer,
      processor: processor
    } do
      test_pid = self()

      {:ok, _consumer1} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          tag: :consumer1,
          subscribe_to: [processor]
        )

      {:ok, _consumer2} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          tag: :consumer2,
          subscribe_to: [processor]
        )

      # Trigger initial demand after consumers are subscribed
      GenStage.demand(producer, :forward)

      # DemandDispatcher distributes events between consumers
      {events1, events2} = collect_tagged_events([], [])

      # Together they should have received all ticks
      total_events = length(events1) + length(events2)
      assert total_events == length(sample_ticks())

      # All events should have mock_output
      all_events = events1 ++ events2

      for event <- all_events do
        assert %MarketEvent{} = event
        assert Map.has_key?(event.data, "mock_output")
      end
    end
  end

  describe "pipeline integration" do
    test "works in full pipeline: DataFeedStage -> ProcessorStage -> Consumer", %{
      tick_feed: tick_feed
    } do
      data_feed = start_producer(tick_feed)
      GenStage.demand(data_feed, :forward)

      {:ok, processor} =
        ProcessorStage.start_link(
          {MockProcessor, [test_pid: self()]},
          subscribe_to: [data_feed]
        )

      test_pid = self()

      {:ok, _consumer} =
        GenStage.start_link(TestEventConsumer,
          test_pid: test_pid,
          subscribe_to: [processor]
        )

      all_events = collect_all_events([])

      assert length(all_events) == length(sample_ticks())

      # All should be properly processed
      for event <- all_events do
        assert %MarketEvent{} = event
        assert Map.has_key?(event.data, "mock_output")
      end
    end
  end

  ## Private functions

  defp sample_ticks do
    base_time = ~U[2025-09-04 10:20:00.000Z]

    [
      build_tick(base_time, 0, 2000.0),
      build_tick(base_time, 30, 2001.0),
      build_tick(base_time, 60, 2002.0),
      build_tick(base_time, 90, 2003.0)
    ]
  end

  defp build_tick(base_time, offset_seconds, base_price) do
    %Tick{
      time: DateTime.add(base_time, offset_seconds, :second),
      bid: base_price,
      ask: base_price + 2.0,
      bid_volume: 100.0,
      ask_volume: 150.0
    }
  end

  defp start_producer(feed, name \\ "xauusd") do
    {:ok, producer} = DataFeedStage.start_link({MemoryDataFeed, [from: feed]}, name: name)
    producer
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

  defp collect_tagged_events(acc1, acc2) do
    receive do
      {:events, :consumer1, events} ->
        collect_tagged_events(acc1 ++ events, acc2)

      {:events, :consumer2, events} ->
        collect_tagged_events(acc1, acc2 ++ events)

      {:producer_done, :normal} ->
        {acc1, acc2}
    after
      500 -> {acc1, acc2}
    end
  end
end
