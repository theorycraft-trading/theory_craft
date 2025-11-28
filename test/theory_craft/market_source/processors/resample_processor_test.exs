defmodule TheoryCraft.MarketSource.ResampleProcessorTest do
  use ExUnit.Case, async: true

  alias TheoryCraft.MarketSource.{Bar, Tick}
  alias TheoryCraft.MarketSource.MarketEvent
  alias TheoryCraft.MarketSource.ResampleProcessor

  ## Tests

  describe "init/1" do
    test "initializes with required options" do
      opts = [data: "xauusd", timeframe: "t5", name: "xauusd"]

      assert {:ok, state} = ResampleProcessor.init(opts)
      assert %ResampleProcessor{} = state
      assert state.data_name == "xauusd"
      assert state.timeframe == {"t", 5}
      assert state.name == "xauusd"
      assert state.price_type == :mid
      assert state.fake_volume? == true
      assert state.market_open == ~T[00:00:00]
      assert state.current_bar == nil
      assert state.tick_counter == nil
    end

    test "initializes with custom name" do
      opts = [data: "xauusd", timeframe: "t5", name: "custom_name"]

      assert {:ok, state} = ResampleProcessor.init(opts)
      assert state.name == "custom_name"
    end

    test "initializes with custom price_type :bid" do
      opts = [data: "xauusd", timeframe: "t5", price_type: :bid, name: "xauusd"]

      assert {:ok, state} = ResampleProcessor.init(opts)
      assert state.price_type == :bid
    end

    test "initializes with custom price_type :ask" do
      opts = [data: "xauusd", timeframe: "t5", price_type: :ask, name: "xauusd"]

      assert {:ok, state} = ResampleProcessor.init(opts)
      assert state.price_type == :ask
    end

    test "initializes with fake_volume? false" do
      opts = [data: "xauusd", timeframe: "t5", fake_volume?: false, name: "xauusd"]

      assert {:ok, state} = ResampleProcessor.init(opts)
      assert state.fake_volume? == false
    end

    test "initializes with custom market_open" do
      opts = [data: "xauusd", timeframe: "t5", market_open: ~T[09:30:00], name: "xauusd"]

      assert {:ok, state} = ResampleProcessor.init(opts)
      assert state.market_open == ~T[09:30:00]
    end

    test "raises error when data option is missing" do
      opts = [timeframe: "t5"]

      assert_raise ArgumentError, ~r/Missing required option: data/, fn ->
        ResampleProcessor.init(opts)
      end
    end

    test "raises error when timeframe option is missing" do
      opts = [data: "xauusd"]

      assert_raise ArgumentError, ~r/Missing required option: timeframe/, fn ->
        ResampleProcessor.init(opts)
      end
    end
  end

  describe "next/2 - first tick (initialization)" do
    test "creates first bar from tick with :mid price" do
      opts = [data: "xauusd", timeframe: "t5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert %Bar{} = bar
      assert bar.time == ~U[2024-01-01 10:00:00Z]
      assert bar.open == 2001.0
      assert bar.high == 2001.0
      assert bar.low == 2001.0
      assert bar.close == 2001.0
      assert bar.volume == 1.0

      assert new_state.tick_counter == 1
      assert new_state.current_bar == bar
    end

    test "creates first bar with :bid price" do
      opts = [data: "xauusd", timeframe: "t5", price_type: :bid, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.open == 2000.0
      assert bar.close == 2000.0
    end

    test "creates first bar with :ask price" do
      opts = [data: "xauusd", timeframe: "t5", price_type: :ask, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.open == 2002.0
      assert bar.close == 2002.0
    end

    test "creates first bar with real volume when available" do
      opts = [data: "xauusd", timeframe: "t5", fake_volume?: false, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick =
        build_tick(~U[2024-01-01 10:00:00Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 10.0,
          ask_volume: 15.0
        )

      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == 25.0
    end

    test "creates first bar with nil volume when fake_volume? is false and no volume" do
      opts = [data: "xauusd", timeframe: "t5", fake_volume?: false, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == nil
    end

    test "creates first bar with fake volume when fake_volume? is true and no volume provided" do
      opts = [data: "xauusd", timeframe: "t5", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == 1.0
    end

    test "creates first bar with real volume when fake_volume? is true and volume is provided" do
      opts = [data: "xauusd", timeframe: "t5", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick =
        build_tick(~U[2024-01-01 10:00:00Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 10.0,
          ask_volume: 15.0
        )

      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == 25.0
    end
  end

  describe "next/2 - updating bar within timeframe" do
    setup do
      opts = [data: "xauusd", timeframe: "t3", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Process first tick
      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      %{state: state}
    end

    test "updates bar with second tick", %{state: state} do
      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.time == ~U[2024-01-01 10:00:00Z]
      assert bar.open == 2001.0
      assert bar.high == 2004.0
      assert bar.low == 2001.0
      assert bar.close == 2004.0
      assert bar.volume == 2.0

      assert new_state.tick_counter == 2
    end

    test "updates high price correctly", %{state: state} do
      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2010.0, ask: 2012.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.high == 2011.0
    end

    test "updates low price correctly", %{state: state} do
      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 1990.0, ask: 1992.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.low == 1991.0
    end

    test "accumulates volume when fake_volume? is false" do
      opts = [data: "xauusd", timeframe: "t3", fake_volume?: false, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 =
        build_tick(
          ~U[2024-01-01 10:00:00Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 10.0,
          ask_volume: 15.0
        )

      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 =
        build_tick(~U[2024-01-01 10:00:01Z],
          bid: 2003.0,
          ask: 2005.0,
          bid_volume: 5.0,
          ask_volume: 8.0
        )

      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == 38.0
    end

    test "accumulates real volume when fake_volume? is true and volume is provided" do
      opts = [data: "xauusd", timeframe: "t3", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 =
        build_tick(~U[2024-01-01 10:00:00Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 10.0,
          ask_volume: 15.0
        )

      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 =
        build_tick(~U[2024-01-01 10:00:01Z],
          bid: 2003.0,
          ask: 2005.0,
          bid_volume: 5.0,
          ask_volume: 8.0
        )

      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == 38.0
    end

    test "increments by 1.0 when fake_volume? is true and no volume provided" do
      opts = [data: "xauusd", timeframe: "t3", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == 2.0
    end

    test "mixes fake and real volume when fake_volume? is true" do
      opts = [data: "xauusd", timeframe: "t3", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick without volume -> fake volume = 1.0
      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      # Second tick with real volume -> 1.0 + 20.0 = 21.0
      tick2 =
        build_tick(~U[2024-01-01 10:00:01Z],
          bid: 2003.0,
          ask: 2005.0,
          bid_volume: 8.0,
          ask_volume: 12.0
        )

      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      assert %MarketEvent{data: %{"xauusd" => bar}} = new_event
      assert bar.volume == 21.0
    end
  end

  describe "next/2 - creating new bar when counter reaches multiplier" do
    test "creates new bar after multiplier ticks" do
      opts = [data: "xauusd", timeframe: "t3", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      ticks = build_tick_sequence(4)

      # Process first 3 ticks
      {event, state} = process_ticks(state, Enum.take(ticks, 3))
      bar = event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:00:00Z]

      # 4th tick should create new bar
      tick4 = Enum.at(ticks, 3)
      event4 = %MarketEvent{data: %{"xauusd" => tick4}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event4, state)

      new_bar = new_event.data["xauusd"]
      assert new_bar.time == ~U[2024-01-01 10:00:03Z]
      assert new_bar.open == new_bar.close
      assert new_state.tick_counter == 1
    end
  end

  describe "next/2 - market_open transition" do
    test "creates new bar when crossing market_open time" do
      opts = [data: "xauusd", timeframe: "t5", market_open: ~T[10:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick before market open (counter = 1)
      tick1 = build_tick(~U[2024-01-01 09:59:59Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      # Second tick still before market open (counter = 2)
      tick2 = build_tick(~U[2024-01-01 09:59:59Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, event2_result, state} = ResampleProcessor.next(event2, state)

      assert state.tick_counter == 2
      bar_before = event2_result.data["xauusd"]
      assert bar_before.time == ~U[2024-01-01 09:59:59Z]

      # Third tick at or after market open should create new bar
      tick3 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2005.0, ask: 2007.0)
      event3 = %MarketEvent{data: %{"xauusd" => tick3}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event3, state)

      new_bar = new_event.data["xauusd"]
      assert new_bar.time == ~U[2024-01-01 10:00:00Z]
      assert new_state.tick_counter == 1

      # Fourth tick at same time should update the bar
      tick4 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2006.0, ask: 2008.0)
      event4 = %MarketEvent{data: %{"xauusd" => tick4}}
      {:ok, event4_result, state4} = ResampleProcessor.next(event4, new_state)

      bar4 = event4_result.data["xauusd"]
      assert bar4.time == ~U[2024-01-01 10:00:00Z]
      assert state4.tick_counter == 2
      assert bar4.close == 2007.0
    end
  end

  describe "next/2 - edge cases" do
    test "handles tick with only bid price" do
      opts = [data: "xauusd", timeframe: "t5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = %Tick{
        time: ~U[2024-01-01 10:00:00Z],
        bid: 2000.0,
        ask: nil,
        bid_volume: 10.0,
        ask_volume: nil
      }

      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2000.0
    end

    test "handles tick with only ask price" do
      opts = [data: "xauusd", timeframe: "t5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = %Tick{
        time: ~U[2024-01-01 10:00:00Z],
        bid: nil,
        ask: 2002.0,
        bid_volume: nil,
        ask_volume: 15.0
      }

      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2002.0
    end

    test "raises error when both bid and ask are nil" do
      opts = [data: "xauusd", timeframe: "t5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = %Tick{
        time: ~U[2024-01-01 10:00:00Z],
        bid: nil,
        ask: nil,
        bid_volume: nil,
        ask_volume: nil
      }

      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert_raise RuntimeError, "Both ask and bid can't be nil", fn ->
        ResampleProcessor.next(event, state)
      end
    end

    test "handles mixed volume availability" do
      opts = [data: "xauusd", timeframe: "t2", fake_volume?: false, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick with volume
      tick1 =
        build_tick(~U[2024-01-01 10:00:00Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 10.0,
          ask_volume: 15.0
        )

      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      # Second tick without volume
      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      # Should keep the previous volume when new tick has no volume
      assert bar.volume == 25.0
    end
  end

  describe "next/2 - monthly timeframes" do
    test "creates first bar aligned on first of month" do
      opts = [data: "xauusd", timeframe: "M", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Tick on Jan 17 should align to Jan 1
      tick = build_tick(~U[2024-01-17 15:45:30Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 09:30:00Z]
      assert bar.open == 2001.0
      assert new_state.next_time == ~U[2024-02-01 09:30:00Z]
    end

    test "updates bar within same month" do
      opts = [data: "xauusd", timeframe: "M", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-05 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-25 18:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 00:00:00Z]
      assert bar.open == 2001.0
      assert bar.close == 2004.0
      assert new_state.next_time == ~U[2024-02-01 00:00:00Z]
    end

    test "creates new bar on next month" do
      opts = [data: "xauusd", timeframe: "M", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-20 15:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-02-10 12:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-02-01 09:30:00Z]
      assert bar.open == 2004.0
      assert new_state.next_time == ~U[2024-03-01 09:30:00Z]
    end

    test "handles M with multiplier > 1" do
      opts = [data: "xauusd", timeframe: "M3", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-17 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 00:00:00Z]
      assert new_state.next_time == ~U[2024-04-01 00:00:00Z]
    end

    test "handles month overflow (Jan 31 to Feb)" do
      opts = [data: "xauusd", timeframe: "M", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-31 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-02-15 10:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-02-01 00:00:00Z]
      # Feb has 29 days in 2024 (leap year), but we start on 1st
      assert new_state.next_time == ~U[2024-03-01 00:00:00Z]
    end

    test "accumulates volume with fake_volume?" do
      opts = [data: "xauusd", timeframe: "M", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-10 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-20 15:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 2.0
    end

    test "handles different price types" do
      opts = [data: "xauusd", timeframe: "M", price_type: :bid, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-17 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2000.0
    end
  end

  describe "next/2 - weekly timeframes" do
    test "creates first bar aligned on Monday market_open by default" do
      opts = [data: "xauusd", timeframe: "W", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # 2024-01-17 is a Wednesday, should align to Monday 2024-01-15
      tick = build_tick(~U[2024-01-17 15:45:30Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-15 09:30:00Z]
      assert bar.open == 2001.0
      assert new_state.next_time == ~U[2024-01-22 09:30:00Z]
    end

    test "creates first bar aligned on Sunday when weekly_open: :sunday" do
      opts = [
        data: "xauusd",
        timeframe: "W",
        weekly_open: :sunday,
        market_open: ~T[00:00:00],
        name: "xauusd"
      ]

      {:ok, state} = ResampleProcessor.init(opts)

      # 2024-01-17 is a Wednesday, should align to Sunday 2024-01-14
      tick = build_tick(~U[2024-01-17 15:45:30Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-14 00:00:00Z]
      assert bar.open == 2001.0
      assert new_state.next_time == ~U[2024-01-21 00:00:00Z]
    end

    test "updates bar within same week" do
      opts = [data: "xauusd", timeframe: "W", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-15 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-19 18:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-15 00:00:00Z]
      assert bar.open == 2001.0
      assert bar.close == 2004.0
      assert new_state.next_time == ~U[2024-01-22 00:00:00Z]
    end

    test "creates new bar on next week" do
      opts = [data: "xauusd", timeframe: "W", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-17 15:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-23 12:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-22 09:30:00Z]
      assert bar.open == 2004.0
      assert new_state.next_time == ~U[2024-01-29 09:30:00Z]
    end

    test "handles W with multiplier > 1" do
      opts = [data: "xauusd", timeframe: "W2", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-17 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-15 00:00:00Z]
      assert new_state.next_time == ~U[2024-01-29 00:00:00Z]
    end

    test "accumulates volume with fake_volume?" do
      opts = [data: "xauusd", timeframe: "W", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-15 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-18 15:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 2.0
    end

    test "handles different price types" do
      opts = [data: "xauusd", timeframe: "W", price_type: :ask, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-17 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2002.0
    end
  end

  describe "next/2 - daily timeframes" do
    test "creates first bar aligned on market_open" do
      opts = [data: "xauusd", timeframe: "D", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Tick at 15:45 should align to 09:30:00 same day
      tick = build_tick(~U[2024-01-15 15:45:30Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-15 09:30:00Z]
      assert bar.open == 2001.0
      assert new_state.next_time == ~U[2024-01-16 09:30:00Z]
    end

    test "updates bar within same day" do
      opts = [data: "xauusd", timeframe: "D", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-15 10:30:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-15 18:45:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-15 00:00:00Z]
      assert bar.open == 2001.0
      assert bar.close == 2004.0
      assert bar.high == 2004.0
      assert new_state.next_time == ~U[2024-01-16 00:00:00Z]
    end

    test "creates new bar on next day" do
      opts = [data: "xauusd", timeframe: "D", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-15 15:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-16 12:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-16 09:30:00Z]
      assert bar.open == 2004.0
      assert new_state.next_time == ~U[2024-01-17 09:30:00Z]
    end

    test "handles D with multiplier > 1" do
      opts = [data: "xauusd", timeframe: "D3", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-15 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-15 00:00:00Z]
      assert new_state.next_time == ~U[2024-01-18 00:00:00Z]
    end

    test "accumulates volume with fake_volume?" do
      opts = [data: "xauusd", timeframe: "D", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-15 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-15 15:00:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 2.0
    end

    test "handles different price types" do
      opts = [data: "xauusd", timeframe: "D", price_type: :bid, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-15 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2000.0
    end
  end

  describe "next/2 - hour-based timeframes" do
    test "creates first bar with aligned time" do
      opts = [data: "xauusd", timeframe: "h4", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Tick at 11:23:45 should align to 08:00:00 for h4
      tick = build_tick(~U[2024-01-01 11:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 08:00:00Z]
      assert bar.open == 2001.0
      assert new_state.next_time == ~U[2024-01-01 12:00:00Z]
    end

    test "updates bar within same timeframe period" do
      opts = [data: "xauusd", timeframe: "h1", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:17:30Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:45:15Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:00:00Z]
      assert bar.open == 2001.0
      assert bar.close == 2004.0
      assert bar.high == 2004.0
      assert new_state.next_time == ~U[2024-01-01 11:00:00Z]
    end

    test "creates new bar when crossing timeframe boundary" do
      opts = [data: "xauusd", timeframe: "h4", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:30:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 14:15:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 12:00:00Z]
      assert bar.open == 2004.0
      assert bar.close == 2004.0
      assert new_state.next_time == ~U[2024-01-01 16:00:00Z]
    end

    test "handles h1 (1 hour) timeframe" do
      opts = [data: "xauusd", timeframe: "h1", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:00:00Z]
      assert new_state.next_time == ~U[2024-01-01 11:00:00Z]
    end

    test "accumulates volume with fake_volume?" do
      opts = [data: "xauusd", timeframe: "h1", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:12:30Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:45:00Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 2.0
    end

    test "accumulates real volume when provided" do
      opts = [data: "xauusd", timeframe: "h1", fake_volume?: false, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 =
        build_tick(~U[2024-01-01 10:12:30Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 20.0,
          ask_volume: 30.0
        )

      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 =
        build_tick(~U[2024-01-01 10:45:00Z],
          bid: 2003.0,
          ask: 2005.0,
          bid_volume: 15.0,
          ask_volume: 25.0
        )

      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 90.0
    end

    test "handles :bid price type" do
      opts = [data: "xauusd", timeframe: "h1", price_type: :bid, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2000.0
    end

    test "handles :ask price type" do
      opts = [data: "xauusd", timeframe: "h1", price_type: :ask, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2002.0
    end
  end

  describe "next/2 - minute-based timeframes" do
    test "creates first bar with aligned time" do
      opts = [data: "xauusd", timeframe: "m5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Tick at 10:23:45 should align to 10:20:00 for m5
      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:20:00Z]
      assert bar.open == 2001.0
      assert new_state.next_time == ~U[2024-01-01 10:25:00Z]
    end

    test "updates bar within same timeframe period" do
      opts = [data: "xauusd", timeframe: "m15", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:17:30Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:22:15Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:15:00Z]
      assert bar.open == 2001.0
      assert bar.close == 2004.0
      assert bar.high == 2004.0
      assert new_state.next_time == ~U[2024-01-01 10:30:00Z]
    end

    test "creates new bar when crossing timeframe boundary" do
      opts = [data: "xauusd", timeframe: "m5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:22:30Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:27:15Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:25:00Z]
      assert bar.open == 2004.0
      assert bar.close == 2004.0
      assert new_state.next_time == ~U[2024-01-01 10:30:00Z]
    end

    test "handles m1 (1 minute) timeframe" do
      opts = [data: "xauusd", timeframe: "m1", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:23:00Z]
      assert new_state.next_time == ~U[2024-01-01 10:24:00Z]
    end

    test "accumulates volume with fake_volume?" do
      opts = [data: "xauusd", timeframe: "m5", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:12:30Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:14:45Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 2.0
    end

    test "accumulates real volume when provided" do
      opts = [data: "xauusd", timeframe: "m5", fake_volume?: false, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 =
        build_tick(~U[2024-01-01 10:12:30Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 100.0,
          ask_volume: 150.0
        )

      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 =
        build_tick(~U[2024-01-01 10:14:45Z],
          bid: 2003.0,
          ask: 2005.0,
          bid_volume: 50.0,
          ask_volume: 75.0
        )

      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 375.0
    end

    test "handles :bid price type" do
      opts = [data: "xauusd", timeframe: "m5", price_type: :bid, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2000.0
    end

    test "handles :ask price type" do
      opts = [data: "xauusd", timeframe: "m5", price_type: :ask, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2002.0
    end
  end

  describe "next/2 - second-based timeframes" do
    test "creates first bar with aligned time" do
      opts = [data: "xauusd", timeframe: "s5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Tick at 10:00:23 should align to 10:00:20 for s5
      tick = build_tick(~U[2024-01-01 10:00:23Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:00:20Z]
      assert bar.open == 2001.0
      assert new_state.next_time == ~U[2024-01-01 10:00:25Z]
    end

    test "updates bar within same timeframe period" do
      opts = [data: "xauusd", timeframe: "s10", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:00:12Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:00:15Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:00:10Z]
      assert bar.open == 2001.0
      assert bar.close == 2004.0
      assert bar.high == 2004.0
      assert new_state.next_time == ~U[2024-01-01 10:00:20Z]
    end

    test "creates new bar when crossing timeframe boundary" do
      opts = [data: "xauusd", timeframe: "s5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:00:22Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:00:27Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.time == ~U[2024-01-01 10:00:25Z]
      assert bar.open == 2004.0
      assert bar.close == 2004.0
      assert new_state.next_time == ~U[2024-01-01 10:00:30Z]
    end

    test "handles :bid price type" do
      opts = [data: "xauusd", timeframe: "s15", price_type: :bid, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:23Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2000.0
    end

    test "handles :ask price type" do
      opts = [data: "xauusd", timeframe: "s30", price_type: :ask, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:23Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      assert bar.open == 2002.0
    end

    test "accumulates volume with fake_volume? true" do
      opts = [data: "xauusd", timeframe: "s5", fake_volume?: true, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:00:12Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:00:14Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 2.0
    end

    test "accumulates real volume when provided" do
      opts = [data: "xauusd", timeframe: "s5", fake_volume?: false, name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 =
        build_tick(~U[2024-01-01 10:00:12Z],
          bid: 2000.0,
          ask: 2002.0,
          bid_volume: 10.0,
          ask_volume: 15.0
        )

      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      tick2 =
        build_tick(~U[2024-01-01 10:00:14Z],
          bid: 2003.0,
          ask: 2005.0,
          bid_volume: 5.0,
          ask_volume: 8.0
        )

      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      assert bar.volume == 38.0
    end
  end

  describe "next/2 - different timezones support" do
    test "handles daily timeframe with Europe/Paris timezone" do
      opts = [data: "xauusd", timeframe: "D", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Create a tick in Europe/Paris timezone
      {:ok, paris_time} = DateTime.new(~D[2024-01-15], ~T[15:45:30], "Europe/Paris")
      tick = build_tick(paris_time, bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      # Should align to market_open on the same day in Europe/Paris
      assert bar.time.time_zone == "Europe/Paris"
      assert DateTime.to_date(bar.time) == ~D[2024-01-15]
      assert DateTime.to_time(bar.time) == ~T[09:30:00]
      assert bar.open == 2001.0

      # Next time should also be in Europe/Paris
      assert new_state.next_time.time_zone == "Europe/Paris"
      assert DateTime.to_date(new_state.next_time) == ~D[2024-01-16]
      assert DateTime.to_time(new_state.next_time) == ~T[09:30:00]
    end

    test "handles weekly timeframe with America/New_York timezone" do
      opts = [data: "xauusd", timeframe: "W", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # 2024-01-17 is a Wednesday, should align to Monday 2024-01-15
      {:ok, ny_time} = DateTime.new(~D[2024-01-17], ~T[15:45:30], "America/New_York")
      tick = build_tick(ny_time, bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      # Should align to Monday market_open in America/New_York
      assert bar.time.time_zone == "America/New_York"
      assert DateTime.to_date(bar.time) == ~D[2024-01-15]
      assert DateTime.to_time(bar.time) == ~T[09:30:00]
      assert bar.open == 2001.0

      # Next time should be next Monday in America/New_York
      assert new_state.next_time.time_zone == "America/New_York"
      assert DateTime.to_date(new_state.next_time) == ~D[2024-01-22]
      assert DateTime.to_time(new_state.next_time) == ~T[09:30:00]
    end

    test "handles monthly timeframe with Europe/Paris timezone" do
      opts = [data: "xauusd", timeframe: "M", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # Tick on Jan 17 should align to Jan 1 in Europe/Paris
      {:ok, paris_time} = DateTime.new(~D[2024-01-17], ~T[15:45:30], "Europe/Paris")
      tick = build_tick(paris_time, bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      bar = new_event.data["xauusd"]
      # Should align to first of month + market_open in Europe/Paris
      assert bar.time.time_zone == "Europe/Paris"
      assert DateTime.to_date(bar.time) == ~D[2024-01-01]
      assert DateTime.to_time(bar.time) == ~T[09:30:00]
      assert bar.open == 2001.0

      # Next time should be next month in Europe/Paris
      assert new_state.next_time.time_zone == "Europe/Paris"
      assert DateTime.to_date(new_state.next_time) == ~D[2024-02-01]
      assert DateTime.to_time(new_state.next_time) == ~T[09:30:00]
    end

    test "handles crossing month boundary with America/New_York timezone" do
      opts = [data: "xauusd", timeframe: "M", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick in January
      {:ok, ny_time1} = DateTime.new(~D[2024-01-20], ~T[15:00:00], "America/New_York")
      tick1 = build_tick(ny_time1, bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      # Second tick in February
      {:ok, ny_time2} = DateTime.new(~D[2024-02-10], ~T[12:00:00], "America/New_York")
      tick2 = build_tick(ny_time2, bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      # New bar should be in February in America/New_York timezone
      assert bar.time.time_zone == "America/New_York"
      assert DateTime.to_date(bar.time) == ~D[2024-02-01]
      assert DateTime.to_time(bar.time) == ~T[00:00:00]
      assert bar.open == 2004.0

      # Next time should be March in America/New_York
      assert new_state.next_time.time_zone == "America/New_York"
      assert DateTime.to_date(new_state.next_time) == ~D[2024-03-01]
    end

    test "preserves timezone across bar updates" do
      opts = [data: "xauusd", timeframe: "D", market_open: ~T[00:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick creates bar
      {:ok, paris_time1} = DateTime.new(~D[2024-01-15], ~T[10:30:00], "Europe/Paris")
      tick1 = build_tick(paris_time1, bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      # Second tick updates same bar
      {:ok, paris_time2} = DateTime.new(~D[2024-01-15], ~T[18:45:00], "Europe/Paris")
      tick2 = build_tick(paris_time2, bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      # Bar time should still be in Europe/Paris
      assert bar.time.time_zone == "Europe/Paris"
      assert DateTime.to_date(bar.time) == ~D[2024-01-15]
      assert bar.open == 2001.0
      assert bar.close == 2004.0

      # Next time should still be in Europe/Paris
      assert new_state.next_time.time_zone == "Europe/Paris"
      assert DateTime.to_date(new_state.next_time) == ~D[2024-01-16]
    end

    test "handles DST transitions correctly" do
      opts = [data: "xauusd", timeframe: "D", market_open: ~T[09:30:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # March 31, 2024 is DST transition in Europe/Paris (UTC+1 -> UTC+2)
      {:ok, before_dst} = DateTime.new(~D[2024-03-31], ~T[15:00:00], "Europe/Paris")
      tick1 = build_tick(before_dst, bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _event, state} = ResampleProcessor.next(event1, state)

      # April 1, 2024 is after DST transition
      {:ok, after_dst} = DateTime.new(~D[2024-04-01], ~T[12:00:00], "Europe/Paris")
      tick2 = build_tick(after_dst, bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event2, state)

      bar = new_event.data["xauusd"]
      # Should handle DST correctly, timezone preserved
      assert bar.time.time_zone == "Europe/Paris"
      assert DateTime.to_date(bar.time) == ~D[2024-04-01]
      assert DateTime.to_time(bar.time) == ~T[09:30:00]

      assert new_state.next_time.time_zone == "Europe/Paris"
      assert DateTime.to_date(new_state.next_time) == ~D[2024-04-02]
    end
  end

  describe "next/2 - output name vs input data" do
    test "writes bars to 'name' key, not 'data' key (tick timeframe)" do
      opts = [data: "xauusd_ticks", timeframe: "t3", name: "xauusd_t3"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd_ticks" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      # Input ticks should still be present
      assert %Tick{} = new_event.data["xauusd_ticks"]

      # Output bars should be in the name key
      assert %Bar{} = new_event.data["xauusd_t3"]
      assert new_event.data["xauusd_t3"].open == 2001.0
    end

    test "writes bars to 'name' key, not 'data' key (minute timeframe)" do
      opts = [data: "eurusd_ticks", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 1.0850, ask: 1.0852)
      event = %MarketEvent{data: %{"eurusd_ticks" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      # Input ticks should still be present
      assert %Tick{} = new_event.data["eurusd_ticks"]

      # Output bars should be in the name key
      assert %Bar{} = new_event.data["eurusd_m5"]
      assert new_event.data["eurusd_m5"].time == ~U[2024-01-01 10:20:00Z]
      assert new_event.data["eurusd_m5"].open == 1.0851
    end

    test "uses default name when not specified" do
      opts = [data: "btcusd", timeframe: "h1"]
      {:ok, state} = ResampleProcessor.init(opts)

      assert state.name == "btcusd_h1"

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 50_000.0, ask: 50_010.0)
      event = %MarketEvent{data: %{"btcusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      # Input ticks should still be present
      assert %Tick{} = new_event.data["btcusd"]

      # Output bars should be in the default name key
      assert %Bar{} = new_event.data["btcusd_h1"]
    end

    test "preserves both tick and bar data across updates" do
      opts = [data: "gold_ticks", timeframe: "t2", name: "gold_t2"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick
      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"gold_ticks" => tick1}}
      {:ok, event_after_first, state} = ResampleProcessor.next(event1, state)

      # Both should be present
      assert %Tick{} = event_after_first.data["gold_ticks"]
      assert %Bar{} = event_after_first.data["gold_t2"]

      # Second tick (updates bar)
      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2003.0, ask: 2005.0)
      event2 = %MarketEvent{data: %{"gold_ticks" => tick2}}
      {:ok, event_after_second, _state} = ResampleProcessor.next(event2, state)

      # Both should still be present
      assert %Tick{} = event_after_second.data["gold_ticks"]
      assert event_after_second.data["gold_ticks"].bid == 2003.0

      assert %Bar{} = event_after_second.data["gold_t2"]
      assert event_after_second.data["gold_t2"].close == 2004.0
    end

    test "works with daily timeframe" do
      opts = [data: "spy_ticks", timeframe: "D", name: "spy_daily", market_open: ~T[09:30:00]]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-15 15:45:30Z], bid: 480.0, ask: 480.5)
      event = %MarketEvent{data: %{"spy_ticks" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      # Both should be present
      assert %Tick{} = new_event.data["spy_ticks"]
      assert %Bar{} = new_event.data["spy_daily"]
      assert new_event.data["spy_daily"].time == ~U[2024-01-15 09:30:00Z]
    end

    test "allows same value for data and name (overwrites input)" do
      opts = [data: "xauusd", timeframe: "m5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:23:45Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      # Should have overwritten the tick with the bar
      assert %Bar{} = new_event.data["xauusd"]
      refute match?(%Tick{}, new_event.data["xauusd"])
    end
  end

  describe "bar metadata" do
    test "new_bar? is true for first tick" do
      opts = [data: "xauusd", timeframe: "t5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)
      assert %Bar{new_bar?: true} = new_event.data["xauusd"]
    end

    test "new_bar? is false when updating same bar" do
      opts = [data: "xauusd", timeframe: "t5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _new_event1, state} = ResampleProcessor.next(event1, state)

      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event2, _new_state} = ResampleProcessor.next(event2, state)

      assert %Bar{new_bar?: false} = new_event2.data["xauusd"]
    end

    test "new_bar? is true when creating new bar (tick-based)" do
      opts = [data: "xauusd", timeframe: "t2", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First bar
      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _new_event1, state} = ResampleProcessor.next(event1, state)

      # Second tick updates same bar
      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event2, state} = ResampleProcessor.next(event2, state)
      assert %Bar{new_bar?: false} = new_event2.data["xauusd"]

      # Third tick creates new bar (counter reached)
      tick3 = build_tick(~U[2024-01-01 10:00:02Z], bid: 2002.0, ask: 2004.0)
      event3 = %MarketEvent{data: %{"xauusd" => tick3}}
      {:ok, new_event3, _new_state} = ResampleProcessor.next(event3, state)
      assert %Bar{new_bar?: true} = new_event3.data["xauusd"]
    end

    test "new_bar? is true when creating new bar (time-based)" do
      opts = [data: "xauusd", timeframe: "m5", name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First bar at 10:00
      tick1 = build_tick(~U[2024-01-01 10:02:30Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, new_event1, state} = ResampleProcessor.next(event1, state)
      assert %Bar{new_bar?: true, time: ~U[2024-01-01 10:00:00Z]} = new_event1.data["xauusd"]

      # Second tick updates same bar
      tick2 = build_tick(~U[2024-01-01 10:03:00Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event2, state} = ResampleProcessor.next(event2, state)
      assert %Bar{new_bar?: false} = new_event2.data["xauusd"]

      # Third tick creates new bar at 10:05
      tick3 = build_tick(~U[2024-01-01 10:05:00Z], bid: 2002.0, ask: 2004.0)
      event3 = %MarketEvent{data: %{"xauusd" => tick3}}
      {:ok, new_event3, _new_state} = ResampleProcessor.next(event3, state)
      assert %Bar{new_bar?: true, time: ~U[2024-01-01 10:05:00Z]} = new_event3.data["xauusd"]
    end

    test "new_market? is false for first tick" do
      opts = [data: "xauusd", timeframe: "t5", market_open: ~T[09:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      tick = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event = %MarketEvent{data: %{"xauusd" => tick}}

      assert {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)
      assert %Bar{new_market?: false} = new_event.data["xauusd"]
    end

    test "new_market? is true when crossing market_open boundary (tick-based)" do
      opts = [data: "xauusd", timeframe: "t5", market_open: ~T[09:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick before market_open
      tick1 = build_tick(~U[2024-01-01 08:59:55Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _new_event1, state} = ResampleProcessor.next(event1, state)

      # Second tick still before market_open
      tick2 = build_tick(~U[2024-01-01 08:59:57Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event2, state} = ResampleProcessor.next(event2, state)
      assert %Bar{new_market?: false} = new_event2.data["xauusd"]

      # Third tick crosses market_open boundary
      tick3 = build_tick(~U[2024-01-01 09:00:05Z], bid: 2002.0, ask: 2004.0)
      event3 = %MarketEvent{data: %{"xauusd" => tick3}}
      {:ok, new_event3, _new_state} = ResampleProcessor.next(event3, state)
      assert %Bar{new_bar?: true, new_market?: true} = new_event3.data["xauusd"]
    end

    test "new_market? is true when crossing market_open boundary (time-based)" do
      opts = [data: "xauusd", timeframe: "m5", market_open: ~T[09:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick before market_open
      tick1 = build_tick(~U[2024-01-01 08:57:30Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, new_event1, state} = ResampleProcessor.next(event1, state)
      assert %Bar{new_market?: false} = new_event1.data["xauusd"]

      # Second tick still before market_open
      tick2 = build_tick(~U[2024-01-01 08:59:00Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, new_event2, state} = ResampleProcessor.next(event2, state)
      assert %Bar{new_market?: false} = new_event2.data["xauusd"]

      # Third tick crosses market_open boundary - should create new bar with new_market? = true
      tick3 = build_tick(~U[2024-01-01 09:00:05Z], bid: 2002.0, ask: 2004.0)
      event3 = %MarketEvent{data: %{"xauusd" => tick3}}
      {:ok, new_event3, _new_state} = ResampleProcessor.next(event3, state)
      assert %Bar{new_bar?: true, new_market?: true} = new_event3.data["xauusd"]
    end

    test "new_market? is false when next_time reached without crossing market_open (time-based)" do
      opts = [data: "xauusd", timeframe: "m5", market_open: ~T[09:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First tick after market_open
      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _new_event1, state} = ResampleProcessor.next(event1, state)

      # Second tick updates same bar
      tick2 = build_tick(~U[2024-01-01 10:02:00Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, _new_event2, state} = ResampleProcessor.next(event2, state)

      # Third tick creates new bar (next_time reached) but no market_open crossing
      tick3 = build_tick(~U[2024-01-01 10:05:00Z], bid: 2002.0, ask: 2004.0)
      event3 = %MarketEvent{data: %{"xauusd" => tick3}}
      {:ok, new_event3, _new_state} = ResampleProcessor.next(event3, state)
      assert %Bar{new_bar?: true, new_market?: false} = new_event3.data["xauusd"]
    end

    test "new_market? is false when counter reaches limit without crossing market_open (tick-based)" do
      opts = [data: "xauusd", timeframe: "t2", market_open: ~T[09:00:00], name: "xauusd"]
      {:ok, state} = ResampleProcessor.init(opts)

      # First bar after market_open
      tick1 = build_tick(~U[2024-01-01 10:00:00Z], bid: 2000.0, ask: 2002.0)
      event1 = %MarketEvent{data: %{"xauusd" => tick1}}
      {:ok, _new_event1, state} = ResampleProcessor.next(event1, state)

      # Second tick updates same bar
      tick2 = build_tick(~U[2024-01-01 10:00:01Z], bid: 2001.0, ask: 2003.0)
      event2 = %MarketEvent{data: %{"xauusd" => tick2}}
      {:ok, _new_event2, state} = ResampleProcessor.next(event2, state)

      # Third tick creates new bar (counter reached) but no market_open crossing
      tick3 = build_tick(~U[2024-01-01 10:00:02Z], bid: 2002.0, ask: 2004.0)
      event3 = %MarketEvent{data: %{"xauusd" => tick3}}
      {:ok, new_event3, _new_state} = ResampleProcessor.next(event3, state)
      assert %Bar{new_bar?: true, new_market?: false} = new_event3.data["xauusd"]
    end
  end

  describe "next/2 - Bar -> Bar resampling" do
    test "creates first bar from source bar (m1 -> m5)" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      source_bar = %Bar{
        time: ~U[2024-01-01 10:02:30Z],
        open: 1.0850,
        high: 1.0860,
        low: 1.0845,
        close: 1.0855,
        volume: 100.0
      }

      event = %MarketEvent{data: %{"eurusd_m1" => source_bar}}
      assert {:ok, new_event, new_state} = ResampleProcessor.next(event, state)

      result_bar = new_event.data["eurusd_m5"]
      assert result_bar.time == ~U[2024-01-01 10:00:00Z]
      assert result_bar.open == 1.0850
      assert result_bar.high == 1.0860
      assert result_bar.low == 1.0845
      assert result_bar.close == 1.0855
      assert result_bar.volume == 100.0
      assert new_state.current_bar == result_bar
      assert new_state.next_time == ~U[2024-01-01 10:05:00Z]
    end

    test "updates bar within same timeframe period" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{
        time: ~U[2024-01-01 10:01:00Z],
        open: 1.0850,
        high: 1.0860,
        low: 1.0845,
        close: 1.0855,
        volume: 100.0
      }

      event1 = %MarketEvent{data: %{"eurusd_m1" => bar1}}
      {:ok, _event1, state1} = ResampleProcessor.next(event1, state)

      bar2 = %Bar{
        time: ~U[2024-01-01 10:02:00Z],
        open: 1.0855,
        high: 1.0870,
        low: 1.0850,
        close: 1.0865,
        volume: 150.0
      }

      event2 = %MarketEvent{data: %{"eurusd_m1" => bar2}}
      {:ok, new_event2, new_state} = ResampleProcessor.next(event2, state1)

      result_bar = new_event2.data["eurusd_m5"]
      assert result_bar.time == ~U[2024-01-01 10:00:00Z]
      assert result_bar.open == 1.0850
      assert result_bar.high == 1.0870
      assert result_bar.low == 1.0845
      assert result_bar.close == 1.0865
      assert result_bar.volume == 250.0
      assert new_state.next_time == ~U[2024-01-01 10:05:00Z]
    end

    test "creates new bar when crossing timeframe boundary" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{
        time: ~U[2024-01-01 10:04:00Z],
        open: 1.0850,
        high: 1.0860,
        low: 1.0845,
        close: 1.0855,
        volume: 100.0
      }

      event1 = %MarketEvent{data: %{"eurusd_m1" => bar1}}
      {:ok, _event1, state1} = ResampleProcessor.next(event1, state)

      bar2 = %Bar{
        time: ~U[2024-01-01 10:05:00Z],
        open: 1.0855,
        high: 1.0870,
        low: 1.0850,
        close: 1.0865,
        volume: 150.0
      }

      event2 = %MarketEvent{data: %{"eurusd_m1" => bar2}}
      {:ok, new_event2, new_state} = ResampleProcessor.next(event2, state1)

      result_bar = new_event2.data["eurusd_m5"]
      assert result_bar.time == ~U[2024-01-01 10:05:00Z]
      assert result_bar.open == 1.0855
      assert result_bar.high == 1.0870
      assert result_bar.low == 1.0850
      assert result_bar.close == 1.0865
      assert result_bar.volume == 150.0
      assert new_state.next_time == ~U[2024-01-01 10:10:00Z]
    end

    test "handles volume accumulation correctly with multiple bars" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{
        time: ~U[2024-01-01 10:00:00Z],
        open: 1.0,
        high: 1.0,
        low: 1.0,
        close: 1.0,
        volume: 100.0
      }

      bars = [
        bar1,
        %Bar{bar1 | time: ~U[2024-01-01 10:01:00Z], volume: 150.0},
        %Bar{bar1 | time: ~U[2024-01-01 10:02:00Z], volume: 200.0},
        %Bar{bar1 | time: ~U[2024-01-01 10:03:00Z], volume: 120.0},
        %Bar{bar1 | time: ~U[2024-01-01 10:04:00Z], volume: 80.0}
      ]

      {final_event, _final_state} = process_bars(state, bars, "eurusd_m1")

      assert final_event.data["eurusd_m5"].volume == 650.0
    end

    test "handles nil volumes" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{time: ~U[2024-01-01 10:01:00Z], open: 1.0, high: 1.0, low: 1.0, close: 1.0}
      event1 = %MarketEvent{data: %{"eurusd_m1" => bar1}}
      {:ok, event1_result, state1} = ResampleProcessor.next(event1, state)

      assert event1_result.data["eurusd_m5"].volume == nil

      bar2 = %Bar{bar1 | time: ~U[2024-01-01 10:02:00Z]}
      event2 = %MarketEvent{data: %{"eurusd_m1" => bar2}}
      {:ok, event2_result, _state2} = ResampleProcessor.next(event2, state1)

      assert event2_result.data["eurusd_m5"].volume == nil
    end

    test "handles mixed nil and non-nil volumes" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{time: ~U[2024-01-01 10:01:00Z], open: 1.0, high: 1.0, low: 1.0, close: 1.0}
      event1 = %MarketEvent{data: %{"eurusd_m1" => bar1}}
      {:ok, _event1, state1} = ResampleProcessor.next(event1, state)

      bar2 = %Bar{bar1 | time: ~U[2024-01-01 10:02:00Z], volume: 100.0}
      event2 = %MarketEvent{data: %{"eurusd_m1" => bar2}}
      {:ok, event2_result, state2} = ResampleProcessor.next(event2, state1)

      assert event2_result.data["eurusd_m5"].volume == 100.0

      bar3 = %Bar{bar1 | time: ~U[2024-01-01 10:03:00Z]}
      event3 = %MarketEvent{data: %{"eurusd_m1" => bar3}}
      {:ok, event3_result, _state3} = ResampleProcessor.next(event3, state2)

      assert event3_result.data["eurusd_m5"].volume == 100.0
    end

    test "preserves source bar in event data" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      source_bar = %Bar{
        time: ~U[2024-01-01 10:02:30Z],
        open: 1.0,
        high: 1.0,
        low: 1.0,
        close: 1.0
      }

      event = %MarketEvent{data: %{"eurusd_m1" => source_bar}}
      {:ok, new_event, _new_state} = ResampleProcessor.next(event, state)

      assert new_event.data["eurusd_m1"] == source_bar
      assert new_event.data["eurusd_m5"] != nil
      assert new_event.data["eurusd_m5"] != source_bar
    end

    test "recalculates new_bar? flag for target timeframe" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{
        time: ~U[2024-01-01 10:01:00Z],
        open: 1.0,
        high: 1.0,
        low: 1.0,
        close: 1.0,
        new_bar?: false
      }

      event1 = %MarketEvent{data: %{"eurusd_m1" => bar1}}
      {:ok, event1_result, state1} = ResampleProcessor.next(event1, state)

      assert event1_result.data["eurusd_m5"].new_bar? == true

      bar2 = %Bar{bar1 | time: ~U[2024-01-01 10:02:00Z]}
      event2 = %MarketEvent{data: %{"eurusd_m1" => bar2}}
      {:ok, event2_result, state2} = ResampleProcessor.next(event2, state1)

      assert event2_result.data["eurusd_m5"].new_bar? == false

      bar3 = %Bar{bar1 | time: ~U[2024-01-01 10:05:00Z]}
      event3 = %MarketEvent{data: %{"eurusd_m1" => bar3}}
      {:ok, event3_result, _state3} = ResampleProcessor.next(event3, state2)

      assert event3_result.data["eurusd_m5"].new_bar? == true
    end

    test "recalculates new_market? flag for target timeframe" do
      opts = [data: "eurusd_m1", timeframe: "m5", name: "eurusd_m5", market_open: ~T[09:30:00]]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{time: ~U[2024-01-01 09:28:00Z], open: 1.0, high: 1.0, low: 1.0, close: 1.0}
      event1 = %MarketEvent{data: %{"eurusd_m1" => bar1}}
      {:ok, event1_result, state1} = ResampleProcessor.next(event1, state)

      assert event1_result.data["eurusd_m5"].new_market? == false

      bar2 = %Bar{bar1 | time: ~U[2024-01-01 09:29:00Z]}
      event2 = %MarketEvent{data: %{"eurusd_m1" => bar2}}
      {:ok, event2_result, state2} = ResampleProcessor.next(event2, state1)

      assert event2_result.data["eurusd_m5"].new_market? == false
      assert event2_result.data["eurusd_m5"].new_bar? == false

      bar3 = %Bar{bar1 | time: ~U[2024-01-01 09:30:00Z]}
      event3 = %MarketEvent{data: %{"eurusd_m1" => bar3}}
      {:ok, event3_result, _state3} = ResampleProcessor.next(event3, state2)

      assert event3_result.data["eurusd_m5"].new_bar? == true
      assert event3_result.data["eurusd_m5"].new_market? == true
    end

    test "raises error for tick-based timeframe with Bar input" do
      opts = [data: "eurusd_m1", timeframe: "t5", name: "eurusd_t5"]
      {:ok, state} = ResampleProcessor.init(opts)

      source_bar = %Bar{
        time: ~U[2024-01-01 10:02:30Z],
        open: 1.0,
        high: 1.0,
        low: 1.0,
        close: 1.0
      }

      event = %MarketEvent{data: %{"eurusd_m1" => source_bar}}

      assert_raise RuntimeError, ~r/Data must be Tick, got/, fn ->
        ResampleProcessor.next(event, state)
      end
    end

    test "hourly resampling from minute bars (m1 -> h1)" do
      opts = [data: "eurusd_m1", timeframe: "h1", name: "eurusd_h1"]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{
        time: ~U[2024-01-01 10:30:00Z],
        open: 1.0850,
        high: 1.0860,
        low: 1.0845,
        close: 1.0855,
        volume: 100.0
      }

      event1 = %MarketEvent{data: %{"eurusd_m1" => bar1}}
      {:ok, event1_result, state1} = ResampleProcessor.next(event1, state)

      assert event1_result.data["eurusd_h1"].time == ~U[2024-01-01 10:00:00Z]
      assert state1.next_time == ~U[2024-01-01 11:00:00Z]

      bar2 = %Bar{
        time: ~U[2024-01-01 10:45:00Z],
        open: 1.0855,
        high: 1.0870,
        low: 1.0840,
        close: 1.0865,
        volume: 150.0
      }

      event2 = %MarketEvent{data: %{"eurusd_m1" => bar2}}
      {:ok, event2_result, state2} = ResampleProcessor.next(event2, state1)

      assert event2_result.data["eurusd_h1"].time == ~U[2024-01-01 10:00:00Z]
      assert event2_result.data["eurusd_h1"].new_bar? == false
      assert event2_result.data["eurusd_h1"].high == 1.0870
      assert event2_result.data["eurusd_h1"].low == 1.0840

      bar3 = %Bar{
        time: ~U[2024-01-01 11:00:00Z],
        open: 1.0865,
        high: 1.0880,
        low: 1.0860,
        close: 1.0875,
        volume: 200.0
      }

      event3 = %MarketEvent{data: %{"eurusd_m1" => bar3}}
      {:ok, event3_result, _state3} = ResampleProcessor.next(event3, state2)

      assert event3_result.data["eurusd_h1"].time == ~U[2024-01-01 11:00:00Z]
      assert event3_result.data["eurusd_h1"].new_bar? == true
    end

    test "daily resampling from hourly bars (h1 -> D)" do
      opts = [data: "eurusd_h1", timeframe: "D", name: "eurusd_D", market_open: ~T[00:00:00]]
      {:ok, state} = ResampleProcessor.init(opts)

      bar1 = %Bar{
        time: ~U[2024-01-01 10:00:00Z],
        open: 1.0850,
        high: 1.0860,
        low: 1.0845,
        close: 1.0855,
        volume: 1000.0
      }

      event1 = %MarketEvent{data: %{"eurusd_h1" => bar1}}
      {:ok, event1_result, state1} = ResampleProcessor.next(event1, state)

      assert event1_result.data["eurusd_D"].time == ~U[2024-01-01 00:00:00Z]
      assert state1.next_time == ~U[2024-01-02 00:00:00Z]

      bar2 = %Bar{
        time: ~U[2024-01-01 15:00:00Z],
        open: 1.0855,
        high: 1.0890,
        low: 1.0830,
        close: 1.0880,
        volume: 1500.0
      }

      event2 = %MarketEvent{data: %{"eurusd_h1" => bar2}}
      {:ok, event2_result, state2} = ResampleProcessor.next(event2, state1)

      assert event2_result.data["eurusd_D"].time == ~U[2024-01-01 00:00:00Z]
      assert event2_result.data["eurusd_D"].high == 1.0890
      assert event2_result.data["eurusd_D"].low == 1.0830
      assert event2_result.data["eurusd_D"].volume == 2500.0

      bar3 = %Bar{
        time: ~U[2024-01-02 00:00:00Z],
        open: 1.0880,
        high: 1.0900,
        low: 1.0870,
        close: 1.0895,
        volume: 800.0
      }

      event3 = %MarketEvent{data: %{"eurusd_h1" => bar3}}
      {:ok, event3_result, _state3} = ResampleProcessor.next(event3, state2)

      assert event3_result.data["eurusd_D"].time == ~U[2024-01-02 00:00:00Z]
      assert event3_result.data["eurusd_D"].new_bar? == true
      assert event3_result.data["eurusd_D"].volume == 800.0
    end

    test "raises error when data is not Tick or Bar" do
      opts = [data: "eurusd", timeframe: "m5", name: "eurusd_m5"]
      {:ok, state} = ResampleProcessor.init(opts)

      event = %MarketEvent{data: %{"eurusd" => "invalid data"}}

      assert_raise RuntimeError, ~r/Data must be Tick or Bar/, fn ->
        ResampleProcessor.next(event, state)
      end
    end
  end

  ## Private test helpers

  defp build_tick(time, opts) do
    bid = Keyword.get(opts, :bid)
    ask = Keyword.get(opts, :ask)
    bid_volume = Keyword.get(opts, :bid_volume)
    ask_volume = Keyword.get(opts, :ask_volume)

    %Tick{
      time: time,
      bid: bid,
      ask: ask,
      bid_volume: bid_volume,
      ask_volume: ask_volume
    }
  end

  defp build_tick_sequence(count) do
    Enum.map(0..(count - 1), fn i ->
      time = DateTime.add(~U[2024-01-01 10:00:00Z], i, :second)
      base_price = 2000.0 + i
      build_tick(time, bid: base_price, ask: base_price + 2.0)
    end)
  end

  defp process_ticks(state, ticks) do
    Enum.reduce(ticks, {nil, state}, fn tick, {_event, acc_state} ->
      event = %MarketEvent{data: %{"xauusd" => tick}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event, acc_state)
      {new_event, new_state}
    end)
  end

  defp process_bars(state, bars, data_name) do
    Enum.reduce(bars, {nil, state}, fn bar, {_event, acc_state} ->
      event = %MarketEvent{data: %{data_name => bar}}
      {:ok, new_event, new_state} = ResampleProcessor.next(event, acc_state)
      {new_event, new_state}
    end)
  end
end
