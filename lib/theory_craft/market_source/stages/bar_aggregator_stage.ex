defmodule TheoryCraft.MarketSource.BarAggregatorStage do
  @moduledoc """
  A GenStage producer_consumer that aggregates bar events, emitting only completed bars.

  This stage filters out intra-bar tick events and only emits events when at least
  one of the tracked bars becomes complete (i.e., when `new_bar? = true` for any
  tracked bar). This is useful for:
  - Avoiding redundant indicator calculations on incomplete bars
  - Processing only completed bars in trading strategies
  - Reducing downstream event volume

  ## Aggregation Logic

  For each tracked bar name:
  1. On the first `new_bar? = true` event, store the bar but don't emit (no previous bar)
  2. On subsequent `new_bar? = true` events:
     - Emit the previously stored bar with its original `new_bar?` and `new_market?` flags
     - Store the new bar
  3. On `new_bar? = false` events:
     - Update the stored bar's OHLCV values while preserving original flags

  The stage emits an event when at least one tracked bar has a completed bar to emit
  (OR logic across multiple bars).

  ## Flag Preservation

  Each completed bar is emitted with the flags from its first tick:
  - `new_bar?` - From the tick that created the bar
  - `new_market?` - From the tick that created the bar

  ## Important: 1-Tick Lag

  This stage introduces a 1-tick lag in emissions. A bar is only emitted when the
  NEXT bar starts (i.e., when `new_bar? = true` arrives for the same bar name).

  **Implications:**
  - The last bar before stream end will only be emitted when the stream terminates
  - In live/real-time scenarios, the last bar of a session may not be emitted until
    the next session starts (e.g., last daily bar emitted at next day's market open)
  - For backtesting, this is usually not an issue as the stream ends naturally
  - For live trading, consider using a timeout mechanism or manual flush if you need
    immediate access to the latest incomplete bar

  ## Examples

      # Aggregate a single bar
      {:ok, aggregator} = BarAggregatorStage.start_link(
        bar_names: ["xauusd_m5"],
        subscribe_to: [upstream_stage]
      )

      # Aggregate multiple bars (OR logic)
      {:ok, aggregator} = BarAggregatorStage.start_link(
        bar_names: ["xauusd_m5", "xauusd_h1"],
        subscribe_to: [upstream_stage]
      )

  """

  use GenStage

  require Logger

  alias TheoryCraft.MarketSource.{Bar, MarketEvent, StageHelpers}

  @typedoc """
  Options for starting a BarAggregatorStage.

  Stage-specific options:
  - `:bar_names` - List of bar names to aggregate (required)
  - `:subscribe_to` - List of producers to subscribe to (required)

  GenStage/GenServer options:
  - `:name` - Register the stage with a name
  - `:timeout` - Timeout for GenServer.start_link
  - `:debug` - Debug options
  - `:spawn_opt` - Options for spawning the process
  - `:hibernate_after` - Hibernate after inactivity

  Subscription options:
  - `:min_demand` - Minimum demand
  - `:max_demand` - Maximum demand
  - `:buffer_size` - Size of the buffer (default: 10000)
  - `:buffer_keep` - Whether to keep `:first` or `:last` (default: `:last`)
  """
  @type start_option ::
          {:bar_names, [String.t()]}
          | {:subscribe_to, [GenStage.stage() | {GenStage.stage(), keyword()}]}
          | {:name, atom()}
          | {:timeout, timeout()}
          | {:debug, Keyword.t()}
          | {:spawn_opt, Process.spawn_opt()}
          | {:hibernate_after, timeout()}
          | {:min_demand, non_neg_integer()}
          | {:max_demand, non_neg_integer()}
          | {:buffer_size, non_neg_integer()}
          | {:buffer_keep, :first | :last}

  ## Public API

  @doc """
  Starts a BarAggregatorStage as a GenStage producer_consumer.

  ## Parameters

    - `opts` - Keyword list of options (must include `:bar_names` and `:subscribe_to`)

  ## Returns

    - `{:ok, pid}` on success
    - `{:error, reason}` on failure

  ## Examples

      # Basic usage
      {:ok, stage} = BarAggregatorStage.start_link(
        bar_names: ["xauusd_m5"],
        subscribe_to: [upstream_stage]
      )

      # Multiple bars
      {:ok, stage} = BarAggregatorStage.start_link(
        bar_names: ["xauusd_m5", "xauusd_h1"],
        subscribe_to: [upstream_stage]
      )

  """
  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(opts) do
    gen_stage_opts = StageHelpers.extract_gen_stage_opts(opts)
    GenStage.start_link(__MODULE__, opts, gen_stage_opts)
  end

  ## GenStage callbacks

  @impl true
  def init(opts) do
    bar_names = Keyword.fetch!(opts, :bar_names)
    subscribe_to = Keyword.fetch!(opts, :subscribe_to)
    subscription_opts = StageHelpers.extract_subscription_opts(opts)

    if bar_names == [] do
      raise ArgumentError, "bar_names cannot be empty"
    end

    Logger.debug("BarAggregatorStage starting with bar_names=#{inspect(bar_names)}")

    state =
      StageHelpers.init_tracking_state(%{
        bar_names: bar_names,
        # %{"bar_name" => {bar, original_new_bar?, original_new_market?, already_emitted?}}
        stored_bars: Map.new(bar_names, fn name -> {name, nil} end),
        # %MarketEvent{}
        pending_event: nil
      })

    stage_opts = Keyword.merge([subscribe_to: subscribe_to], subscription_opts)

    {:producer_consumer, state, stage_opts}
  end

  @impl true
  def handle_subscribe(:producer, _opts, from, state) do
    StageHelpers.handle_producer_subscribe(state, from)
  end

  @impl true
  def handle_subscribe(:consumer, _opts, from, state) do
    StageHelpers.handle_consumer_subscribe(state, from)
  end

  @impl true
  def handle_cancel(_reason, from, state) do
    StageHelpers.handle_cancel_with_termination(state, from, "BarAggregatorStage")
  end

  @impl true
  def handle_info(:stop, state) do
    %{bar_names: bar_names, stored_bars: stored_bars, pending_event: pending_event} = state

    # Check if we actually have stored bars to emit
    has_stored_bars =
      Enum.any?(bar_names, fn bar_name ->
        Map.get(stored_bars, bar_name) != nil
      end)

    if has_stored_bars and not is_nil(pending_event) do
      # Build final emission (no need to mark as emitted since stream is ending)
      %MarketEvent{data: pending_data} = pending_event

      {final_data, _} =
        build_emission_and_mark_emitted(bar_names, pending_data, stored_bars, stored_bars, %{})

      final_event = %MarketEvent{pending_event | data: final_data}

      # Emit final event then schedule final stop
      GenStage.async_info(self(), :final_stop)

      {:noreply, [final_event], state}
    else
      {:stop, :normal, state}
    end
  end

  @impl true
  def handle_info(:final_stop, state) do
    Logger.debug("BarAggregatorStage: Final stop after emitting stored bars")
    {:stop, :normal, state}
  end

  @impl true
  def handle_events(events, _from, state) do
    %{
      bar_names: bar_names,
      stored_bars: stored_bars,
      pending_event: pending_event
    } = state

    {aggregated_events, new_stored_bars, new_pending_event} =
      Enum.reduce(
        events,
        {[], stored_bars, pending_event},
        fn event, {acc_events, acc_bars, acc_pending} ->
          process_event(event, bar_names, acc_bars, acc_pending, acc_events)
        end
      )

    new_state = %{state | stored_bars: new_stored_bars, pending_event: new_pending_event}

    {:noreply, Enum.reverse(aggregated_events), new_state}
  end

  ## Private functions

  defp process_event(event, bar_names, stored_bars, pending_event, acc_events) do
    %MarketEvent{data: event_data} = event

    # Pass 1: Update stored bars and check if we should emit (single recursive pass)
    {should_emit, new_stored_bars} =
      update_bars_and_check_emission(bar_names, event_data, stored_bars)

    if should_emit and not is_nil(pending_event) do
      # Pass 2: Build emission data and mark bars as emitted (single recursive pass)
      # Use OLD stored_bars to get flags from pending_event's bars, then mark in NEW stored_bars
      %MarketEvent{data: pending_data} = pending_event

      {final_data, updated_stored_bars} =
        build_emission_and_mark_emitted(
          bar_names,
          pending_data,
          stored_bars,
          new_stored_bars,
          event_data
        )

      emitted_event = %MarketEvent{pending_event | data: final_data}
      {[emitted_event | acc_events], updated_stored_bars, event}
    else
      {acc_events, new_stored_bars, event}
    end
  end

  # Pass 1: Update stored bars and check if any bar triggers emission
  # Uses tail recursion for optimal performance
  defp update_bars_and_check_emission(bar_names, event_data, stored_bars) do
    update_bars_and_check_emission(bar_names, event_data, stored_bars, false, stored_bars)
  end

  defp update_bars_and_check_emission([], _event_data, _stored_bars, should_emit, acc_stored) do
    {should_emit, acc_stored}
  end

  defp update_bars_and_check_emission(
         [bar_name | rest],
         event_data,
         stored_bars,
         should_emit_acc,
         acc_stored
       ) do
    case Map.get(event_data, bar_name) do
      %Bar{new_bar?: true, new_market?: new_market?} = bar ->
        old_stored = Map.get(stored_bars, bar_name)
        new_entry = {bar, true, new_market?, false}
        updated_stored = Map.put(acc_stored, bar_name, new_entry)
        triggers_emission = not is_nil(old_stored)

        update_bars_and_check_emission(
          rest,
          event_data,
          stored_bars,
          should_emit_acc or triggers_emission,
          updated_stored
        )

      %Bar{new_bar?: false} = bar ->
        case Map.get(stored_bars, bar_name) do
          {_old_bar, old_new_bar?, old_new_market?, already_emitted?} ->
            new_entry = {bar, old_new_bar?, old_new_market?, already_emitted?}
            updated_stored = Map.put(acc_stored, bar_name, new_entry)

            update_bars_and_check_emission(
              rest,
              event_data,
              stored_bars,
              should_emit_acc,
              updated_stored
            )

          nil ->
            update_bars_and_check_emission(
              rest,
              event_data,
              stored_bars,
              should_emit_acc,
              acc_stored
            )
        end

      _ ->
        update_bars_and_check_emission(
          rest,
          event_data,
          stored_bars,
          should_emit_acc,
          acc_stored
        )
    end
  end

  # Pass 2: Build emission data with correct flags and mark bars as emitted
  # Uses tail recursion for optimal performance
  # old_stored_bars: contains flags from pending_event's bars (for emission)
  # new_stored_bars: contains updated bars to mark as emitted
  defp build_emission_and_mark_emitted(
         bar_names,
         pending_data,
         old_stored_bars,
         new_stored_bars,
         event_data
       ) do
    build_emission_and_mark_emitted(
      bar_names,
      pending_data,
      old_stored_bars,
      new_stored_bars,
      event_data,
      pending_data,
      new_stored_bars
    )
  end

  defp build_emission_and_mark_emitted(
         [],
         _pending_data,
         _old_stored_bars,
         _new_stored_bars,
         _event_data,
         acc_final_data,
         acc_stored
       ) do
    {acc_final_data, acc_stored}
  end

  defp build_emission_and_mark_emitted(
         [bar_name | rest],
         pending_data,
         old_stored_bars,
         new_stored_bars,
         event_data,
         acc_final_data,
         acc_stored
       ) do
    pending_bar = Map.get(pending_data, bar_name)
    stored_entry = Map.get(old_stored_bars, bar_name)

    case {stored_entry, pending_bar} do
      {{stored_bar, original_new_bar?, original_new_market?, already_emitted?}, %Bar{} = bar} ->
        # Apply emission flags
        {emit_new_bar?, emit_new_market?} =
          if already_emitted? do
            {false, false}
          else
            {original_new_bar?, original_new_market?}
          end

        updated_bar = %Bar{bar | new_bar?: emit_new_bar?, new_market?: emit_new_market?}
        new_final_data = Map.put(acc_final_data, bar_name, updated_bar)

        # Mark as emitted if this is an update (new_bar?=false) and not already emitted
        new_stored =
          case Map.get(event_data, bar_name) do
            %Bar{new_bar?: false} when not already_emitted? ->
              Map.put(
                acc_stored,
                bar_name,
                {stored_bar, original_new_bar?, original_new_market?, true}
              )

            _ ->
              acc_stored
          end

        build_emission_and_mark_emitted(
          rest,
          pending_data,
          old_stored_bars,
          new_stored_bars,
          event_data,
          new_final_data,
          new_stored
        )

      _ ->
        build_emission_and_mark_emitted(
          rest,
          pending_data,
          old_stored_bars,
          new_stored_bars,
          event_data,
          acc_final_data,
          acc_stored
        )
    end
  end
end
