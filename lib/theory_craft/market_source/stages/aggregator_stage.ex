defmodule TheoryCraft.MarketSource.AggregatorStage do
  @moduledoc """
  A GenStage producer_consumer that synchronizes events from multiple parallel producers.

  This stage subscribes to N producers running in parallel (e.g., multiple indicators
  processing the same events) and synchronizes their outputs by:
  1. Buffering events from each producer separately
  2. Emitting events only when ALL producers have sent at least 1 event
  3. Merging parallel events into a single MarketEvent with combined data

  ## Synchronization Logic

  The aggregator maintains a separate buffer for each producer. When events arrive:
  - Events are added to the corresponding producer's buffer
  - The aggregator calculates `min_count = min(all buffer sizes)`
  - It emits `min_count` events by:
    - Taking the first `min_count` events from each buffer
    - Zipping them together (1st from each, 2nd from each, etc.)
    - Merging their `data` maps into single MarketEvents
  - It asks for more events when all buffers are at the same level

  This ensures synchronized processing where each emitted MarketEvent contains
  data from all parallel processors at the same logical time.

  ## Examples

      # Start aggregator for 3 parallel processors
      {:ok, aggregator} = AggregatorStage.start_link(
        producer_count: 3,
        subscribe_to: [] # Subscriptions added later
      )

      # Each ProcessorStage subscribes with its index
      {:ok, p1} = ProcessorStage.start_link(
        {Indicator1, opts},
        subscribe_to: [{producer, index: 0}]
      )

  ## Options

    - `:producer_count` - Number of parallel producers to expect (required)
    - `:subscribe_to` - List of producers to subscribe to (can be empty, added later)
    - `:min_demand` - Minimum demand (default: 0)
    - `:max_demand` - Maximum demand (default: 1000)
  """

  use GenStage

  require Logger

  alias TheoryCraft.MarketSource.{MarketEvent, StageHelpers}

  @typedoc """
  Options for starting an AggregatorStage.

  Stage-specific options:
  - `:producer_count` - Number of producers to aggregate (required)
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
          {:producer_count, pos_integer()}
          | {:subscribe_to, [GenStage.stage() | {GenStage.stage(), keyword()}]}
          | {:name, atom()}
          | {:timeout, timeout()}
          | {:debug, Keyword.t()}
          | {:spawn_opt, Process.spawn_opt()}
          | {:hibernate_after, timeout()}
          | {:min_demand, non_neg_integer()}
          | {:max_demand, pos_integer()}
          | {:buffer_size, non_neg_integer()}
          | {:buffer_keep, :first | :last}

  ## Public API

  @doc """
  Starts an AggregatorStage as a GenStage producer_consumer.

  ## Parameters

    - `opts` - Keyword list of options (must include `:producer_count`)

  ## Returns

    - `{:ok, pid}` on success
    - `{:error, reason}` on failure

  ## Examples

      # Aggregator for 3 parallel processors
      {:ok, aggregator} = AggregatorStage.start_link(
        producer_count: 3,
        subscribe_to: [{upstream1, index: 0}, {upstream2, index: 1}, {upstream3, index: 2}]
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
    Logger.debug("AggregatorStage starting with opts=#{inspect(opts)}")

    producer_count = Keyword.fetch!(opts, :producer_count)
    max_demand = Keyword.get(opts, :max_demand, 1000)
    indices = Enum.to_list(0..(producer_count - 1))

    state =
      StageHelpers.init_tracking_state(%{
        max_demand: max_demand,
        buffers: Map.new(indices, &{&1, []}),
        next_index: 0
      })

    subscribe_to = Keyword.get(opts, :subscribe_to, [])

    # Extract subscription options
    # Note: max_demand is excluded because it's used internally by this stage for GenStage.ask
    subscription_opts =
      opts
      |> Keyword.delete(:max_demand)
      |> StageHelpers.extract_subscription_opts()
      |> Keyword.merge(subscribe_to: subscribe_to)

    Logger.debug("AggregatorStage initialized successfully")

    {:producer_consumer, state, subscription_opts}
  end

  @impl true
  def handle_subscribe(:producer, _opts, from, state) do
    %{max_demand: max_demand, next_index: index} = state

    Logger.debug("AggregatorStage: Producer subscribed with index=#{index}")

    # Register producer with its index and increment next_index
    new_state =
      state
      |> Map.update!(:producers, &Map.put(&1, from, index))
      |> Map.update!(:next_index, &(&1 + 1))

    # Ask for events with manual demand
    GenStage.ask(from, max_demand)

    {:manual, new_state}
  end

  @impl true
  def handle_subscribe(:consumer, _opts, {_pid, ref}, state) do
    %{consumers: consumers} = state
    new_state = %{state | consumers: Map.put(consumers, ref, true)}
    {:automatic, new_state}
  end

  @impl true
  def handle_cancel(_reason, from, state) do
    %{producers: producers, consumers: consumers} = state
    {_pid, ref} = from

    case {producers, consumers} do
      {%{^from => _index}, _consumers} ->
        # Producer cancelled
        updated_producers = Map.delete(producers, from)

        Logger.debug(
          "AggregatorStage: Producer cancelled, #{map_size(updated_producers)} remaining"
        )

        if map_size(updated_producers) == 0 do
          # Last producer cancelled, stop this stage
          Logger.debug("AggregatorStage: Last producer cancelled, stopping")
          GenStage.async_info(self(), :stop)
        end

        {:noreply, [], %{state | producers: updated_producers}}

      {_producers, %{^ref => _}} ->
        # Consumer cancelled
        new_consumers = Map.delete(consumers, ref)

        if map_size(new_consumers) == 0 do
          # Last consumer cancelled, stop this stage
          {:stop, :normal, state}
        else
          {:noreply, [], %{state | consumers: new_consumers}}
        end
    end
  end

  @impl true
  def handle_info(:stop, state) do
    StageHelpers.handle_stop_message(state, "AggregatorStage")
  end

  @impl true
  def handle_events(events, from, state) do
    %{
      buffers: buffers,
      producers: producers,
      max_demand: max_demand
    } = state

    # Get the index for this producer
    index = Map.fetch!(producers, from)

    # Add events to the buffer for this producer
    buffers = Map.update!(buffers, index, fn buffer -> buffer ++ events end)

    # Calculate buffer sizes
    buffer_sizes = Enum.map(Map.values(buffers), &length/1)
    min_size = Enum.min(buffer_sizes)
    max_size = Enum.max(buffer_sizes)

    # Emit synchronized events
    {merged_events, updated_buffers} = merge_and_emit(buffers, min_size)

    # Ask for more events if all buffers are at the same level
    if min_size == max_size do
      for {producer_from, _index} <- producers do
        :ok = GenStage.ask(producer_from, max_demand)
      end
    end

    {:noreply, merged_events, %{state | buffers: updated_buffers}}
  end

  ## Private functions

  # No events to merge
  defp merge_and_emit(buffers, 0), do: {[], buffers}

  # Merge min_size events from all buffers
  defp merge_and_emit(buffers, count) do
    # Split events from each buffer
    updates =
      buffers
      |> Map.to_list()
      |> Enum.sort_by(fn {index, _buffer} -> index end)
      |> Enum.map(fn {index, buffer} ->
        {to_emit, remaining} = Enum.split(buffer, count)
        {index, to_emit, remaining}
      end)

    # Extract events to merge (list of lists)
    events_by_producer =
      Enum.map(updates, fn {_index, to_emit, _remaining} -> to_emit end)

    # Zip events together and merge their data maps
    merged_events =
      events_by_producer
      |> Enum.zip()
      |> Enum.map(fn events_tuple ->
        events = Tuple.to_list(events_tuple)

        # Take time and source from the first event
        [first_event | _rest] = events

        # Merge all data maps from parallel events
        merged_data =
          Enum.reduce(events, %{}, fn event, acc ->
            Map.merge(acc, event.data)
          end)

        %MarketEvent{first_event | data: merged_data}
      end)

    # Update buffers with remaining events
    updated_buffers =
      Map.new(updates, fn {index, _to_emit, remaining} ->
        {index, remaining}
      end)

    {merged_events, updated_buffers}
  end
end
