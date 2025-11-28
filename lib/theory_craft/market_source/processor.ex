defmodule TheoryCraft.MarketSource.Processor do
  @moduledoc """
  Behaviour for stateful stream processors that transform market data.

  A Processor is a stateful component that processes a stream of `MarketEvent` structs,
  transforming or enriching the data as it flows through. Processors can perform various
  operations such as:

  - **Data transformation**: Convert ticks to bars, apply indicators, etc.
  - **Data enrichment**: Add calculated fields, merge multiple data sources
  - **Filtering**: Remove unwanted events based on criteria
  - **State tracking**: Maintain running calculations across events

  ## Processor Lifecycle

  1. **Initialization** (`init/1`): Called once at the start to set up initial state
  2. **Processing** (`next/2`): Called for each event, receives the event and current state,
     returns the transformed event and new state

  ## Implementing a Processor

  To create a custom processor, implement the `TheoryCraft.MarketSource.Processor` behaviour:

      defmodule MyProcessor do
        @behaviour TheoryCraft.MarketSource.Processor

        @impl true
        def init(opts) do
          # Initialize your processor state
          state = %{counter: 0}
          {:ok, state}
        end

        @impl true
        def next(event, state) do
          # Process the event and update state
          new_event = transform_event(event)
          new_state = %{state | counter: state.counter + 1}
          {:ok, new_event, new_state}
        end
      end

  ## Built-in Processors

  - `TheoryCraft.MarketSource.ResampleProcessor` - Resamples Tick or Bar data to OHLC bars

  ## Integration with MarketSource

  Processors are typically used with `TheoryCraft.MarketSource` which orchestrates
  the data flow through multiple processors:

      market =
        %MarketSource{}
        |> MarketSource.add_data(tick_stream, name: "eurusd")
        |> MarketSource.add_processor(MyProcessor, processor_opts)
        |> MarketSource.stream()

  See `TheoryCraft.MarketSource` for more details on building processing pipelines.
  """

  alias TheoryCraft.MarketSource.MarketEvent

  @typedoc """
  A Processor specification as a tuple of module and options, or just a module.

  When only the module is provided, options is an empty list.
  """
  @type spec :: {module(), Keyword.t()} | module()

  @doc """
  Initializes the processor with the given options.

  This callback is invoked once when the processor is added to a pipeline. It should
  return `{:ok, state}` where `state` is the initial state that will be passed to
  subsequent `next/2` calls.

  ## Parameters

    - `opts` - Keyword list of options for configuring the processor. The available
      options depend on the specific processor implementation.

  ## Returns

    - `{:ok, state}` - The initial processor state

  ## Examples

      # Simple stateless processor
      def init(_opts) do
        {:ok, %{}}
      end

      # Processor with configuration
      def init(opts) do
        threshold = Keyword.get(opts, :threshold, 100)
        {:ok, %{threshold: threshold, count: 0}}
      end

      # Processor that validates options
      def init(opts) do
        case Keyword.fetch(opts, :required_param) do
          {:ok, value} -> {:ok, %{param: value}}
          :error -> raise ArgumentError, "Missing required option: :required_param"
        end
      end

  """
  @callback init(opts :: Keyword.t()) :: {:ok, state :: any()}

  @doc """
  Processes a single market event and returns the transformed event with updated state.

  This callback is invoked for each event in the stream. It receives the current event
  and the processor's state, and must return the transformed event along with the new state.

  The processor can:
  - Transform the event data (add, modify, or remove fields)
  - Maintain state across events (counters, accumulators, etc.)
  - Read from multiple data streams in the event's data map
  - Write to new or existing keys in the event's data map

  ## Parameters

    - `event` - The current `MarketEvent` to process
    - `state` - The current processor state (from `init/1` or previous `next/2` call)

  ## Returns

    - `{:ok, updated_event, new_state}` - A tuple containing:
      - `updated_event` - The transformed `MarketEvent`
      - `new_state` - The updated processor state for the next event

  ## Examples

      # Simple transformation that adds a field
      def next(event, state) do
        updated_data = Map.put(event.data, "processed", true)
        updated_event = %MarketEvent{event | data: updated_data}
        {:ok, updated_event, state}
      end

      # Stateful processor that counts events
      def next(event, state) do
        new_state = Map.update!(state, :count, &(&1 + 1))

        # Add count to event metadata
        metadata = Map.put(event.metadata, :event_count, new_state.count)
        updated_event = %MarketEvent{event | metadata: metadata}

        {:ok, updated_event, new_state}
      end

      # Processor that reads input and writes output to different keys
      def next(event, state) do
        # Read tick from "eurusd_ticks"
        tick = event.data["eurusd_ticks"]

        # Transform to bar
        bar = process_tick(tick, state)

        # Write bar to "eurusd_m5" (preserves original tick)
        updated_data = Map.put(event.data, "eurusd_m5", bar)
        updated_event = %MarketEvent{event | data: updated_data}

        {:ok, updated_event, state}
      end

  """
  @callback next(event :: MarketEvent.t(), state :: any()) ::
              {:ok, updated_event :: MarketEvent.t(), new_state :: any()}
end
