defmodule TheoryCraft.MarketSource.IndicatorProcessor do
  @moduledoc """
  A Processor that wraps a `TheoryCraft.Indicator` behaviour for integration into processing pipelines.

  This processor handles the initialization and delegation of values from market events to the wrapped
  indicator module, allowing indicators to be used seamlessly in `MarketSource` pipelines.

  ## Examples

      # Creating an indicator processor for a custom SMA indicator
      alias TheoryCraft.MarketSource.IndicatorProcessor

      {:ok, processor_state} = IndicatorProcessor.init(
        module: MyIndicators.SMA,
        period: 20,
        name: "sma20"
      )

      # Process an event through the indicator
      event = %MarketEvent{data: %{"eurusd_m5" => bar}}
      {:ok, updated_event, new_state} = IndicatorProcessor.next(event, processor_state)

      # The updated event now contains the indicator output
      updated_event.data["sma20"]  # => SMA value

  ## Integration with MarketSource

  Indicators are typically added to a market source using the `TA` macro syntax:

      require TheoryCraftTA.TA, as: TA

      market =
        %MarketSource{}
        |> MarketSource.add_data(bar_stream, name: "eurusd_m5")
        |> MarketSource.add_indicator(TA.sma(eurusd_m5[:close], 20, name: "sma20"))
        |> MarketSource.stream()

  See `TheoryCraft.MarketSource` for more details.

  """

  alias __MODULE__
  alias TheoryCraft.MarketSource.MarketEvent
  alias TheoryCraft.Utils

  @behaviour TheoryCraft.MarketSource.Processor

  @typedoc """
  The processor state containing the indicator module and its state.
  """
  @type t :: %__MODULE__{
          module: module(),
          state: any(),
          name: String.t()
        }

  defstruct [:module, :state, :name]

  ## Processor behaviour

  @doc """
  Initializes the indicator processor with the given options.

  This function extracts the indicator module from the options, then forwards all
  remaining options to the indicator's `init/1` callback to construct the processor state.

  ## Options

    - `:module` (required) - The indicator module implementing `TheoryCraft.Indicator`
    - All other options are passed through to the indicator's `init/1` callback

  The indicator itself is responsible for validating its required options (such as
  `:data` for the data stream name and `:name` for the output name).

  ## Returns

    - `{:ok, state}` - The initial processor state containing the indicator module
      and the indicator's internal state
    - `{:error, reason}` - If the indicator's initialization fails

  ## Examples

      # Initialize with a simple moving average indicator
      iex> IndicatorProcessor.init(module: MyIndicators.SMA, data: "eurusd_m5", name: "sma20", period: 20)
      {:ok, %IndicatorProcessor{
        indicator_module: MyIndicators.SMA,
        indicator_state: %{period: 20, data: "eurusd_m5", name: "sma20"}
      }}

  ## Errors

      # Missing required module option
      iex> IndicatorProcessor.init(data: "eurusd", name: "sma", period: 20)
      ** (ArgumentError) Missing required option: module

  """
  @impl true
  @spec init(Keyword.t()) :: {:ok, t()}
  def init(opts) do
    indicator_module = Utils.required_opt!(opts, :module)
    name = Utils.required_opt!(opts, :name)

    # Forward all options (except :module) to the indicator
    indicator_opts = Keyword.delete(opts, :module)

    case indicator_module.init(indicator_opts) do
      {:ok, indicator_state} ->
        state = %IndicatorProcessor{
          module: indicator_module,
          state: indicator_state,
          name: name
        }

        {:ok, state}

      error ->
        error
    end
  end

  @doc """
  Processes a MarketEvent by delegating to the indicator.

  This function forwards the event to the indicator's `next/2` callback and updates
  the processor state with the indicator's new state. The indicator is responsible for:
  - Extracting the relevant data from the event
  - Determining if it's a new bar
  - Calculating its output value
  - Writing the output to the event

  ## Parameters

    - `event` - The `MarketEvent` to process
    - `state` - The processor state containing the indicator module and its state

  ## Returns

    - `{:ok, updated_event, new_state}` - A tuple containing:
      - `updated_event` - The `MarketEvent` with the indicator output added by the indicator
      - `new_state` - The updated processor state with new indicator state
    - `{:error, reason}` - If the indicator's processing fails

  ## Examples

      # Process a market event through an SMA indicator
      {:ok, state} = IndicatorProcessor.init(
        module: MyIndicators.SMA,
        data: "eurusd_m5",
        name: "sma5",
        period: 5
      )

      bar = %TheoryCraft.Bar{
        time: ~U[2024-01-01 10:00:00Z],
        open: 100.0,
        high: 102.0,
        low: 99.0,
        close: 101.0,
        volume: 1000.0
      }

      event = %MarketEvent{data: %{"eurusd_m5" => bar}}
      {:ok, updated_event, new_state} = IndicatorProcessor.next(event, state)

      # The indicator has added its output to the event
      updated_event.data["sma5"]  # => SMA value calculated by the indicator

      # Process another event
      event2 = %MarketEvent{data: %{"eurusd_m5" => updated_bar}}
      {:ok, updated_event2, newer_state} = IndicatorProcessor.next(event2, new_state)

  """
  @impl true
  @spec next(MarketEvent.t(), t()) :: {:ok, MarketEvent.t(), t()}
  def next(%MarketEvent{} = event, %IndicatorProcessor{} = processor_state) do
    %MarketEvent{data: event_data} = event

    %IndicatorProcessor{
      module: indicator_module,
      state: indicator_state,
      name: name
    } = processor_state

    case indicator_module.next(event, indicator_state) do
      {:ok, indicator_value, new_indicator_state} ->
        # Add the indicator value to the event data under the configured output name
        new_data = Map.put(event_data, name, indicator_value)
        updated_event = %MarketEvent{event | data: new_data}
        new_state = %IndicatorProcessor{processor_state | state: new_indicator_state}

        {:ok, updated_event, new_state}

      error ->
        error
    end
  end
end
