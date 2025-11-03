defmodule TheoryCraft.MarketSource do
  @moduledoc """
  Main orchestrator for building and running backtesting simulations using GenStage pipelines.

  The `MarketSource` provides a fluent API for constructing complex data processing pipelines
  with market data. It uses a builder pattern to configure the pipeline, then materializes it
  into a streaming architecture when `stream/1` is called.

  ## Usage

      require TheoryCraftTA.TA, as: TA

      # Build a pipeline with explicit names
      market =
        %MarketSource{}
        |> add_data_ticks_from_csv("ticks.csv", name: "XAUUSD")
        |> resample("m5", data: "XAUUSD", name: "XAUUSD_m5")
        |> resample("h1", data: "XAUUSD", name: "XAUUSD_h1")
        |> add_indicators_layer([
          TA.sma(XAUUSD_m5[:close], 20, name: "ind1"),
          TA.ema(XAUUSD_h1[:close], 50, name: "ind2")
        ])

      # Stream events
      market
      |> stream()
      |> Enum.each(fn event ->
        # Process each market event
      end)

      # Simplified usage with default names
      %MarketSource{}
      |> add_data_ticks_from_csv("ticks.csv", name: "XAUUSD")
      |> resample("m5")   # data="XAUUSD", name="XAUUSD_m5" (automatic)
      |> resample("h1")   # data="XAUUSD", name="XAUUSD_h1" (automatic)
      |> stream()

  ## Default Names

  To simplify pipeline construction, the market source provides automatic name generation:

  ### Data Feed Names

  When `add_data/3` is called without a `:name` option, the name defaults to a numeric
  index (0 for the first feed, 1 for the second, etc.):

      add_data(market, MemoryDataFeed, from: feed)
      # name defaults to 0

  ### Processor Names

  When `resample/3` is called without `:data` or `:name` options:

  - `:data` defaults to the single data feed's name
  - `:name` defaults to `"{data}_{timeframe}"`

  Example:

      # With data feed named "XAUUSD"
      resample(market, "m5")
      # Equivalent to: resample(market, "m5", data: "XAUUSD", name: "XAUUSD_m5")

  This allows for concise pipeline construction when working with a single data feed:

      %MarketSource{}
      |> add_data(MemoryDataFeed, from: feed, name: "XAUUSD")
      |> resample("m1")   # Creates "XAUUSD_m1"
      |> resample("m5")   # Creates "XAUUSD_m5"
      |> resample("h1")   # Creates "XAUUSD_h1"

  ## Bar Aggregation

  By default, resampling emits an event for every tick, even if the bar is incomplete.
  To emit only completed bars, use either `aggregate_bars/2` or the `:only_bar` option:

      # Single resample with only_bar: true
      %MarketSource{}
      |> add_data_ticks_from_csv("ticks.csv", name: "XAUUSD")
      |> resample("m5", only_bar: true)
      |> stream()

      # Multiple resamples with aggregate_bars
      %MarketSource{}
      |> add_data_ticks_from_csv("ticks.csv", name: "XAUUSD")
      |> resample("m5", name: "XAUUSD_m5")
      |> resample("h1", name: "XAUUSD_h1")
      |> aggregate_bars(["XAUUSD_m5", "XAUUSD_h1"])
      |> stream()

  **Important**: When using `:only_bar: true`, do NOT add more resample layers after it,
  as downstream processors would receive incomplete data. For multiple resamples, use
  `aggregate_bars/2` after all resampling operations.

  ## Limitations

  - Currently supports one data feed at a time
  - Strategy execution not yet implemented (placeholder: `add_strategy`)
  """

  alias __MODULE__
  alias TheoryCraft.{TimeFrame, Utils}

  alias TheoryCraft.MarketSource.{
    AggregatorStage,
    BarAggregatorStage,
    BroadcastStage,
    DataFeedStage,
    Indicator,
    IndicatorProcessor,
    MarketEvent,
    Processor,
    ProcessorStage,
    TicksCSVDataFeed,
    TickToBarProcessor
  }

  defstruct [
    # Data feeds as keyword list: [name: {module, opts}]
    data_feeds: [],
    # All data stream names (feeds + processor outputs)
    data_streams: [],
    # Building phase - store processor specs
    processor_layers: [],
    # Future features (placeholders)
    strategies: []
  ]

  @type strategy_spec :: {module(), Keyword.t()}

  @type t :: %MarketSource{
          data_feeds: Keyword.t({module(), Keyword.t()}),
          data_streams: [String.t() | non_neg_integer()],
          processor_layers: [[Processor.spec()]],
          strategies: [strategy_spec()]
        }

  # require TheoryCraftTA.TA, as: TA
  #
  # stream =
  #   %MarketSource{}
  #   |> add_data_ticks_from_csv(filename, [name: "XAUUSD"] ++ opts)
  #   |> resample("m5", data: "XAUUSD", name: "XAUUSD_m5")
  #   |> resample("h1", data: "XAUUSD", name: "XAUUSD_h1")
  #   |> add_indicators_layer([
  #     TA.volume(XAUUSD_m5[:volume], name: "volume"),
  #     TA.sma(XAUUSD_m5[:close], 20, name: "short_term_m5"),
  #     TA.sma(XAUUSD_m5[:close], 100, name: "long_term_m5"),
  #     TA.atr(XAUUSD_m5, 14, name: "atr_14"),
  #     TA.rsi(XAUUSD_h1[:close], 14, name: "rsi_14")
  #   ], concurrency: 4)
  #   |> add_indicator(TA.sma(volume[:value], 14, name: "volume_sma_14"))
  #   |> add_strategy(TheoryCraft.Strategies.MyStrategy)
  #   |> stream()

  # Enum.each(stream, fn event ->
  #   IO.inspect(event)
  # end)

  ## Public API

  @doc """
  Adds a data source to the market source.

  The `:name` option is optional. If not provided, the name defaults to the number
  of existing data feeds (0 for the first feed, 1 for the second, etc.).

  Currently, only one data feed is supported. An error is raised if you try to add
  a second data feed.

  ## Parameters

    - `market`: The market source.
    - `source`: Either:
      - A module implementing the `TheoryCraft.DataFeed` behaviour
      - An `Enumerable` (list, stream, etc.) containing `Tick` or `Bar` structs
    - `opts`: Options including:
      - `:name` - Optional name for this data stream (default: numeric index)
      - For DataFeed modules: other options are passed to the DataFeed module
      - For enumerables: `:name` is the only relevant option

  ## Examples

      # With DataFeed module and explicit name
      add_data(market, MemoryDataFeed, from: feed, name: "XAUUSD")

      # With DataFeed module and default name (will be 0)
      add_data(market, MemoryDataFeed, from: feed)

      # With enumerable (stream or list)
      ticks = [%Tick{...}, %Tick{...}]
      add_data(market, ticks, name: "XAUUSD")

      # With stream
      stream = Stream.map(ticks, & &1)
      add_data(market, stream, name: "XAUUSD")

  ## Notes

  The `:name` option is used as the data stream identifier and is NOT passed
  to the DataFeed module. All other options are passed through to the DataFeed.

  """
  @spec add_data(t(), module() | Enumerable.t(), Keyword.t()) :: t()
  def add_data(market, source, opts \\ [])

  def add_data(%MarketSource{} = market, data_feed_spec, opts)
      when is_atom(data_feed_spec) or is_tuple(data_feed_spec) do
    %MarketSource{data_feeds: data_feeds, data_streams: data_streams} = market

    if length(data_feeds) > 0 do
      raise ArgumentError, "Currently only one data feed is supported"
    end

    # Normalize the data feed spec
    {data_feed_module, data_feed_opts} = Utils.normalize_spec(data_feed_spec)

    # Default name = number of existing feeds
    name = Keyword.get_lazy(opts, :name, fn -> length(data_feeds) end)
    # Remove :name from opts and merge with data_feed_opts
    feed_opts = opts |> Keyword.delete(:name) |> Keyword.merge(data_feed_opts)

    %MarketSource{
      market
      | data_feeds: [{name, {data_feed_module, feed_opts}}],
        data_streams: [name | data_streams]
    }
  end

  def add_data(%MarketSource{} = market, enumerable, opts) do
    %MarketSource{data_feeds: data_feeds, data_streams: data_streams} = market

    if length(data_feeds) > 0 do
      raise ArgumentError, "Currently only one data feed is supported"
    end

    # Default name = number of existing feeds
    name = Keyword.get_lazy(opts, :name, fn -> length(data_feeds) end)

    %MarketSource{
      market
      | data_feeds: [{name, enumerable}],
        data_streams: [name | data_streams]
    }
  end

  @doc """
  Adds a data feed from a CSV file containing tick data.

  ## Parameters

    - `market`: The market source.
    - `file_path`: The path to the CSV file.
    - `opts`: Optional parameters for the data feed.

  """
  @spec add_data_ticks_from_csv(t(), String.t(), Keyword.t()) :: t()
  def add_data_ticks_from_csv(%MarketSource{} = market, file_path, opts \\ []) do
    data_feed_opts = [file: file_path] ++ opts
    add_data(market, TicksCSVDataFeed, data_feed_opts)
  end

  @doc """
  Resamples the data to a different timeframe.

  Creates a new processor layer with a single TickToBarProcessor.

  ## Default Names

  - If `:data` is not provided, uses the name of the single data feed
  - If `:name` is not provided, generates it as `"{data}_{timeframe}"`

  ## Parameters

    - `market`: The market source.
    - `timeframe`: The new timeframe to resample the data to.
    - `opts`: Optional parameters:
      - `:data` - Source data name (default: data feed name)
      - `:name` - Output data name (default: `"{data}_{timeframe}"`)
      - `:only_bar` - If true, automatically adds aggregation to emit only completed bars (default: false)
      - Other processor options

  ## Important Notes

  When using `:only_bar: true`:
  - Do NOT add more resample layers after this one, as they would receive incomplete data
  - For multiple resamples, use `aggregate_bars/2` after all resamples instead
  - **1-tick lag**: Bars are emitted when the next bar starts. In live trading, the last
    bar of a session will not be emitted until the next session begins (see `aggregate_bars/2`
    for details)

  ## Examples

      # With explicit data and name
      resample(market, "m5", data: "XAUUSD", name: "XAUUSD_m5")

      # With default names (if data feed is named "XAUUSD")
      resample(market, "m5")  # data="XAUUSD", name="XAUUSD_m5"

      # With only_bar to emit only completed bars
      resample(market, "m5", only_bar: true)

  """
  @spec resample(t(), String.t(), Keyword.t()) :: t()
  def resample(%MarketSource{} = market, timeframe, opts \\ []) do
    %MarketSource{
      data_streams: data_streams,
      processor_layers: processor_layers
    } = market

    if not TimeFrame.valid?(timeframe) do
      raise ArgumentError, "Invalid timeframe #{inspect(timeframe)}"
    end

    {only_bar, opts_without_only_bar} = Keyword.pop(opts, :only_bar, false)

    # Deduce :data if not provided (from data feeds)
    data_name =
      Keyword.get_lazy(opts_without_only_bar, :data, fn ->
        fetch_default_data_name(market)
      end)

    # Validate that data_name exists
    if data_name not in data_streams do
      raise ArgumentError, "Data stream #{inspect(data_name)} not found"
    end

    # Generate :name if not provided
    output_name =
      Keyword.get_lazy(opts_without_only_bar, :name, fn ->
        "#{data_name}_#{timeframe}"
      end)

    # Build processor options
    processor_opts =
      opts_without_only_bar
      |> Keyword.put(:timeframe, timeframe)
      |> Keyword.put(:data, data_name)
      |> Keyword.put(:name, output_name)

    processor_spec = {TickToBarProcessor, processor_opts}

    # Add new layer with single processor and track new data stream
    resampled_market =
      %MarketSource{
        market
        | processor_layers: processor_layers ++ [[processor_spec]],
          data_streams: [output_name | data_streams]
      }

    if only_bar do
      aggregate_bars(resampled_market, output_name)
    else
      resampled_market
    end
  end

  @doc """
  Adds a layer with multiple processors running in parallel.

  This creates a layer where multiple processors (indicators) process events
  simultaneously. Events are broadcast to all processors, and their outputs
  are synchronized and merged by an AggregatorStage.

  ## Default Names

  - If `:data` is not provided in processor opts, uses the name of the single data feed
  - If `:name` is not provided, generates it from the module name in snake_case
    (e.g., `TheoryCraft.Indicators.SMA` → `"sma"`)
  - If the generated name already exists, adds a numeric suffix (`"sma_1"`, `"sma_2"`, etc.)

  ## Parameters

    - `market`: The market source.
    - `processor_specs`: List of `{module, opts}` tuples for each processor.
    - `opts`: Optional parameters (e.g., `:concurrency` - currently unused).

  ## Examples

      require TheoryCraftTA.TA, as: TA

      # With explicit data and names
      market
      |> add_indicators_layer([
        TA.volume(XAUUSD_m5[:volume], name: "volume"),
        TA.sma(XAUUSD_m5[:close], 20, name: "sma_20")
      ])

      # With default names (auto-generated)
      market
      |> add_indicators_layer([
        TA.volume(XAUUSD_m5[:volume]),
        TA.sma(XAUUSD_m5[:close], 20)
      ])

  """
  @spec add_indicators_layer(t(), [Indicator.spec()], Keyword.t()) :: t()
  def add_indicators_layer(%MarketSource{} = market, indicator_specs, _opts \\ [])
      when is_list(indicator_specs) do
    %MarketSource{data_streams: data_streams, processor_layers: processor_layers} = market

    if indicator_specs == [] do
      raise ArgumentError, "indicator_specs cannot be empty"
    end

    # Process each indicator spec: add defaults, generate names, validate
    {enhanced_specs, new_data_names} =
      Enum.map_reduce(indicator_specs, [], fn indicator_spec, generated_names ->
        {module, indicator_opts} = Utils.normalize_spec(indicator_spec)

        # Deduce :data if not provided
        data_name =
          Keyword.get_lazy(indicator_opts, :data, fn ->
            fetch_default_data_name(market)
          end)

        # Validate that data source exists
        if data_name not in data_streams do
          raise ArgumentError, "Data stream #{inspect(data_name)} not found"
        end

        # Generate :name if not provided
        output_name =
          Keyword.get_lazy(indicator_opts, :name, fn ->
            generate_indicator_name(module, data_streams, generated_names)
          end)

        # Validate that the name is not already taken
        all_taken_names = data_streams ++ generated_names

        if output_name in all_taken_names do
          raise ArgumentError,
                "Data stream name #{inspect(output_name)} is already taken. " <>
                  "Please provide a unique :name option."
        end

        # Add :data and :name to opts
        enhanced_opts =
          indicator_opts
          |> Keyword.put_new(:data, data_name)
          |> Keyword.put_new(:name, output_name)

        # Wrap indicator in IndicatorProcessor
        processor_spec = {IndicatorProcessor, Keyword.put(enhanced_opts, :module, module)}

        {processor_spec, generated_names ++ [output_name]}
      end)

    # Add new layer with multiple processors and track new data streams
    %MarketSource{
      market
      | processor_layers: processor_layers ++ [enhanced_specs],
        data_streams: new_data_names ++ data_streams
    }
  end

  @doc """
  Adds a single indicator/processor as a new layer.

  This is a convenience function that creates a layer with a single processor.
  Equivalent to `add_indicators_layer(market, [{module, opts}])`.

  ## Default Names

  - If `:data` is not provided, uses the name of the single data feed
  - If `:name` is not provided, generates it from the module name in snake_case
    (e.g., `TheoryCraft.Indicators.SMA` → `"sma"`)
  - If the generated name already exists, adds a numeric suffix (`"sma_1"`, `"sma_2"`, etc.)

  ## Parameters

    - `market`: The market source.
    - `processor_module`: The processor/indicator module.
    - `opts`: Options for the processor.

  ## Examples

      require TheoryCraftTA.TA, as: TA

      # With explicit name
      market
      |> add_indicator(TA.sma(volume[:value], 14, name: "sma_14"))

      # With default name (auto-generated from indicator type)
      market
      |> add_indicator(TA.sma(eurusd_m5[:close], 14))

      # Multiple indicators with different periods
      market
      |> add_indicator(TA.sma(eurusd_m5[:close], 14))
      |> add_indicator(TA.sma(eurusd_m5[:close], 20))
      |> add_indicator(TA.sma(eurusd_m5[:close], 50))

  """
  @spec add_indicator(t(), module(), Keyword.t()) :: t()
  def add_indicator(%MarketSource{} = market, processor_module, opts) do
    %MarketSource{data_streams: data_streams, processor_layers: processor_layers} = market

    # Deduce :data if not provided
    data_name =
      Keyword.get_lazy(opts, :data, fn ->
        fetch_default_data_name(market)
      end)

    # Validate data source
    if data_name not in data_streams do
      raise ArgumentError, "Data stream #{inspect(data_name)} not found"
    end

    # Generate :name if not provided
    output_name =
      Keyword.get_lazy(opts, :name, fn ->
        generate_indicator_name(processor_module, data_streams)
      end)

    # Validate that the name is not already taken
    if output_name in data_streams do
      raise ArgumentError,
            "Data stream name #{inspect(output_name)} is already taken. " <>
              "Please provide a unique :name option."
    end

    # Add :data to opts if not present
    enhanced_opts =
      opts
      |> Keyword.put_new(:data, data_name)
      |> Keyword.put_new(:name, output_name)

    # Wrap indicator in IndicatorProcessor
    processor_spec = {IndicatorProcessor, Keyword.put(enhanced_opts, :module, processor_module)}

    # Add new layer with single processor and track new data stream
    %MarketSource{
      market
      | processor_layers: processor_layers ++ [[processor_spec]],
        data_streams: [output_name | data_streams]
    }
  end

  @doc """
  Aggregates bar events by only emitting completed bars.

  This function adds a BarAggregatorStage that filters out intra-bar tick events,
  emitting only when bars are complete (i.e., when `new_bar? = true`). This is useful
  for avoiding redundant indicator calculations on incomplete bars.

  ## Parameters

    - `market`: The market source.
    - `bar_names`: A single bar name (string) or list of bar names to aggregate.

  ## Returns

    - The updated market source with the aggregation layer added.

  ## Behavior

  The aggregator emits events when at least one of the tracked bars becomes complete
  (OR logic). Each completed bar is emitted with its original `new_bar?` and
  `new_market?` flags from the tick that created it.

  ## Important Notes

  - This should be used AFTER all resampling operations are complete
  - Do NOT add more resample layers after aggregation, as the data would be incomplete
  - For a single resample with aggregation, you can use `resample("m5", only_bar: true)` instead

  ## 1-Tick Lag Warning

  Bar aggregation introduces a **1-tick lag** in emissions. A bar is only emitted when
  the next bar starts. This means:

  - The last bar before stream end will only be emitted when the stream terminates
  - **In live/real-time trading**: The last bar of a session (e.g., daily close) will
    NOT be emitted until the next session starts (e.g., next day's market open)
  - For backtesting, this is usually not an issue as streams end naturally
  - For live trading, you may need a timeout mechanism or manual flush to access the
    latest incomplete bar immediately

  ## Examples

      # Aggregate a single bar
      market
      |> MarketSource.resample("m5", name: "data_m5")
      |> MarketSource.aggregate_bars("data_m5")

      # Aggregate multiple bars
      market
      |> MarketSource.resample("m5", name: "data_m5")
      |> MarketSource.resample("h1", name: "data_h1")
      |> MarketSource.aggregate_bars(["data_m5", "data_h1"])

      # With indicators after aggregation
      market
      |> MarketSource.resample("m5", name: "data_m5")
      |> MarketSource.aggregate_bars("data_m5")
      |> MarketSource.add_indicator(TA.sma(data_m5[:close], 20))

  """
  @spec aggregate_bars(t(), String.t() | [String.t()]) :: t()
  def aggregate_bars(%MarketSource{} = market, bar_name_or_bar_names) do
    %MarketSource{data_streams: data_streams, processor_layers: processor_layers} = market

    bar_names = List.wrap(bar_name_or_bar_names)

    if bar_names == [] do
      raise ArgumentError, "bar_names cannot be empty"
    end

    for bar_name <- bar_names do
      if bar_name not in data_streams do
        raise ArgumentError, "Data stream #{inspect(bar_name)} not found"
      end
    end

    aggregator_spec = {BarAggregatorStage, [bar_names: bar_names]}

    %MarketSource{market | processor_layers: processor_layers ++ [[aggregator_spec]]}
  end

  @doc """
  Adds a trading strategy to the market source.

  Multiple strategies can be added to the market source. Each strategy can have its own
  configuration options.

  **Note**: Strategy execution is not yet implemented.

  ## Parameters

    - `market`: The market source.
    - `strategy_module`: The strategy module to use.
    - `opts`: Optional parameters for the strategy (default: `[]`).

  ## Examples

      # Add strategy without options
      add_strategy(market, MyStrategy)

      # Add strategy with options
      add_strategy(market, MyStrategy, risk_level: :high, max_positions: 5)

      # Add multiple strategies
      market
      |> add_strategy(Strategy1, param1: 100)
      |> add_strategy(Strategy2, param2: 200)

  """
  @spec add_strategy(t(), module() | {module(), Keyword.t()}, Keyword.t()) :: t()
  def add_strategy(%MarketSource{} = market, strategy_spec, opts \\ [])
      when is_atom(strategy_spec) or is_tuple(strategy_spec) do
    %MarketSource{strategies: strategies} = market
    # When opts is provided, merge them with the spec opts
    {strategy_module, spec_opts} = Utils.normalize_spec(strategy_spec)
    strategy_opts = Keyword.merge(spec_opts, opts)

    %MarketSource{market | strategies: strategies ++ [{strategy_module, strategy_opts}]}
  end

  @doc """
  Materializes the market source into a GenStage pipeline and returns an Enumerable stream.

  This function:
  1. Starts a DataFeedStage producer from the data feed spec
  2. For each processor layer:
     - Single processor: starts a ProcessorStage
     - Multiple processors: starts BroadcastDispatcher → N ProcessorStages → AggregatorStage
  3. Returns `GenStage.stream/1` of the final stage

  ## Parameters

    - `market`: The market source.
    - `opts`: Optional parameters (currently unused).

  ## Returns

  An Enumerable that yields MarketEvents.

  ## Examples

      market
      |> MarketSource.stream()
      |> Enum.take(100)

  """
  @spec stream(t(), Keyword.t()) :: Enumerable.t(MarketEvent.t())
  def stream(%MarketSource{} = market, _opts \\ []) do
    %MarketSource{data_feeds: data_feeds} = market

    if data_feeds == [] do
      raise ArgumentError,
            "No data feed configured. Use add_data/3 or add_data_ticks_from_csv/3 first."
    end

    # Materialize the GenStage pipeline
    {data_feed_pid, final_stage_pid} = materialize_pipeline(market)

    # Return GenStage stream with producers specified
    GenStage.stream([{final_stage_pid, cancel: :transient}], producers: [data_feed_pid])
  end

  ## Private functions

  # Generates a unique indicator name based on the module
  # Returns a name in snake_case format, with a numeric suffix if there are collisions
  defp generate_indicator_name(module, existing_names, already_generated \\ []) do
    # Extract module name and convert to snake_case
    base_name =
      module
      |> Module.split()
      |> List.last()
      |> Macro.underscore()

    all_taken_names = existing_names ++ already_generated

    # Find a unique name by adding suffix if needed
    find_unique_name(base_name, all_taken_names, 0)
  end

  # Recursively finds a unique name by adding numeric suffixes
  defp find_unique_name(base_name, taken_names, 0) do
    if base_name in taken_names do
      find_unique_name(base_name, taken_names, 1)
    else
      base_name
    end
  end

  defp find_unique_name(base_name, taken_names, suffix) do
    candidate = "#{base_name}_#{suffix}"

    if candidate in taken_names do
      find_unique_name(base_name, taken_names, suffix + 1)
    else
      candidate
    end
  end

  # Fetches the default data name from data feeds
  # Returns the name of the single data feed, or raises if none or multiple
  defp fetch_default_data_name(%MarketSource{data_feeds: data_feeds}) do
    case data_feeds do
      [{name, _feed_spec}] ->
        name

      [] ->
        raise ArgumentError, "No data feeds available"

      _ ->
        raise ArgumentError,
              "Multiple data feeds found, please specify :data option explicitly"
    end
  end

  # Materializes the GenStage pipeline from specs
  defp materialize_pipeline(%MarketSource{} = market) do
    %MarketSource{data_feeds: data_feeds, processor_layers: processor_layers} = market

    # Extract the single data feed source (can be {module, opts} or enumerable)
    [{data_name, source}] = data_feeds

    # Start DataFeedStage producer with demand: :accumulate
    {:ok, data_feed_pid} = DataFeedStage.start_link(source, name: data_name)

    # Build pipeline left to right
    final_stage_pid =
      Enum.reduce(processor_layers, data_feed_pid, fn layer, upstream_pid ->
        materialize_layer(layer, upstream_pid)
      end)

    {data_feed_pid, final_stage_pid}
  end

  # Materialize a single processor layer
  defp materialize_layer([{BarAggregatorStage, opts}], upstream_pid) do
    # BarAggregatorStage - start with bar_names option
    {:ok, aggregator_pid} =
      BarAggregatorStage.start_link(
        Keyword.merge(opts, subscribe_to: [{upstream_pid, cancel: :transient}])
      )

    aggregator_pid
  end

  defp materialize_layer([processor_spec], upstream_pid) do
    # Single processor - start ProcessorStage with subscription
    {:ok, processor_pid} =
      ProcessorStage.start_link(
        processor_spec,
        subscribe_to: [{upstream_pid, cancel: :transient}]
      )

    processor_pid
  end

  defp materialize_layer(processor_specs, upstream_pid)
       when is_list(processor_specs) and length(processor_specs) > 1 do
    # Multiple processors - need broadcast → N processors → aggregator

    # 1. Start BroadcastStage
    {:ok, broadcast_pid} =
      BroadcastStage.start_link(subscribe_to: [{upstream_pid, cancel: :transient}])

    # 2. Start N ProcessorStages first
    processor_pids =
      Enum.map(processor_specs, fn processor_spec ->
        {:ok, processor_pid} =
          ProcessorStage.start_link(
            processor_spec,
            subscribe_to: [{broadcast_pid, cancel: :transient}]
          )

        processor_pid
      end)

    # 3. Start AggregatorStage with all ProcessorStages in subscribe_to
    subscriptions = Enum.map(processor_pids, fn pid -> {pid, cancel: :transient} end)

    {:ok, aggregator_pid} =
      AggregatorStage.start_link(
        producer_count: length(processor_specs),
        subscribe_to: subscriptions
      )

    # Return aggregator as the final stage of this layer
    aggregator_pid
  end
end
