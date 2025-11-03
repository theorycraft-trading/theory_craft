defmodule TheoryCraft.MarketSource.DataFeedStage do
  @moduledoc """
  A GenStage producer that wraps a `TheoryCraft.DataFeed` behaviour.

  This stage acts as a producer in a GenStage pipeline, converting a DataFeed's
  enumerable stream into a demand-driven data source with backpressure support.

  The stage wraps each tick or bar from the data source in a `MarketEvent` struct,
  associating it with a data stream name. This allows downstream processors to identify
  and process data from different sources.

  ## Implementation

  This module uses `GenStage.from_enumerable/2` to create a GenStage producer from
  the DataFeed's stream, providing immediate GenStage integration with backpressure
  support and demand-driven processing.

  ## Examples

      # Start with DataFeed module and options tuple
      {:ok, stage} = DataFeedStage.start_link(
        {TicksCSVDataFeed, [file: "ticks.csv"]},
        name: "XAUUSD"
      )

      # Start with DataFeed module only (no feed options)
      {:ok, stage} = DataFeedStage.start_link(
        MemoryDataFeed,
        name: "XAUUSD"
      )

      # Start with Enumerable directly
      {:ok, stage} = DataFeedStage.start_link(
        [tick1, tick2, tick3],
        name: "XAUUSD"
      )

      # With named stage
      {:ok, stage} = DataFeedStage.start_link(
        {TicksCSVDataFeed, [file: "ticks.csv"]},
        name: "XAUUSD",
        stage_name: :my_feed
      )

  ## Source Parameter

  The first parameter can be one of:
    - A tuple `{feed_module, feed_opts}` where `feed_module` implements the `TheoryCraft.DataFeed` behaviour
    - A module implementing the `TheoryCraft.DataFeed` behaviour (equivalent to `{feed_module, []}`)
    - An `Enumerable` (e.g., list, stream) containing ticks or bars

  ## Options

  The second parameter is a keyword list with:
    - `:name` - Name for the data stream in MarketEvent (required)
    - `:stage_name` - Register the stage with a name (optional)
    - `:max_demand` - Maximum demand for the dispatcher (default: 1000)
  """

  require Logger

  alias TheoryCraft.MarketSource.{DataFeed, MarketEvent, StageHelpers}
  alias TheoryCraft.Utils

  @typedoc """
  Options for starting a DataFeedStage.

  Stage-specific options:
  - `:name` - Name for the data stream
  - `:stage_name` - Optional name for the GenStage process

  GenStage/GenServer options:
  - `:timeout` - Timeout for GenServer.start_link
  - `:debug` - Debug options
  - `:spawn_opt` - Options for spawning the process
  - `:hibernate_after` - Hibernate after inactivity

  Producer options:
  - `:max_demand` - Maximum demand from consumers
  - `:buffer_size` - Size of the buffer (default: 10000)
  - `:buffer_keep` - Whether to keep `:first` or `:last` (default: `:last`)
  """
  @type start_option ::
          {:name, String.t()}
          | {:stage_name, atom()}
          | {:timeout, timeout()}
          | {:debug, Keyword.t()}
          | {:spawn_opt, Process.spawn_opt()}
          | {:hibernate_after, timeout()}
          | {:max_demand, pos_integer()}
          | {:buffer_size, non_neg_integer()}
          | {:buffer_keep, :first | :last}

  ## Public API

  @doc """
  Starts a DataFeedStage as a GenStage producer.

  This function creates a GenStage producer from a DataFeed by calling the feed's
  `stream/1` function and wrapping the result with `GenStage.from_enumerable/2`.

  ## Parameters

    - `source` - Can be one of:
      - A tuple `{feed_module, feed_opts}` where `feed_module` implements the `TheoryCraft.DataFeed` behaviour
      - A module implementing the `TheoryCraft.DataFeed` behaviour (equivalent to `{feed_module, []}`)
      - An `Enumerable` (e.g., list, stream)
    - `opts` - Keyword list of options:
      - `:name` - Name for the data stream in MarketEvent (required)
      - `:stage_name` - Register the stage with a name (optional)
      - `:max_demand` - Maximum demand for the dispatcher (default: 1000)

  ## Returns

    - `{:ok, pid}` on success
    - `{:error, reason}` on failure

  ## Examples

      # With DataFeed module and options
      {:ok, stage} = DataFeedStage.start_link(
        {TicksCSVDataFeed, [file: "data.csv"]},
        name: "XAUUSD"
      )

      # With DataFeed module only (no feed options)
      {:ok, stage} = DataFeedStage.start_link(
        MemoryDataFeed,
        name: "XAUUSD"
      )

      # With Enumerable directly
      {:ok, stage} = DataFeedStage.start_link(
        [tick1, tick2, tick3],
        name: "XAUUSD"
      )

      # With named stage
      {:ok, stage} = DataFeedStage.start_link(
        {TicksCSVDataFeed, [file: "data.csv"]},
        name: "XAUUSD",
        stage_name: :my_feed,
        max_demand: 500
      )

  """
  @spec start_link(DataFeed.spec() | Enumerable.t(), [start_option()]) :: GenServer.on_start()
  def start_link(source, opts \\ [])

  def start_link(feed_spec, opts) when is_atom(feed_spec) or is_tuple(feed_spec) do
    {feed_module, feed_opts} = Utils.normalize_spec(feed_spec)

    case feed_module.stream(feed_opts) do
      {:ok, stream} ->
        Logger.debug("DataFeedStage starting with feed_module=#{inspect(feed_module)}")
        do_start_link(stream, opts)

      {:error, reason} ->
        Logger.error("DataFeedStage: Failed to create stream: #{inspect(reason)}")
        {:error, {:data_feed_error, reason}}
    end
  end

  def start_link(enumerable, opts) do
    Logger.debug("DataFeedStage starting with enumerable")
    do_start_link(enumerable, opts)
  end

  ## Private functions

  defp do_start_link(enumerable, opts) do
    name = Keyword.fetch!(opts, :name)
    max_demand = Keyword.get(opts, :max_demand, 1000)
    stage_name = Keyword.get(opts, :stage_name)

    # Extract GenStage/GenServer options, excluding :name which is for data stream name
    gen_stage_opts =
      opts
      |> Keyword.drop([:name, :max_demand])
      |> StageHelpers.extract_gen_stage_opts()

    gen_stage_opts =
      if stage_name do
        Keyword.put(gen_stage_opts, :name, stage_name)
      else
        gen_stage_opts
      end

    Logger.debug("name=#{name}, max_demand=#{max_demand}")

    # Wrap each event in a MarketEvent struct
    wrapped_stream =
      Stream.map(enumerable, fn event ->
        # Should be a Tick or Bar here
        %{time: time} = event

        %MarketEvent{
          time: time,
          source: name,
          data: %{name => event}
        }
      end)

    GenStage.from_enumerable(
      wrapped_stream,
      Keyword.merge(gen_stage_opts,
        on_cancel: :stop,
        demand: :accumulate,
        dispatcher: {GenStage.DemandDispatcher, max_demand: max_demand}
      )
    )
  end
end
