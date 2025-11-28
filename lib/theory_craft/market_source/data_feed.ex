defmodule TheoryCraft.MarketSource.DataFeed do
  @moduledoc ~S"""
  Behaviour for data sources that provide streams of market data.

  A DataFeed is responsible for providing a continuous, ordered stream of `Tick` or `Bar`
  structs representing historical or real-time market data. Data feeds are the entry point
  for market data into TheoryCraft's processing pipeline.

  ## Characteristics

  - **Streaming**: Data feeds provide lazy enumerables that can handle large datasets efficiently
  - **Ordered**: Data must be ordered by time (ascending), as many processors depend on chronological order
  - **Type-consistent**: A feed should emit either Ticks or Bars, not mixed types
  - **Configurable**: Feeds accept options to customize data source, filtering, etc.

  ## Built-in DataFeed Implementations

  - `TheoryCraft.MarketSource.MemoryDataFeed` - In-memory ETS-based storage for testing and caching
  - `TheoryCraft.MarketSource.TicksCSVDataFeed` - Reads tick data from CSV files

  ## Implementing a DataFeed

  To create a custom data feed, use the `TheoryCraft.MarketSource.DataFeed` module and implement
  the `stream/1` callback:

      defmodule MyDataFeed do
        use TheoryCraft.MarketSource.DataFeed

        @impl true
        def stream(opts) do
          symbol = Keyword.fetch!(opts, :symbol)
          start_date = Keyword.get(opts, :start_date)

          case load_data(symbol, start_date) do
            {:ok, data} ->
              stream = Stream.map(data, &parse_tick/1)
              {:ok, stream}

            {:error, reason} ->
              {:error, reason}
          end
        end
      end

  The `use TheoryCraft.MarketSource.DataFeed` macro automatically provides a default `stream!/1`
  implementation that raises on errors.

  ## Usage with MarketSource

  Data feeds are typically used as the starting point for a `MarketSource` pipeline:

      {:ok, feed_stream} = MyDataFeed.stream(symbol: "EURUSD", start_date: ~D[2024-01-01])

      market =
        %MarketSource{}
        |> MarketSource.add_data(feed_stream, name: "eurusd_ticks")
        |> MarketSource.add_processor(ResampleProcessor, data: "eurusd_ticks", timeframe: "m5")
        |> MarketSource.stream()

      # Process the data
      Enum.each(market, fn event ->
        # Handle each market event
      end)

  ## Examples

      # Using a data feed with error handling
      case MyDataFeed.stream(symbol: "EURUSD") do
        {:ok, stream} ->
          stream |> Enum.take(10) |> inspect_data()

        {:error, reason} ->
          Logger.error("Failed to create stream: #{inspect(reason)}")
      end

      # Using the bang version for simpler code (raises on error)
      stream = MyDataFeed.stream!(symbol: "EURUSD")
      stream |> Enum.take(10) |> inspect_data()

  ## Time Ordering

  DataFeeds **must** provide data in ascending chronological order. This is critical because:

  - Processors like `ResampleProcessor` assume time-ordered data for bar boundary detection
  - Strategies depend on receiving events in the order they occurred historically
  - The `MarketSource` does not perform any sorting or time validation

  If your data source does not guarantee ordering, you must sort the data before yielding it.
  """

  alias TheoryCraft.MarketSource.{Bar, Tick}

  @typedoc """
  A DataFeed specification as a tuple of module and options, or just a module.

  When only the module is provided, options is an empty list.
  """
  @type spec :: {module(), Keyword.t()} | module()

  @typedoc """
  A stream of market data (Ticks or Bars).

  The stream must be:
  - Lazy (using `Stream` functions)
  - Time-ordered (ascending)
  - Type-consistent (all Ticks or all Bars)
  """
  @type stream :: Enumerable.t(Tick.t() | Bar.t())

  @doc ~S"""
  Creates a stream of market data with the given options.

  This callback should establish a connection to the data source (file, database, API, etc.),
  configure any necessary filters or transformations, and return a lazy enumerable stream
  of `Tick` or `Bar` structs.

  ## Parameters

    - `opts` - Keyword list of options for configuring the data feed. Common options include:
      - `:symbol` - The market symbol (e.g., "EURUSD", "BTCUSD")
      - `:start_date` / `:end_date` - Date range for historical data
      - `:source` - Path to file, database connection, etc.

      Specific implementations may define additional options.

  ## Returns

    - `{:ok, stream}` - A stream of `Tick` or `Bar` structs ordered by time
    - `{:error, reason}` - An error tuple if the stream cannot be created

  ## Examples

      # Simple file-based data feed
      @impl true
      def stream(opts) do
        path = Keyword.fetch!(opts, :path)

        case File.read(path) do
          {:ok, contents} ->
            stream = contents
            |> String.split("\\n")
            |> Stream.map(&parse_line/1)
            |> Stream.reject(&is_nil/1)

            {:ok, stream}

          {:error, reason} ->
            {:error, "Failed to read file: #{reason}"}
        end
      end

      # Database-based data feed with filtering
      @impl true
      def stream(opts) do
        symbol = Keyword.fetch!(opts, :symbol)
        start_date = Keyword.get(opts, :start_date)
        end_date = Keyword.get(opts, :end_date)

        query = build_query(symbol, start_date, end_date)

        case Database.query(query) do
          {:ok, rows} ->
            stream = Stream.map(rows, &row_to_tick/1)
            {:ok, stream}

          {:error, reason} ->
            {:error, reason}
        end
      end

      # In-memory data feed
      @impl true
      def stream(opts) do
        ticks = Keyword.fetch!(opts, :ticks)

        # Ensure data is sorted by time
        sorted_ticks = Enum.sort_by(ticks, & &1.time, DateTime)
        stream = Stream.map(sorted_ticks, & &1)

        {:ok, stream}
      end

  """
  @callback stream(Keyword.t()) :: {:ok, stream()} | {:error, any()}

  @doc """
  Creates a stream of market data, raising on error.

  This is a convenience function that wraps `stream/1` and raises an `ArgumentError`
  if the stream cannot be created. This is useful when you want to fail fast rather
  than handle errors explicitly.

  A default implementation is provided when you `use TheoryCraft.MarketSource.DataFeed`, but it
  can be overridden if needed.

  ## Parameters

    - `opts` - Keyword list of options (same as `stream/1`)

  ## Returns

    - A stream of `Tick` or `Bar` structs ordered by time

  ## Raises

    - `ArgumentError` if the stream cannot be created

  ## Examples

      # Using stream! for simpler code
      stream = MyDataFeed.stream!(path: "data.csv")
      Enum.each(stream, &process_tick/1)

      # This will raise if the file doesn't exist
      stream = MyDataFeed.stream!(path: "nonexistent.csv")
      # ** (ArgumentError) Failed to create stream: :enoent

  """
  @callback stream!(Keyword.t()) :: stream()

  ## Public API

  @doc false
  defmacro __using__(_opts) do
    quote do
      @behaviour TheoryCraft.MarketSource.DataFeed

      @impl true
      def stream!(opts) do
        case stream(opts) do
          {:ok, stream} -> stream
          {:error, reason} -> raise ArgumentError, "Failed to create stream: #{inspect(reason)}"
        end
      end

      defoverridable stream!: 1
    end
  end
end
