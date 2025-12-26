defmodule TheoryCraft.TimeSeries do
  @moduledoc """
  A time-indexed series for storing time series data with DateTime keys.

  `TimeSeries` is a wrapper around `DataSeries` that maintains a synchronized list
  of `DateTime` values alongside the data values. It provides efficient access to
  time series data both by index and by DateTime, while enforcing chronological order.

  ## Key Features

  - **Chronologically ordered**: Values must be added in chronological order (newest last)
  - **Dual access**: Access by integer index or by DateTime
  - **Read-optimized**: Fast access to recent values using list head operations
  - **Access protocol**: Supports bracket syntax with both index and DateTime
  - **Enumerable protocol**: Enumerates over `{datetime, value}` tuples

  ## Use Cases

  TimeSeries is ideal for:
  - Storing market data (ticks, bars) with timestamps
  - Maintaining time-indexed technical indicators
  - Time series analysis where temporal lookups are needed

  ## Chronological Order Enforcement

  - `add/3`: Raises if the new datetime is <= the last datetime
  - `update/3`: Raises if the new datetime is < the last datetime, updates if ==, inserts if >

  ## Access Protocol

  TimeSeries implements the `Access` behaviour with dual index support:

  - With integer index: Returns the value only (positional lookup)
  - With DateTime: Returns the value only (temporal lookup)
  - `get_and_update/3` supports both index types
  - `pop/2`: Always raises an error (popping is not supported)

  ## Enumerable Protocol

  TimeSeries implements the `Enumerable` protocol, enumerating over `{datetime, value}` tuples.

  ## Performance Characteristics

  - **Add/Update**: O(1) for adding/updating the most recent value
  - **Access by index**: O(n) where n is the index
  - **Access by DateTime**: O(n) linear search
  - **Last**: O(1) (head access)
  - **Size**: O(1)

  ## Examples

      # Create an empty TimeSeries
      iex> ts = TimeSeries.new()
      iex> TimeSeries.size(ts)
      0

      # Add values with timestamps
      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.size(ts)
      2
      iex> ts[0]
      101.0
      iex> ts[~U[2024-01-01 10:00:00Z]]
      100.0

      # Update existing timestamp
      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = TimeSeries.update(ts, ~U[2024-01-01 10:00:00Z], 105.0)
      iex> ts[0]
      105.0

      # Using Enum functions
      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> Enum.map(ts, fn {_dt, value} -> value * 2 end)
      [202.0, 200.0]

  """

  alias TheoryCraft.DataSeries
  alias __MODULE__

  @behaviour Access

  @enforce_keys [:data, :dt]
  defstruct @enforce_keys

  @type t :: %TimeSeries{data: DataSeries.t(), dt: [DateTime.t()]}
  @type t(value_type) :: %TimeSeries{data: DataSeries.t(value_type), dt: [DateTime.t()]}

  ## Public API

  @doc """
  Creates a new empty TimeSeries.

  ## Options

    - `:max_size` - Maximum number of values to store. When this limit is reached,
      the oldest value is dropped when adding new values. Defaults to `:infinity`.

  ## Returns

    - A new `TimeSeries` struct with empty data and no timestamps

  ## Examples

      iex> ts = TimeSeries.new()
      iex> TimeSeries.size(ts)
      0

      iex> ts = TimeSeries.new(max_size: 100)
      iex> ts.data.max_size
      100

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %TimeSeries{
      data: DataSeries.new(opts),
      dt: []
    }
  end

  @doc """
  Adds a new value with its timestamp to the TimeSeries.

  The new value must have a datetime that is strictly greater than the last datetime
  in the series. If you need to update an existing timestamp, use `update/3` instead.

  ## Parameters

    - `series` - The TimeSeries to add the value to
    - `datetime` - The DateTime timestamp for this value
    - `value` - The value to add (can be any type)

  ## Returns

    - A new `TimeSeries` with the value added

  ## Raises

    - `ArgumentError` if datetime <= last datetime in the series

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.size(ts)
      2

      # Raises if datetime is not strictly increasing
      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> TimeSeries.add(ts, ~U[2024-01-01 10:00:00Z], 105.0)
      ** (ArgumentError) datetime must be strictly greater than the last datetime. Got 2024-01-01T10:00:00Z, last was 2024-01-01T10:00:00Z. Use update/3 if you want to update an existing timestamp.

  """
  @spec add(t(), DateTime.t(), any()) :: t()
  def add(%TimeSeries{data: data, dt: dt}, datetime, value) do
    case dt do
      [] ->
        %TimeSeries{
          data: DataSeries.add(data, value),
          dt: [datetime]
        }

      [last_dt | _] ->
        case DateTime.compare(datetime, last_dt) do
          :gt ->
            insert_value(data, dt, datetime, value)

          _ ->
            raise ArgumentError,
                  "datetime must be strictly greater than the last datetime. " <>
                    "Got #{DateTime.to_iso8601(datetime)}, " <>
                    "last was #{DateTime.to_iso8601(last_dt)}. " <>
                    "Use update/3 if you want to update an existing timestamp."
        end
    end
  end

  @doc """
  Updates or inserts a value at the given timestamp.

  This function behaves differently based on the datetime comparison:
  - If datetime > last datetime: inserts the new value (like `add/3`)
  - If datetime == last datetime: updates the existing value
  - If datetime < last datetime: raises an error

  ## Parameters

    - `series` - The TimeSeries to update
    - `datetime` - The DateTime timestamp for this value
    - `value` - The value to update or insert

  ## Returns

    - A new `TimeSeries` with the value updated or inserted

  ## Raises

    - `ArgumentError` if datetime < last datetime in the series

  ## Examples

      # Insert new value when datetime > last
      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.update(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = TimeSeries.update(ts, ~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.size(ts)
      2

      # Update existing value when datetime == last
      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.update(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = TimeSeries.update(ts, ~U[2024-01-01 10:00:00Z], 105.0)
      iex> ts[0]
      105.0

      # Raises if datetime < last
      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.update(ts, ~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.update(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      ** (ArgumentError) datetime must be >= the last datetime. Got 2024-01-01T10:00:00Z, last was 2024-01-01T10:01:00Z.

  """
  @spec update(t(), DateTime.t(), any()) :: t()
  def update(%TimeSeries{data: data, dt: dt}, datetime, value) do
    case dt do
      [] ->
        %TimeSeries{
          data: DataSeries.add(data, value),
          dt: [datetime]
        }

      [last_dt | _rest_dt] ->
        case DateTime.compare(datetime, last_dt) do
          :gt ->
            insert_value(data, dt, datetime, value)

          :eq ->
            # Update existing value at head - optimized with replace_at
            new_data = DataSeries.replace_at(data, 0, value)
            %TimeSeries{data: new_data, dt: dt}

          :lt ->
            raise ArgumentError,
                  "datetime must be >= the last datetime. " <>
                    "Got #{DateTime.to_iso8601(datetime)}, " <>
                    "last was #{DateTime.to_iso8601(last_dt)}."
        end
    end
  end

  @doc """
  Returns the most recent value in the TimeSeries.

  ## Parameters

    - `series` - The TimeSeries to get the last value from

  ## Returns

    - The most recent value
    - `nil` if the TimeSeries is empty

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.last(ts)
      101.0

      iex> ts = TimeSeries.new()
      iex> TimeSeries.last(ts)
      nil

  """
  @spec last(t()) :: any() | nil
  def last(%TimeSeries{data: data}) do
    DataSeries.last(data)
  end

  @doc """
  Returns the current number of values in the TimeSeries.

  ## Parameters

    - `series` - The TimeSeries to get the size of

  ## Returns

    - A non-negative integer representing the current number of stored values

  ## Examples

      iex> ts = TimeSeries.new()
      iex> TimeSeries.size(ts)
      0

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.size(ts)
      2

  """
  @spec size(t()) :: non_neg_integer()
  def size(%TimeSeries{data: data}) do
    DataSeries.size(data)
  end

  @doc """
  Returns the value at the given index or DateTime.

  ## Parameters

    - `series` - The TimeSeries to get the value from
    - `key` - Can be:
      - Integer index where 0 is newest, 1 is second newest, etc.
      - Negative index where -1 is oldest, -2 is second oldest, etc.
      - DateTime for exact datetime match

  ## Returns

    - The value if the key is found
    - `nil` if the key is not found or out of bounds

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.at(ts, 0)
      101.0
      iex> TimeSeries.at(ts, 1)
      100.0
      iex> TimeSeries.at(ts, -1)
      100.0
      iex> TimeSeries.at(ts, ~U[2024-01-01 10:00:00Z])
      100.0
      iex> TimeSeries.at(ts, ~U[2024-01-01 11:00:00Z])
      nil

  """
  @spec at(t(), integer() | DateTime.t()) :: any() | nil
  def at(%TimeSeries{data: data}, index) when is_integer(index) do
    DataSeries.at(data, index)
  end

  def at(%TimeSeries{data: data, dt: dt}, %DateTime{} = datetime) do
    case find_datetime_index(dt, datetime) do
      nil -> nil
      index -> DataSeries.at(data, index)
    end
  end

  @doc """
  Replaces the value at the given index or datetime.

  This function replaces the value at the specified index or datetime with a new value.
  It is optimized for replacing the head element (index 0 or most recent datetime).

  ## Parameters

    - `series` - The TimeSeries to update
    - `key` - Can be:
      - Integer index: 0 is newest, 1 is second newest, etc.
      - DateTime: exact datetime match
    - `value` - The new value to set

  ## Returns

    - A new `TimeSeries` with the value replaced
    - Returns the original series if the key is not found

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> ts = TimeSeries.replace_at(ts, 0, 999.0)
      iex> ts[0]
      999.0

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = TimeSeries.replace_at(ts, ~U[2024-01-01 10:00:00Z], 999.0)
      iex> ts[~U[2024-01-01 10:00:00Z]]
      999.0

  """
  @spec replace_at(t(), integer() | DateTime.t(), any()) :: t()
  def replace_at(%TimeSeries{data: data} = series, index, value) when is_integer(index) do
    new_data = DataSeries.replace_at(data, index, value)
    %TimeSeries{series | data: new_data}
  end

  def replace_at(%TimeSeries{data: data, dt: dt} = series, %DateTime{} = datetime, value) do
    case find_datetime_index(dt, datetime) do
      nil ->
        series

      index ->
        new_data = DataSeries.replace_at(data, index, value)
        %TimeSeries{series | data: new_data}
    end
  end

  @doc """
  Returns the list of all DateTime keys (timestamps) in the TimeSeries.

  The keys are returned in reverse chronological order (newest first).

  ## Parameters

    - `series` - The TimeSeries to get keys from

  ## Returns

    - A list of `DateTime.t()` timestamps

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.keys(ts)
      [~U[2024-01-01 10:01:00Z], ~U[2024-01-01 10:00:00Z]]

      iex> ts = TimeSeries.new()
      iex> TimeSeries.keys(ts)
      []

  """
  @spec keys(t()) :: [DateTime.t()]
  def keys(%TimeSeries{dt: dt}) do
    dt
  end

  @doc """
  Returns the list of all values in the TimeSeries.

  The values are returned in reverse chronological order (newest first),
  matching the order of keys returned by `keys/1`.

  ## Parameters

    - `series` - The TimeSeries to get values from

  ## Returns

    - A list of values

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> TimeSeries.values(ts)
      [101.0, 100.0]

      iex> ts = TimeSeries.new()
      iex> TimeSeries.values(ts)
      []

  """
  @spec values(t()) :: [any()]
  def values(%TimeSeries{data: %DataSeries{data: data}}) do
    data
  end

  ## Access behaviour

  @doc """
  Fetches the value(s) at the given index, range, or datetime.

  This function is part of the `Access` behaviour and allows using bracket syntax
  and `get_in/2` to access values.

  ## Parameters

    - `series` - The TimeSeries to fetch from
    - `key` - Can be:
      - Integer index: returns `{:ok, value}`
      - DateTime: returns `{:ok, value}`
      - Range: returns `{:ok, [value]}`

  ## Returns

    - `{:ok, value}` for integer index or DateTime
    - `{:ok, [value]}` for Range (list of values)
    - `:error` if not found or out of bounds

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> ts = ts |> TimeSeries.add(~U[2024-01-01 10:01:00Z], 101.0)
      iex> Access.fetch(ts, 0)
      {:ok, 101.0}
      iex> Access.fetch(ts, ~U[2024-01-01 10:00:00Z])
      {:ok, 100.0}
      iex> Access.fetch(ts, 0..1)
      {:ok, [101.0, 100.0]}

  """
  @impl Access
  def fetch(%TimeSeries{data: data}, index) when is_integer(index) do
    case Access.fetch(data, index) do
      {:ok, value} -> {:ok, value}
      :error -> :error
    end
  end

  def fetch(%TimeSeries{data: data}, %Range{} = range) do
    Access.fetch(data, range)
  end

  def fetch(%TimeSeries{data: data, dt: dt}, %DateTime{} = datetime) do
    case find_datetime_index(dt, datetime) do
      nil -> :error
      index -> {:ok, DataSeries.at(data, index)}
    end
  end

  @doc """
  Gets the value at the given index or datetime and updates it with a function.

  This function is part of the `Access` behaviour and allows using `update_in/3` and
  `get_and_update_in/3`.

  ## Parameters

    - `series` - The TimeSeries to update
    - `key` - Can be:
      - Integer index: accesses by position
      - DateTime: accesses by timestamp
    - `function` - A function that receives the current value and returns
      `{get_value, new_value}`

  ## Returns

    - `{old_value, new_series}` for both integer index and DateTime

  ## Raises

    - `ArgumentError` if index/datetime is out of bounds or not found
    - `ArgumentError` if function returns `:pop`

  ## Examples

      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> {old, new_ts} = Access.get_and_update(ts, 0, fn val -> {val, val * 2} end)
      iex> old
      100.0
      iex> new_ts[0]
      200.0

      iex> ts = TimeSeries.new()
      iex> ts = TimeSeries.add(ts, ~U[2024-01-01 10:00:00Z], 100.0)
      iex> {old, new_ts} = Access.get_and_update(ts, ~U[2024-01-01 10:00:00Z], fn val -> {val, val * 2} end)
      iex> old
      100.0
      iex> new_ts[~U[2024-01-01 10:00:00Z]]
      200.0

  """
  @impl Access
  def get_and_update(%TimeSeries{data: data} = series, index, function) when is_integer(index) do
    size = DataSeries.size(data)

    if index < -size or index >= size do
      raise ArgumentError, "index #{index} out of bounds for TimeSeries of size #{size}"
    end

    current_value = DataSeries.at(data, index)

    case function.(current_value) do
      {get_value, new_value} ->
        {_, updated_data} = Access.get_and_update(data, index, fn _old -> {nil, new_value} end)
        new_series = %TimeSeries{series | data: updated_data}
        {get_value, new_series}

      :pop ->
        raise ArgumentError, "cannot pop from a TimeSeries"
    end
  end

  def get_and_update(%TimeSeries{data: data, dt: dt} = series, %DateTime{} = datetime, function) do
    case find_datetime_index(dt, datetime) do
      nil ->
        raise ArgumentError,
              "datetime #{DateTime.to_iso8601(datetime)} not found in TimeSeries"

      index ->
        current_value = DataSeries.at(data, index)

        case function.(current_value) do
          {get_value, new_value} ->
            update_series_value(series, data, index, get_value, new_value)

          :pop ->
            raise ArgumentError, "cannot pop from a TimeSeries"
        end
    end
  end

  @doc """
  Pop is not supported for TimeSeries.

  This function is part of the `Access` behaviour but always raises an error because
  popping values from a TimeSeries is not a supported operation.

  ## Raises

    - Always raises a RuntimeError

  ## Examples

      iex> ts = TimeSeries.new() |> TimeSeries.add(~U[2024-01-01 10:00:00Z], 100.0)
      iex> Access.pop(ts, 0)
      ** (RuntimeError) you cannot pop a TimeSeries

  """
  @impl Access
  def pop(_series, _key) do
    raise "you cannot pop a TimeSeries"
  end

  ## Internal use ONLY

  @doc false
  def find_datetime_index(dt, datetime) do
    Enum.find_index(dt, &(DateTime.compare(&1, datetime) == :eq))
  end

  ## Private functions

  defp insert_value(data, dt, datetime, value) do
    new_data = DataSeries.add(data, value)
    new_dt = [datetime | dt]

    # Truncate dt if max_size is reached
    final_dt =
      if DataSeries.size(new_data) < length(new_dt) do
        new_dt |> Enum.reverse() |> tl() |> Enum.reverse()
      else
        new_dt
      end

    %TimeSeries{data: new_data, dt: final_dt}
  end

  defp update_series_value(%TimeSeries{} = series, data, index, get_value, new_value) do
    {_, updated_data} = Access.get_and_update(data, index, fn _old -> {nil, new_value} end)
    new_series = %TimeSeries{series | data: updated_data}

    {get_value, new_series}
  end
end

defimpl Enumerable, for: TheoryCraft.TimeSeries do
  @moduledoc """
  Enumerable protocol implementation for TimeSeries.

  This allows using all `Enum` functions on a TimeSeries. The enumeration order
  is newest to oldest, and each element is a `{datetime, value}` tuple.
  """

  alias TheoryCraft.TimeSeries

  @doc """
  Returns the number of elements in the TimeSeries.
  """
  def count(%TimeSeries{data: data}) do
    {:ok, TheoryCraft.DataSeries.size(data)}
  end

  @doc """
  Checks if a datetime is a member of the TimeSeries.
  """
  def member?(%TimeSeries{dt: dt}, %DateTime{} = datetime) do
    {:ok, TheoryCraft.TimeSeries.find_datetime_index(dt, datetime) != nil}
  end

  def member?(_series, _element) do
    {:ok, false}
  end

  @doc """
  Reduces the TimeSeries to a single value.

  Each element passed to the function is a {datetime, value} tuple.
  """
  def reduce(%TimeSeries{data: %TheoryCraft.DataSeries{data: data_list}, dt: dt}, acc, fun) do
    # Zip the datetimes and values together
    pairs = Enum.zip(dt, data_list)
    Enumerable.List.reduce(pairs, acc, fun)
  end

  @doc """
  Returns a slice of the TimeSeries.

  Each element in the slice is a {datetime, value} tuple.
  """
  def slice(%TimeSeries{data: %TheoryCraft.DataSeries{data: data_list, size: size}, dt: dt}) do
    slicer = fn start, length, step ->
      dt_slice = dt |> Enum.slice(start, length) |> Enum.take_every(step)
      values_slice = data_list |> Enum.slice(start, length) |> Enum.take_every(step)

      Enum.zip(dt_slice, values_slice)
    end

    {:ok, size, slicer}
  end
end

defimpl Inspect, for: TheoryCraft.TimeSeries do
  @moduledoc """
  Inspect protocol implementation for TimeSeries.

  Provides a human-readable representation of the TimeSeries structure.
  """

  import Inspect.Algebra

  def inspect(%TheoryCraft.TimeSeries{} = ts, opts) do
    %TheoryCraft.TimeSeries{
      data: %TheoryCraft.DataSeries{data: data_list, size: size, max_size: max_size},
      dt: dt
    } = ts

    # Zip datetimes with values for display
    pairs = Enum.zip(dt, data_list)

    concat([
      "#TimeSeries<",
      to_doc(pairs, opts),
      ", size: ",
      to_string(size),
      ", max: ",
      to_string(max_size),
      ">"
    ])
  end
end
