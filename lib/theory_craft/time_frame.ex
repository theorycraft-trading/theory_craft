defmodule TheoryCraft.TimeFrame do
  @moduledoc """
  Helpers for working with time frames.
  """

  @type unit :: String.t()
  @type multiplier :: non_neg_integer()
  @type t :: {unit(), multiplier()}

  @units ~w(t s m h D W M)

  ## Public API

  @doc """
  Parses a timeframe string or atom.

  ## Examples

      iex> TimeFrame.parse("m5")
      {:ok, {"m", 5}}

      iex> TimeFrame.parse(:h1)
      {:ok, {"h", 1}}

      iex> TimeFrame.parse("D")
      {:ok, {"D", 1}}

      iex> TimeFrame.parse("i12")
      {:error, {:invalid_unit, "i"}}
      
      iex> TimeFrame.parse("mI")
      {:error, {:invalid_multiplier, "I"}}
      
      iex> TimeFrame.parse("")
      {:error, :invalid_format}

  """
  @spec parse(timeframe) :: {:ok, t()} | {:error, reason}
        when timeframe: String.t() | atom(),
             reason:
               {:invalid_unit, String.t()}
               | {:invalid_multiplier, String.t()}
               | :invalid_format
  def parse(timeframe) do
    str = to_string(timeframe)

    with {:ok, unit} <- extract_unit(str),
         {:ok, mult} <- extract_multiplier(str) do
      {:ok, {unit, mult}}
    end
  end

  @doc """
  Parses a timeframe string or atom. Raises on invalid input.

  ## Examples

      iex> TimeFrame.parse!("m5")
      {"m", 5}

      iex> TimeFrame.parse!("x5")
      ** (ArgumentError) Invalid timeframe unit 'x'. Valid units are: t, s, m, h, D, W, M

  """
  @spec parse!(timeframe) :: t() when timeframe: String.t() | atom()
  def parse!(timeframe) do
    case parse(timeframe) do
      {:ok, result} ->
        result

      {:error, {:invalid_unit, unit}} ->
        all_units = Enum.join(@units, ", ")
        raise ArgumentError, "Invalid timeframe unit '#{unit}'. Valid units are: #{all_units}"

      {:error, {:invalid_multiplier, mult}} ->
        raise ArgumentError, "Invalid timeframe multiplier '#{mult}'. Must be a positive integer"

      {:error, :invalid_format} ->
        raise ArgumentError, "Invalid timeframe format #{inspect(timeframe)}"
    end
  end

  @doc """
  Checks if a timeframe string or atom is valid.

  ## Examples

      iex> TimeFrame.valid?("m5")
      true

      iex> TimeFrame.valid?(:h1)
      true

      iex> TimeFrame.valid?("invalid")
      false

  """
  @spec valid?(timeframe) :: boolean() when timeframe: String.t() | atom()
  def valid?(timeframe) do
    case parse(timeframe) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  ## Private functions

  defp extract_unit(str) do
    case str do
      <<unit::binary-size(1), _rest::binary>> when unit in @units -> {:ok, unit}
      <<unit::binary-size(1), _rest::binary>> -> {:error, {:invalid_unit, unit}}
      _ -> {:error, :invalid_format}
    end
  end

  defp extract_multiplier(str) do
    case str do
      <<_unit::binary-size(1)>> -> {:ok, 1}
      <<_unit::binary-size(1), mult_str::binary>> -> parse_multiplier(mult_str)
      _ -> {:error, :invalid_format}
    end
  end

  defp parse_multiplier(str) do
    case Integer.parse(str) do
      {mult, ""} when mult > 0 -> {:ok, mult}
      _ -> {:error, {:invalid_multiplier, str}}
    end
  end
end
