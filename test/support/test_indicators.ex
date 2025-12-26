defmodule TheoryCraft.TestIndicators do
  @moduledoc false
  # Test indicator modules for testing IndicatorProcessor behavior

  alias TheoryCraft.MarketSource.IndicatorValue

  defmodule SimpleIndicator do
    @moduledoc false
    # A simple test indicator that returns a constant value

    @behaviour TheoryCraft.MarketSource.Indicator

    alias TheoryCraft.MarketSource.IndicatorValue

    @impl true
    def init(opts) do
      constant = Keyword.get(opts, :constant, 10.0)
      data_name = Keyword.fetch!(opts, :data)
      {:ok, %{constant: constant, data_name: data_name}}
    end

    @impl true
    def next(_event, state) do
      %{constant: constant, data_name: data_name} = state

      indicator_value = %IndicatorValue{
        value: constant,
        data_name: data_name
      }

      {:ok, indicator_value, state}
    end
  end

  defmodule SMAIndicator do
    @moduledoc false
    # A test SMA indicator that maintains a simple moving average

    @behaviour TheoryCraft.MarketSource.Indicator

    alias TheoryCraft.MarketSource.{Bar, IndicatorValue}

    @impl true
    def init(opts) do
      period = Keyword.get(opts, :period, 5)
      data_name = Keyword.fetch!(opts, :data)

      {:ok, %{period: period, values: [], data_name: data_name}}
    end

    @impl true
    def next(event, state) do
      %{period: period, values: values, data_name: data_name} = state

      # Extract value from event
      value = event.data[data_name]

      # Extract close price from bar
      close =
        case value do
          %Bar{close: close} -> close
          %{close: close} -> close
          v when is_number(v) -> v
          _ -> 0.0
        end

      # Add value to history
      new_values = [close | values] |> Enum.take(period)

      # Calculate SMA
      count = length(new_values)

      sma =
        if count > 0 do
          Enum.sum(new_values) / count
        else
          nil
        end

      new_state = %{state | values: new_values}

      # Wrap in IndicatorValue
      indicator_value = %IndicatorValue{
        value: sma,
        data_name: data_name
      }

      {:ok, indicator_value, new_state}
    end
  end

  defmodule FailingIndicator do
    @moduledoc false
    # An indicator that fails during init for error testing

    @behaviour TheoryCraft.MarketSource.Indicator

    @impl true
    def init(_opts) do
      case :erlang.phash2(1, 1) do
        0 -> {:error, :init_failed}
        # The second case is never reached, but prevents Dialyzer warnings
        _ -> {:ok, %{}}
      end
    end

    @impl true
    def next(_event, _state) do
      raise "Should not be called"
    end
  end
end
