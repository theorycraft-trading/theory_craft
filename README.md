# TheoryCraft

[TheoryCraft](https://github.com/theorycraft-trading/theory_craft) is a powerful Elixir library for backtesting trading strategies using streaming data pipelines.

TheoryCraft provides a GenStage-based streaming architecture for processing market data through configurable processors and data feeds. Build complex trading strategies with multiple timeframes, technical indicators, and custom processors using a fluent, composable API.

## ⚠️ Development Status

**This library is under active development and the API is subject to frequent changes.**

Breaking changes may occur between releases as we refine the interface and add new features. We recommend pinning to a specific commit hash if you need stability.

## Installation

TheoryCraft is not yet available on Hex. To use it in your project, add it to your `mix.exs`:

```elixir
def deps do
  [
    {:theory_craft, github: "theorycraft-trading/theory_craft"}
  ]
end
```

## Quick Start

### Basic Pipeline

Build a simple market data pipeline with resampling:

```elixir
alias TheoryCraft.MarketSource

# Create a pipeline that loads tick data and resamples to 5-minute and 1-hour bars
market =
  %MarketSource{}
  |> MarketSource.add_data_ticks_from_csv("ticks.csv", name: "XAUUSD")
  |> MarketSource.resample("m5", name: "XAUUSD_m5")
  |> MarketSource.resample("h1", name: "XAUUSD_h1")

# Stream events through the pipeline
for event <- MarketSource.stream(market) do
  IO.inspect(event)
end
```

### With Technical Indicators

Add technical indicators using parallel processing:

```elixir
require TheoryCraftTA.TA, as: TA

alias TheoryCraft.MarketSource.MarketEvent

market =
  %MarketSource{}
  |> MarketSource.add_data_ticks_from_csv("ticks.csv", name: "XAUUSD")
  |> MarketSource.resample("m5", name: "XAUUSD_m5")
  # Indicators are processed in parallel with `add_indicators_layer/2`
  |> MarketSource.add_indicators_layer([
    TA.sma(XAUUSD_m5[:close], 20, name: "sma_20"),
    TA.ema(XAUUSD_m5[:close], 50, name: "ema_50"),
    TA.rsi(XAUUSD_m5[:close], 14, name: "rsi_14")
  ])
  |> MarketSource.add_indicator(TA.atr("XAUUSD_m5", 14, name: "atr_14"))

market
|> MarketSource.stream()
|> Enum.take(100)
|> Enum.each(fn %MarketEvent{data: data} ->
  %{
    "XAUUSD_m5" => bar,
    "sma_20" => sma,
    "ema_50" => ema,
    "rsi_14" => rsi,
    "atr_14" => atr
  } = data

  # Your strategy logic here
end)
```

### Aggregate Only Completed Bars

Filter out incomplete bars to avoid redundant calculations:

```elixir
market =
  %MarketSource{}
  |> MarketSource.add_data_ticks_from_csv("ticks.csv", name: "XAUUSD")
  |> MarketSource.resample("m5", name: "XAUUSD_m5", bar_only: true)
  |> MarketSource.add_indicators_layer([
    TA.sma(XAUUSD_m5[:close], 20, name: "sma_20")
  ])

# Only emits events when bars are complete
for event <- MarketSource.stream(market) do
  # Process only completed bars
end
```

## Features

- **GenStage Pipeline Architecture**: Backpressure-aware streaming with automatic termination handling
- **Parallel Processing**: Automatic parallelization of independent processors
- **Multiple Timeframes**: Resample tick data to any timeframe (m1, m5, h1, D, etc.)
- **Bar Aggregation**: Filter out incomplete bars to reduce redundant calculations
- **Technical Indicators**: Integration with TheoryCraftTA for technical analysis
- **Custom Processors**: Implement the `Processor` behaviour for custom transformations
- **Fluent API**: Build complex pipelines with an intuitive, chainable interface
- **Memory-Efficient**: Streaming architecture processes data without loading everything into memory

## Examples

For detailed examples and tutorials, check out the notebooks in the [livebooks](livebooks/) directory.

## License

Copyright (C) 2025 TheoryCraft Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
