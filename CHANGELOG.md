# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- GenStage-based streaming pipeline architecture with backpressure support
- `MarketSource` fluent API for building data pipelines
- `DataFeed` behaviour for data sources with `stream/1` and `stream!/1`
- `Processor` behaviour for stateful transformations with `init/1` and `next/2`
- `ResampleProcessor` for tick-to-bar and bar-to-bar resampling
- Support for multiple timeframes (t, s, m, h, D, W, M)
- `TimeFrame` module for parsing and validating timeframe strings
- Parallel processing with `BroadcastStage` and `AggregatorStage`
- CSV data feed for loading tick data from files
- `bar_only` option to filter incomplete bars
