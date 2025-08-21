# CHANGELOG


## v0.2.0 (2025-08-21)

### Features

- **stock-indicators**: Support trivial OHLCV passthrough indicator, remove baikal-converters
  dependency and bump baikal-commons to 0.15
  ([#2](https://github.com/Diatonika/baikal-indicators/pull/2),
  [`1246104`](https://github.com/Diatonika/baikal-indicators/commit/124610487f7614a682d87d5cb38e6853f62b4c14))


## v0.1.0 (2025-07-15)

### Features

- **stock-indicators**: Support batched indicators from stock_indicators
  ([#1](https://github.com/Diatonika/baikal-indicators/pull/1),
  [`e3ce239`](https://github.com/Diatonika/baikal-indicators/commit/e3ce239fd764e1d4a14168bf859a7f842dd84d22))

* feat(lean): support LEAN indicators C# bindings

* fix E402 on clr loading __init__

* feat(stock-indicators): support basic batched indicators from stock_indicators

* feat(stock-indicators): cover stock-indicators by basic tests

* feat(stock-indicators): add missing file

* feat(stock-indicators): fix broken test_missing_data resource dir

* feat(stock-indicators): fix test_atr_trailing_stop assertions

* feat(stock-indicators): support price trend, price channels and oscillator indicators

* feat(stock-indicators): fix pytest tests module discovery

* feat(stock-indicators): fix mypy tests double discovery

* feat(stock-indicators): support remaining indicators

* feat(stock-indicators): transition to non-nullable OHLCV

* feat(stock-indicators): explicitly validate time frames before writing parquet

* feat(stock-indicators): explicitly label indicators values as bounded / absolute for future
  normalization

* feat(stock-indicators): support batch_indicator metadata

* feat(stock-indicators): fix format


## v0.0.0 (2025-06-25)
