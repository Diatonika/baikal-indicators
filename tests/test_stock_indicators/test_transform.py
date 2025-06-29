import datetime

from pathlib import Path

from baikal.adapters.binance import (
    BinanceAdapter,
    BinanceDataConfig,
    BinanceDataInterval,
    BinanceDataType,
    BinanceInstrumentType,
)
from baikal.indicators.stock_indicators import BatchIndicator, to_quotes
from baikal.indicators.stock_indicators.price_trend import (
    ATRTrailingStop,
    ATRTrailingStopConfig,
)


def test_to_quotes(shared_datadir: Path) -> None:
    adapter = BinanceAdapter(shared_datadir)
    ohlcv = adapter.load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2019, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2019, 2, 1, tzinfo=datetime.UTC),
    )

    print(ohlcv)

    quotes = to_quotes(ohlcv)
    assert len(quotes) == 44_640


def test_atr_trailing_stop(shared_datadir: Path) -> None:
    adapter = BinanceAdapter(shared_datadir)
    ohlcv = adapter.load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2019, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2019, 2, 1, tzinfo=datetime.UTC),
    )

    atr_trailing_stop = ATRTrailingStop(ATRTrailingStopConfig())
    batch_indicator = BatchIndicator([atr_trailing_stop])

    results = batch_indicator.calculate(
        ohlcv,
        warmup_period=datetime.timedelta(minutes=750),
        window_size=datetime.timedelta(minutes=10000),
        return_frame=True,
    )

    assert results.null_count().sum_horizontal().item() == 0
    assert results["date_time"].len() == 43_890

    assert results["date_time"].first() == datetime.datetime(
        2019, 1, 1, 12, 30, tzinfo=datetime.UTC
    )

    assert results["date_time"].last() == datetime.datetime(
        2019, 1, 31, 23, 59, tzinfo=datetime.UTC
    )


def test_missing_data(shared_datadir: Path) -> None:
    adapter = BinanceAdapter(shared_datadir)
    ohlcv = adapter.load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2020, 3, 4, tzinfo=datetime.UTC),
        datetime.datetime(2020, 3, 5, tzinfo=datetime.UTC),
    )

    atr_trailing_stop = ATRTrailingStop(ATRTrailingStopConfig())
    batch_indicator = BatchIndicator([atr_trailing_stop])

    results = batch_indicator.calculate(
        ohlcv,
        warmup_period=datetime.timedelta(minutes=300),
        window_size=datetime.timedelta(minutes=3000),
        return_frame=True,
    )

    assert results["date_time"].len() == 712

    assert results["date_time"].first() == datetime.datetime(
        2020, 3, 4, 5, tzinfo=datetime.UTC
    )

    assert results["date_time"].last() == datetime.datetime(
        2020, 3, 4, 23, 59, tzinfo=datetime.UTC
    )

    assert results.filter(
        date_time=datetime.datetime(2020, 3, 4, 9, 21, tzinfo=datetime.UTC)
    )["date_time"].len()

    assert not results.filter(
        date_time=datetime.datetime(2020, 3, 4, 9, 22, tzinfo=datetime.UTC)
    )["date_time"].len()

    assert not results.filter(
        date_time=datetime.datetime(2020, 3, 4, 16, 29, tzinfo=datetime.UTC)
    )["date_time"].len()

    assert results.filter(
        date_time=datetime.datetime(2020, 3, 4, 16, 30, tzinfo=datetime.UTC)
    )["date_time"].len()
