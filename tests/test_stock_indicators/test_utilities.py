import datetime

from pathlib import Path

from pandera.typing.polars import DataFrame
from polars import DataFrame as PolarDataFrame, read_parquet

from baikal.common.trade.models import OHLCV
from baikal.converters.binance import (
    BinanceConverter,
    BinanceDataConfig,
    BinanceDataInterval,
    BinanceDataType,
    BinanceInstrumentType,
)
from baikal.indicators.stock_indicators import BatchIndicator, to_quotes
from baikal.indicators.stock_indicators.price_trend import (
    ADX,
    ADXConfig,
    ATRTrailingStop,
    ATRTrailingStopConfig,
)


def test_to_quotes(ohlcv_month: DataFrame[OHLCV]) -> None:
    quotes = to_quotes(ohlcv_month)
    assert len(quotes) == 44_640


def test_batch_indicator(tmp_path: Path, global_datadir: Path) -> None:
    converter = BinanceConverter(global_datadir)
    lazy_ohlcv = converter.load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2020, 3, 4, tzinfo=datetime.UTC),
        datetime.datetime(2020, 3, 5, tzinfo=datetime.UTC),
    )

    ohlcv = DataFrame[OHLCV](lazy_ohlcv.collect())

    atr_trailing_stop = ATRTrailingStop(ATRTrailingStopConfig())
    batch_indicator = BatchIndicator([atr_trailing_stop])

    def _asserts(results: PolarDataFrame) -> None:
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

    memory_results = batch_indicator.calculate(
        ohlcv,
        "1m",
        warmup_period=datetime.timedelta(minutes=300),
        window_size=datetime.timedelta(minutes=3000),
        parquet_path=tmp_path / "parquet",
        return_frame=True,
    )

    _asserts(memory_results)
    _asserts(read_parquet(tmp_path / "parquet"))


def test_batch_indicator_metadata() -> None:
    atr_trailing_stop = ATRTrailingStop(ATRTrailingStopConfig())
    adx = ADX(ADXConfig())

    batch_indicator = BatchIndicator([atr_trailing_stop, adx])

    assert batch_indicator.metadata() == atr_trailing_stop.metadata() | adx.metadata()
