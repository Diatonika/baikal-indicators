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
        window_size=datetime.timedelta(minutes=9250),
        return_frame=True,
    )

    print(results)
    
if __name__ == "__main__":
    test_atr_trailing_stop(Path(__file__).parent / "data")
