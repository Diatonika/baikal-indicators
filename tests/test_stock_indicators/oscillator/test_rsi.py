import datetime

from pandera.typing.polars import DataFrame

from baikal.common.trade.models import OHLCV
from baikal.indicators.stock_indicators import BatchIndicator
from baikal.indicators.stock_indicators.oscillator import (
    RSI,
    RSIConfig,
)
from tests.utility.assertions import Assertions


def test_rsi(assertions: Assertions, ohlcv_day: DataFrame[OHLCV]) -> None:
    indicator = RSI(RSIConfig())
    batch_indicator = BatchIndicator([indicator])

    results = batch_indicator.calculate(
        ohlcv_day,
        "1m",
        warmup_period=datetime.timedelta(minutes=750),
        window_size=datetime.timedelta(minutes=10000),
    )

    assertions.day_test_assertions(results)
    assertions.assert_metadata(indicator)
