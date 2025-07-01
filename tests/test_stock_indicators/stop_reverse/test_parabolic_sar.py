import datetime

from pandera.typing.polars import DataFrame

from baikal.common.trade.models import OHLCV
from baikal.indicators.stock_indicators import BatchIndicator
from baikal.indicators.stock_indicators.stop_reverse import (
    ParabolicSAR,
    ParabolicSARConfig,
)
from tests.utility.assertions import Assertions


def test_parabolic_sar(assertions: Assertions, ohlcv_day: DataFrame[OHLCV]) -> None:
    indicator = ParabolicSAR(ParabolicSARConfig())
    batch_indicator = BatchIndicator([indicator])

    results = batch_indicator.calculate(
        ohlcv_day,
        warmup_period=datetime.timedelta(minutes=750),
        window_size=datetime.timedelta(minutes=10000),
    )

    assertions.day_test_assertions(results)
