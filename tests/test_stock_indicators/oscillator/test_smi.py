import datetime

from pandera.typing.polars import DataFrame
from util import Assertions

from baikal.common.trade.models import OHLCV
from baikal.indicators.stock_indicators import BatchIndicator
from baikal.indicators.stock_indicators.oscillator import (
    SMI,
    SMIConfig,
)


def test_smi(assertions: Assertions, ohlcv_day: DataFrame[OHLCV]) -> None:
    indicator = SMI(SMIConfig())
    batch_indicator = BatchIndicator([indicator])

    results = batch_indicator.calculate(
        ohlcv_day,
        warmup_period=datetime.timedelta(minutes=750),
        window_size=datetime.timedelta(minutes=10000),
    )

    assertions.day_test_assertions(results)
