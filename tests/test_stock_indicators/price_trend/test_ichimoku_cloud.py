import datetime

from pandera.typing.polars import DataFrame

from baikal.common.trade.models import OHLCV
from baikal.indicators.stock_indicators import BatchIndicator
from baikal.indicators.stock_indicators.price_trend import (
    IchimokuCloud,
    IchimokuCloudConfig,
)
from tests.utility.assertions import Assertions


def test_ichimoku_cloud(assertions: Assertions, ohlcv_day: DataFrame[OHLCV]) -> None:
    indicator = IchimokuCloud(IchimokuCloudConfig())
    batch_indicator = BatchIndicator([indicator])

    results = batch_indicator.calculate(
        ohlcv_day,
        "1m",
        warmup_period=datetime.timedelta(minutes=750),
        window_size=datetime.timedelta(minutes=10000),
    )

    assertions.day_test_assertions(results)
    assertions.assert_metadata(indicator)
