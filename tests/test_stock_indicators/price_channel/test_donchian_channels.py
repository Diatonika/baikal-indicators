import datetime

from pandera.typing.polars import DataFrame

from baikal.common.models import OHLCV
from baikal.indicators.stock_indicators import BatchIndicator
from baikal.indicators.stock_indicators.price_channel import (
    DonchianChannels,
    DonchianChannelsConfig,
)
from tests.utility.assertions import Assertions


def test_donchian_channels(assertions: Assertions, ohlcv_day: DataFrame[OHLCV]) -> None:
    indicator = DonchianChannels(DonchianChannelsConfig())
    batch_indicator = BatchIndicator([indicator])

    results = batch_indicator.calculate(
        ohlcv_day,
        "1m",
        warmup_period=datetime.timedelta(minutes=750),
        window_size=datetime.timedelta(minutes=10000),
    )

    assertions.day_test_assertions(results)
    assertions.assert_metadata(indicator)
