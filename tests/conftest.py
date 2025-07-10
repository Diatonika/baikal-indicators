import datetime

from pathlib import Path

import pytest

from pandera.typing.polars import DataFrame

from baikal.common.trade.models import OHLCV
from baikal.converters.binance import (
    BinanceConverter,
    BinanceDataConfig,
    BinanceDataInterval,
    BinanceDataType,
    BinanceInstrumentType,
)
from tests.utility.assertions import Assertions


@pytest.fixture(scope="session")
def global_datadir() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def ohlcv_day(global_datadir: Path) -> DataFrame[OHLCV]:
    converter = BinanceConverter(global_datadir)
    ohlcv = converter.load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2019, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2019, 1, 2, tzinfo=datetime.UTC),
    )

    return DataFrame[OHLCV](ohlcv.collect())


@pytest.fixture(scope="session")
def ohlcv_month(global_datadir: Path) -> DataFrame[OHLCV]:
    converter = BinanceConverter(global_datadir)
    ohlcv = converter.load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2019, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2019, 2, 1, tzinfo=datetime.UTC),
    )

    return DataFrame[OHLCV](ohlcv.collect())


@pytest.fixture(scope="session")
def assertions() -> Assertions:
    return Assertions()
