from pathlib import Path

import pytest

from pandera.typing.polars import DataFrame
from polars import scan_parquet

from baikal.common.models import OHLCV
from tests.utility.assertions import Assertions


@pytest.fixture(scope="session")
def global_datadir() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def ohlcv_day(global_datadir: Path) -> DataFrame[OHLCV]:
    ohlcv = scan_parquet(global_datadir / "2019-01-01.parquet")
    return DataFrame[OHLCV](ohlcv.collect())


@pytest.fixture(scope="session")
def ohlcv_month(global_datadir: Path) -> DataFrame[OHLCV]:
    ohlcv = scan_parquet(global_datadir / "2019-01.parquet")
    return DataFrame[OHLCV](ohlcv.collect())


@pytest.fixture(scope="session")
def assertions() -> Assertions:
    return Assertions()
