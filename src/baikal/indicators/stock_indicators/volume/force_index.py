from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class ForceIndexConfig(BaseModel):
    lookback_periods: int = 13


class ForceIndexModel(TimeSeries):
    # Non-normalized
    force_index: Float64


class ForceIndex(Indicator[ForceIndexConfig, ForceIndexModel]):
    @classmethod
    def model(cls) -> type[ForceIndexModel]:
        return ForceIndexModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ForceIndexModel]:
        results = indicators.get_force_index(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ForceIndexModel](
            {
                "date_time": [value.date for value in results],
                "force_index": [value.force_index for value in results],
            }
        )
