from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class ChopinessIndexConfig(BaseModel):
    lookback_periods: int = 14


class ChopinessIndexModel(TimeSeries):
    chop: Float64


class ChopinessIndex(Indicator[ChopinessIndexConfig, ChopinessIndexModel]):
    @classmethod
    def model(cls) -> type[ChopinessIndexModel]:
        return ChopinessIndexModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ChopinessIndexModel]:
        results = indicators.get_chop(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ChopinessIndexModel](
            {
                "date_time": [value.date for value in results],
                "chop": [value.chop for value in results],
            }
        )
