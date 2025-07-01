from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class UlcerIndexConfig(BaseModel):
    lookback_periods: int = 14


class UlcerIndexModel(TimeSeries):
    ulcer_index: Float64


class UlcerIndex(Indicator[UlcerIndexConfig, UlcerIndexModel]):
    @classmethod
    def model(cls) -> type[UlcerIndexModel]:
        return UlcerIndexModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[UlcerIndexModel]:
        results = indicators.get_ulcer_index(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[UlcerIndexModel](
            {
                "date_time": [value.date for value in results],
                "ulcer_index": [value.ui for value in results],
            }
        )
