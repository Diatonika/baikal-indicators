from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ChopinessIndexConfig(BaseModel):
    lookback_periods: int = 14


class ChopinessIndexModel(TimeSeries):
    chop: Float64  # [0; 1]


class ChopinessIndex(Indicator[ChopinessIndexConfig, ChopinessIndexModel]):
    @classmethod
    def model(cls) -> type[ChopinessIndexModel]:
        return ChopinessIndexModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "chop": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ChopinessIndexModel]:
        results = indicators.get_chop(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ChopinessIndexModel](
            {
                "date_time": [value.date for value in results],
                "chop": [value.chop / 100 for value in results],
            }
        )
