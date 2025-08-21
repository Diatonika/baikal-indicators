from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class UlcerIndexConfig(BaseModel):
    lookback_periods: int = 14


class UlcerIndexModel(TimeSeries):
    ulcer_index: Float64  # [0; 0.5 ->


class UlcerIndex(Indicator[UlcerIndexConfig, UlcerIndexModel]):
    @classmethod
    def model(cls) -> type[UlcerIndexModel]:
        return UlcerIndexModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "ulcer_index": FieldMetadata(range_type=RangeType.BOUNDED),
        }

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
