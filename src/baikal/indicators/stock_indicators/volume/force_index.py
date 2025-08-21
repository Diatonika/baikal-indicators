from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ForceIndexConfig(BaseModel):
    lookback_periods: int = 13


class ForceIndexModel(TimeSeries):
    force_index: Float64  # ABSOLUTE


class ForceIndex(Indicator[ForceIndexConfig, ForceIndexModel]):
    @classmethod
    def model(cls) -> type[ForceIndexModel]:
        return ForceIndexModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "force_index": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

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
