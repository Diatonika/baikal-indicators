from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float32, Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class SuperTrendConfig(BaseModel):
    lookback_periods: int = 10
    multiplier: float = 3.0


class SuperTrendModel(TimeSeries):
    super_trend: Float64  # ABSOLUTE
    super_trend_upper: Float32  # 0 / 1
    super_trend_lower: Float32  # 0 / 1


class SuperTrend(Indicator[SuperTrendConfig, SuperTrendModel]):
    @classmethod
    def model(cls) -> type[SuperTrendModel]:
        return SuperTrendModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "super_trend": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "super_trend_upper": FieldMetadata(range_type=RangeType.BOUNDED),
            "super_trend_lower": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[SuperTrendModel]:
        results = indicators.get_super_trend(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[SuperTrendModel](
            {
                "date_time": [value.date for value in results],
                "super_trend": [value.super_trend for value in results],
                "super_trend_upper": [
                    int(value.upper_band is not None) for value in results
                ],
                "super_trend_lower": [
                    int(value.lower_band is not None) for value in results
                ],
            }
        )
