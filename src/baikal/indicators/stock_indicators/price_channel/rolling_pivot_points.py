from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import PivotPointType, Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class RollingPivotPointsConfig(BaseModel):
    window_periods: int = 12
    offset_periods: int = 6
    point_type: PivotPointType = PivotPointType.STANDARD


class RollingPivotPointsModel(TimeSeries):
    rolling_pivot_r3: Float64  # ABSOLUTE
    rolling_pivot_r2: Float64  # ABSOLUTE
    rolling_pivot_r1: Float64  # ABSOLUTE
    rolling_pivot_point: Float64  # ABSOLUTE
    rolling_pivot_s1: Float64  # ABSOLUTE
    rolling_pivot_s2: Float64  # ABSOLUTE
    rolling_pivot_s3: Float64  # ABSOLUTE


class RollingPivotPoints(Indicator[RollingPivotPointsConfig, RollingPivotPointsModel]):
    @classmethod
    def model(cls) -> type[RollingPivotPointsModel]:
        return RollingPivotPointsModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "rolling_pivot_r3": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "rolling_pivot_r2": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "rolling_pivot_r1": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "rolling_pivot_point": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "rolling_pivot_s1": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "rolling_pivot_s2": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "rolling_pivot_s3": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[RollingPivotPointsModel]:
        results = indicators.get_rolling_pivots(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[RollingPivotPointsModel](
            {
                "date_time": [value.date for value in results],
                "rolling_pivot_r3": [value.r3 for value in results],
                "rolling_pivot_r2": [value.r2 for value in results],
                "rolling_pivot_r1": [value.r1 for value in results],
                "rolling_pivot_point": [value.pp for value in results],
                "rolling_pivot_s1": [value.s1 for value in results],
                "rolling_pivot_s2": [value.s2 for value in results],
                "rolling_pivot_s3": [value.s3 for value in results],
            }
        )
