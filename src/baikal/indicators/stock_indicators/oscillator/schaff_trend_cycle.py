from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class SchaffTrendCycleConfig(BaseModel):
    cycle_periods: int = 10
    fast_periods: int = 23
    slow_periods: int = 50


class SchaffTrendCycleModel(TimeSeries):
    schaff_trend_cycle: Float64  # [0; 1]


class SchaffTrendCycle(Indicator[SchaffTrendCycleConfig, SchaffTrendCycleModel]):
    @classmethod
    def model(cls) -> type[SchaffTrendCycleModel]:
        return SchaffTrendCycleModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "schaff_trend_cycle": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[SchaffTrendCycleModel]:
        results = indicators.get_stc(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[SchaffTrendCycleModel](
            {
                "date_time": [value.date for value in results],
                "schaff_trend_cycle": [value.stc / 100 for value in results],
            }
        )
