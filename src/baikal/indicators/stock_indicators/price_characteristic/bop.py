from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class BOPConfig(BaseModel):
    smooth_periods: int = 14


class BOPModel(TimeSeries):
    bop: Float64  # [-1; 1]


class BOP(Indicator[BOPConfig, BOPModel]):
    @classmethod
    def model(cls) -> type[BOPModel]:
        return BOPModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "bop": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[BOPModel]:
        results = indicators.get_bop(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[BOPModel](
            {
                "date_time": [value.date for value in results],
                "bop": [value.bop for value in results],
            }
        )
