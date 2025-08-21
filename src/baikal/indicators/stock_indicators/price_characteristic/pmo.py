from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class PMOConfig(BaseModel):
    time_periods: int = 35
    smooth_periods: int = 20
    signal_periods: int = 10


class PMOModel(TimeSeries):
    pmo: Float64  # [-1; 1]
    pmo_signal: Float64  # [-1; 1]


class PMO(Indicator[PMOConfig, PMOModel]):
    @classmethod
    def model(cls) -> type[PMOModel]:
        return PMOModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "pmo": FieldMetadata(range_type=RangeType.BOUNDED),
            "pmo_signal": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[PMOModel]:
        results = indicators.get_pmo(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[PMOModel](
            {
                "date_time": [value.date for value in results],
                "pmo": [value.pmo for value in results],
                "pmo_signal": [value.signal for value in results],
            }
        )
