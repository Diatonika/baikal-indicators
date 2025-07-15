from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class CMOConfig(BaseModel):
    lookback_periods: int = 20


class CMOModel(TimeSeries):
    cmo: Float64  # [-1; 1]


class CMO(Indicator[CMOConfig, CMOModel]):
    @classmethod
    def model(cls) -> type[CMOModel]:
        return CMOModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "cmo": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[CMOModel]:
        results = indicators.get_cmo(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[CMOModel](
            {
                "date_time": [value.date for value in results],
                "cmo": [value.cmo / 100 for value in results],
            }
        )
