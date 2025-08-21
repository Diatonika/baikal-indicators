from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class RSIConfig(BaseModel):
    lookback_periods: int = 14


class RSIModel(TimeSeries):
    rsi: Float64  # [0; 1]


class RSI(Indicator[RSIConfig, RSIModel]):
    @classmethod
    def model(cls) -> type[RSIModel]:
        return RSIModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "rsi": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[RSIModel]:
        results = indicators.get_rsi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[RSIModel](
            {
                "date_time": [value.date for value in results],
                "rsi": [value.rsi / 100 for value in results],
            }
        )
