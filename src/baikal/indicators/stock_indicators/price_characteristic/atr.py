from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ATRConfig(BaseModel):
    lookback_periods: int = 14


class ATRModel(TimeSeries):
    atr_percent: Float64  # [0; 1]


class ATR(Indicator[ATRConfig, ATRModel]):
    @classmethod
    def model(cls) -> type[ATRModel]:
        return ATRModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "atr_percent": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ATRModel]:
        results = indicators.get_atr(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ATRModel](
            {
                "date_time": [value.date for value in results],
                "atr_percent": [value.atrp / 100 for value in results],
            }
        )
