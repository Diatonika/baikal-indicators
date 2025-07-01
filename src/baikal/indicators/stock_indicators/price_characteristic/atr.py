from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class ATRConfig(BaseModel):
    lookback_periods: int = 14


class ATRModel(TimeSeries):
    atr_percent: Float64


class ATR(Indicator[ATRConfig, ATRModel]):
    @classmethod
    def model(cls) -> type[ATRModel]:
        return ATRModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ATRModel]:
        results = indicators.get_atr(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ATRModel](
            {
                "date_time": [value.date for value in results],
                "atr_percent": [value.atrp for value in results],
            }
        )
