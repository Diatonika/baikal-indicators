from collections.abc import Iterable

from pandera import Field
from pandera.typing.polars import DataFrame
from polars import Float32, Float64
from pydantic import BaseModel
from stock_indicators import EndType, Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class ATRTrailingStopConfig(BaseModel):
    lookback_periods: int = 21
    multiplier: float = 3
    end_type: EndType = EndType.CLOSE


class ATRTrailingStopModel(TimeSeries):
    atr_stop: Float64 = Field(nullable=True)
    atr_buy_stop: Float32 = Field(nullable=True)
    atr_sell_stop: Float32 = Field(nullable=True)


class ATRTrailingStop(Indicator[ATRTrailingStopConfig, ATRTrailingStopModel]):
    @classmethod
    def model(cls) -> type[ATRTrailingStopModel]:
        return ATRTrailingStopModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ATRTrailingStopModel]:
        results = indicators.get_atr_stop(quotes, **self._config.model_dump())
        return DataFrame[ATRTrailingStopModel](
            {
                "date_time": [value.date for value in results],
                "atr_stop": [value.atr_stop for value in results],
                "atr_buy_stop": [int(value.buy_stop is None) for value in results],
                "atr_sell_stop": [int(value.sell_stop is None) for value in results],
            }
        )
