from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class AroonConfig(BaseModel):
    lookback_periods: int = 25


class AroonModel(TimeSeries):
    aroon_up: Float64
    aroon_down: Float64
    aroon_oscillator: Float64


class Aroon(Indicator[AroonConfig, AroonModel]):
    @classmethod
    def model(cls) -> type[AroonModel]:
        return AroonModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[AroonModel]:
        results = indicators.get_aroon(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[AroonModel](
            {
                "date_time": [value.date for value in results],
                "aroon_up": [value.aroon_up for value in results],
                "aroon_down": [value.aroon_down is None for value in results],
                "aroon_oscillator": [value.oscillator is None for value in results],
            }
        )
