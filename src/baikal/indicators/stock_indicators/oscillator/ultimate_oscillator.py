from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class UltimateOscillatorConfig(BaseModel):
    short_periods: int = 7
    middle_periods: int = 14
    long_periods: int = 28


class UltimateOscillatorModel(TimeSeries):
    ultimate_oscillator: Float64


class UltimateOscillator(Indicator[UltimateOscillatorConfig, UltimateOscillatorModel]):
    @classmethod
    def model(cls) -> type[UltimateOscillatorModel]:
        return UltimateOscillatorModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[UltimateOscillatorModel]:
        results = indicators.get_ultimate(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[UltimateOscillatorModel](
            {
                "date_time": [value.date for value in results],
                "ultimate_oscillator": [value.ultimate for value in results],
            }
        )
