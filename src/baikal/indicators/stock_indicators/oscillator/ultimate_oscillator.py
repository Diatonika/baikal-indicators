from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class UltimateOscillatorConfig(BaseModel):
    short_periods: int = 7
    middle_periods: int = 14
    long_periods: int = 28


class UltimateOscillatorModel(TimeSeries):
    ultimate_oscillator: Float64  # [0; 1]


class UltimateOscillator(Indicator[UltimateOscillatorConfig, UltimateOscillatorModel]):
    @classmethod
    def model(cls) -> type[UltimateOscillatorModel]:
        return UltimateOscillatorModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "ultimate_oscillator": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[UltimateOscillatorModel]:
        results = indicators.get_ultimate(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[UltimateOscillatorModel](
            {
                "date_time": [value.date for value in results],
                "ultimate_oscillator": [value.ultimate / 100 for value in results],
            }
        )
