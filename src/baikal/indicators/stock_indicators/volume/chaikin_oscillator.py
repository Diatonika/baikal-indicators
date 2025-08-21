from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ChaikinOscillatorConfig(BaseModel):
    fast_periods: int = 3
    slow_periods: int = 10


class ChaikinOscillatorModel(TimeSeries):
    chaikin_oscillator: Float64  # ABSOLUTE


class ChaikinOscillator(Indicator[ChaikinOscillatorConfig, ChaikinOscillatorModel]):
    @classmethod
    def model(cls) -> type[ChaikinOscillatorModel]:
        return ChaikinOscillatorModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "chaikin_oscillator": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ChaikinOscillatorModel]:
        results = indicators.get_chaikin_osc(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ChaikinOscillatorModel](
            {
                "date_time": [value.date for value in results],
                "chaikin_oscillator": [value.oscillator for value in results],
            }
        )
