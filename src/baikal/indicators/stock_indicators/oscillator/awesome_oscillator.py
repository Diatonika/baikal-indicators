from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class AwesomeOscillatorConfig(BaseModel):
    fast_periods: int = 5
    slow_periods: int = 34


class AwesomeOscillatorModel(TimeSeries):
    awesome_oscillator: Float64  # ABSOLUTE
    awesome_oscillator_normalized: Float64  # BOUNDED


class AwesomeOscillator(Indicator[AwesomeOscillatorConfig, AwesomeOscillatorModel]):
    @classmethod
    def model(cls) -> type[AwesomeOscillatorModel]:
        return AwesomeOscillatorModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "awesome_oscillator": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "awesome_oscillator_normalized": FieldMetadata(
                range_type=RangeType.BOUNDED
            ),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[AwesomeOscillatorModel]:
        results = indicators.get_awesome(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[AwesomeOscillatorModel](
            {
                "date_time": [value.date for value in results],
                "awesome_oscillator": [value.oscillator for value in results],
                "awesome_oscillator_normalized": [
                    value.normalized for value in results
                ],
            }
        )
