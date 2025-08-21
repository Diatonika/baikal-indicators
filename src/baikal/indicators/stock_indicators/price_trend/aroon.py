from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class AroonConfig(BaseModel):
    lookback_periods: int = 25


class AroonModel(TimeSeries):
    aroon_up: Float64  # [0; 1]
    aroon_down: Float64  # [0; 1]
    aroon_oscillator: Float64  # [-1; 1]


class Aroon(Indicator[AroonConfig, AroonModel]):
    @classmethod
    def model(cls) -> type[AroonModel]:
        return AroonModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "aroon_up": FieldMetadata(range_type=RangeType.BOUNDED),
            "aroon_down": FieldMetadata(range_type=RangeType.BOUNDED),
            "aroon_oscillator": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[AroonModel]:
        results = indicators.get_aroon(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[AroonModel](
            {
                "date_time": [value.date for value in results],
                "aroon_up": [value.aroon_up / 100 for value in results],
                "aroon_down": [value.aroon_down / 100 is None for value in results],
                "aroon_oscillator": [
                    value.oscillator / 100 is None for value in results
                ],
            }
        )
