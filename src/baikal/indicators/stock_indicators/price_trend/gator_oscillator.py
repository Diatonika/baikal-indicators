from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float32, Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class GatorOscillatorConfig(BaseModel):
    pass


class GatorOscillatorModel(TimeSeries):
    gator_upper: Float64
    gator_lower: Float64
    gator_upper_expanding: Float32
    gator_lower_expanding: Float32


class GatorOscillator(Indicator[GatorOscillatorConfig, GatorOscillatorModel]):
    @classmethod
    def model(cls) -> type[GatorOscillatorModel]:
        return GatorOscillatorModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[GatorOscillatorModel]:
        results = indicators.get_gator(quotes).remove_warmup_periods()

        return DataFrame[GatorOscillatorModel](
            {
                "date_time": [value.date for value in results],
                "gator_upper": [value.upper for value in results],
                "gator_lower": [value.lower for value in results],
                "gator_upper_expanding": [
                    value.is_upper_expanding for value in results
                ],
                "gator_lower_expanding": [
                    value.is_lower_expanding for value in results
                ],
            }
        )
