from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel, Field as PydanticField
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class StochasticRSIConfig(BaseModel):
    rsi_periods: int = 14
    stoch_periods: int = PydanticField(default_factory=lambda data: data["rsi_periods"])
    signal_periods: int = 3
    smooth_periods: int = 1


class StochasticRSIModel(TimeSeries):
    stoch_rsi: Float64  # [0; 1]
    stoch_rsi_signal: Float64  # [0; 1]


class StochasticRSI(Indicator[StochasticRSIConfig, StochasticRSIModel]):
    @classmethod
    def model(cls) -> type[StochasticRSIModel]:
        return StochasticRSIModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "stoch_rsi": FieldMetadata(range_type=RangeType.BOUNDED),
            "stoch_rsi_signal": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[StochasticRSIModel]:
        results = indicators.get_stoch_rsi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[StochasticRSIModel](
            {
                "date_time": [value.date for value in results],
                "stoch_rsi": [value.stoch_rsi / 100 for value in results],
                "stoch_rsi_signal": [value.signal / 100 for value in results],
            }
        )
