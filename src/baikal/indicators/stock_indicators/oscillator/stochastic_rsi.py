from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel, Field as PydanticField
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class StochasticRSIConfig(BaseModel):
    rsi_periods: int = 14
    stoch_periods: int = PydanticField(default_factory=lambda data: data["rsi_periods"])
    signal_periods: int = 3
    smooth_periods: int = 1


class StochasticRSIModel(TimeSeries):
    stoch_rsi: Float64
    stoch_rsi_signal: Float64


class StochasticRSI(Indicator[StochasticRSIConfig, StochasticRSIModel]):
    @classmethod
    def model(cls) -> type[StochasticRSIModel]:
        return StochasticRSIModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[StochasticRSIModel]:
        results = indicators.get_stoch_rsi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[StochasticRSIModel](
            {
                "date_time": [value.date for value in results],
                "stoch_rsi": [value.stoch_rsi for value in results],
                "stoch_rsi_signal": [value.signal for value in results],
            }
        )
