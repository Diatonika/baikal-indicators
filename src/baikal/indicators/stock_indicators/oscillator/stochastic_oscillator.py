from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import MAType, Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class StochasticOscillatorConfig(BaseModel):
    lookback_periods: int = 14
    signal_periods: int = 3
    smooth_periods: int = 3
    k_factor: int = 3
    d_factor: int = 2
    ma_type: MAType = MAType.SMMA


class StochasticOscillatorModel(TimeSeries):
    stochastic_oscillator: Float64  # [0; 1]
    stochastic_signal: Float64  # [0; 1]
    stochastic_divergence: Float64  # <- -0.5; 1.5 ->


class StochasticOscillator(
    Indicator[StochasticOscillatorConfig, StochasticOscillatorModel]
):
    @classmethod
    def model(cls) -> type[StochasticOscillatorModel]:
        return StochasticOscillatorModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "stochastic_oscillator": FieldMetadata(range_type=RangeType.BOUNDED),
            "stochastic_signal": FieldMetadata(range_type=RangeType.BOUNDED),
            "stochastic_divergence": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(
        self, quotes: Iterable[Quote]
    ) -> DataFrame[StochasticOscillatorModel]:
        results = indicators.get_stoch(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[StochasticOscillatorModel](
            {
                "date_time": [value.date for value in results],
                "stochastic_oscillator": [value.oscillator / 100 for value in results],
                "stochastic_signal": [value.signal / 100 for value in results],
                "stochastic_divergence": [value.percent_j / 100 for value in results],
            }
        )
