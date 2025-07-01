from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class KlingerVolumeOscillatorConfig(BaseModel):
    fast_periods: int = 34
    slow_periods: int = 55
    signal_periods: int = 13


class KlingerVolumeOscillatorModel(TimeSeries):
    # Non-normalized
    klinger_oscillator: Float64
    # Non-normalized
    klinger_oscillator_signal: Float64


class KlingerVolumeOscillator(
    Indicator[KlingerVolumeOscillatorConfig, KlingerVolumeOscillatorModel]
):
    @classmethod
    def model(cls) -> type[KlingerVolumeOscillatorModel]:
        return KlingerVolumeOscillatorModel

    def calculate(
        self, quotes: Iterable[Quote]
    ) -> DataFrame[KlingerVolumeOscillatorModel]:
        results = indicators.get_kvo(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[KlingerVolumeOscillatorModel](
            {
                "date_time": [value.date for value in results],
                "klinger_oscillator": [value.oscillator for value in results],
                "klinger_oscillator_signal": [value.signal for value in results],
            }
        )
