from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class PVOConfig(BaseModel):
    fast_periods: int = 12
    slow_periods: int = 26
    signal_periods: int = 9


class PVOModel(TimeSeries):
    pvo: Float64
    pvo_signal: Float64
    pvo_histogram: Float64


class PVO(Indicator[PVOConfig, PVOModel]):
    @classmethod
    def model(cls) -> type[PVOModel]:
        return PVOModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[PVOModel]:
        results = indicators.get_pvo(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[PVOModel](
            {
                "date_time": [value.date for value in results],
                "pvo": [value.pvo for value in results],
                "pvo_signal": [value.signal for value in results],
                "pvo_histogram": [value.histogram for value in results],
            }
        )
