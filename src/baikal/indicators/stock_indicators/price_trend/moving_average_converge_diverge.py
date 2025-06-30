from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import CandlePart, Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class MACDConfig(BaseModel):
    fast_periods: int = 12
    slow_periods: int = 26
    signal_periods: int = 9
    candle_part: CandlePart = CandlePart.CLOSE


class MACDModel(TimeSeries):
    macd: Float64
    macd_signal: Float64
    macd_histogram: Float64
    macd_fast_ema: Float64
    macd_slow_ema: Float64


class MACD(Indicator[MACDConfig, MACDModel]):
    @classmethod
    def model(cls) -> type[MACDModel]:
        return MACDModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[MACDModel]:
        results = indicators.get_macd(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[MACDModel](
            {
                "date_time": [value.date for value in results],
                "macd": [value.macd for value in results],
                "macd_signal": [value.signal for value in results],
                "macd_histogram": [value.histogram for value in results],
                "macd_fast_ema": [value.fast_ema for value in results],
                "macd_slow_ema": [value.slow_ema for value in results],
            }
        )
