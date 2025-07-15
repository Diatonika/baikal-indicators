from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import CandlePart, Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class MACDConfig(BaseModel):
    fast_periods: int = 12
    slow_periods: int = 26
    signal_periods: int = 9
    candle_part: CandlePart = CandlePart.CLOSE


class MACDModel(TimeSeries):
    macd: Float64  # ABSOLUTE
    macd_signal: Float64  # ABSOLUTE
    macd_histogram: Float64  # ABSOLUTE
    macd_fast_ema: Float64  # ABSOLUTE
    macd_slow_ema: Float64  # ABSOLUTE


class MACD(Indicator[MACDConfig, MACDModel]):
    @classmethod
    def model(cls) -> type[MACDModel]:
        return MACDModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "macd": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "macd_signal": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "macd_histogram": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "macd_fast_ema": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "macd_slow_ema": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

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
