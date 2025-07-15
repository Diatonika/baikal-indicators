from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class TRIXConfig(BaseModel):
    lookback_periods: int = 15
    signal_periods: int = 9


class TRIXModel(TimeSeries):
    trix_smooth_ema: Float64  # ABSOLUTE
    trix: Float64  # <- -0.02; 0.02 ->
    trix_signal: Float64  # <- -0.02; 0.02 ->


class TRIX(Indicator[TRIXConfig, TRIXModel]):
    @classmethod
    def model(cls) -> type[TRIXModel]:
        return TRIXModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "trix_smooth_ema": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "trix": FieldMetadata(range_type=RangeType.BOUNDED),
            "trix_signal": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[TRIXModel]:
        results = indicators.get_trix(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[TRIXModel](
            {
                "date_time": [value.date for value in results],
                "trix_smooth_ema": [value.ema3 for value in results],
                "trix": [value.trix for value in results],
                "trix_signal": [value.signal for value in results],
            }
        )
