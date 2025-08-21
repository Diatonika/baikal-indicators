from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float32, Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class VolatilityStopConfig(BaseModel):
    lookback_periods: int = 7
    multiplier: float = 3.0


class VolatilityStopModel(TimeSeries):
    volatility_stop: Float64  # ABSOLUTE
    volatility_stop_reverse: Float32  # 0 / 1
    volatility_stop_upper: Float32  # 0 / 1
    volatility_stop_lower: Float32  # 0 / 1


class VolatilityStop(Indicator[VolatilityStopConfig, VolatilityStopModel]):
    @classmethod
    def model(cls) -> type[VolatilityStopModel]:
        return VolatilityStopModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "volatility_stop": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "volatility_stop_reverse": FieldMetadata(range_type=RangeType.BOUNDED),
            "volatility_stop_upper": FieldMetadata(range_type=RangeType.BOUNDED),
            "volatility_stop_lower": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[VolatilityStopModel]:
        results = indicators.get_volatility_stop(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[VolatilityStopModel](
            {
                "date_time": [value.date for value in results],
                "volatility_stop": [value.sar for value in results],
                "volatility_stop_reverse": [
                    None if value.is_stop is None else int(value.is_stop)
                    for value in results
                ],
                "volatility_stop_upper": [
                    int(value.upper_band is not None) for value in results
                ],
                "volatility_stop_lower": [
                    int(value.lower_band is not None) for value in results
                ],
            }
        )
