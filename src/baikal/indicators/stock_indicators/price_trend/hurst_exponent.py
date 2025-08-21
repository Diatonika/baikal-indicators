from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class HurstExponentConfig(BaseModel):
    lookback_periods: int = 100


class HurstExponentModel(TimeSeries):
    hurst_exponent: Float64  # [0; 1]


class HurstExponent(Indicator[HurstExponentConfig, HurstExponentModel]):
    @classmethod
    def model(cls) -> type[HurstExponentModel]:
        return HurstExponentModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "hurst_exponent": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[HurstExponentModel]:
        results = indicators.get_hurst(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[HurstExponentModel](
            {
                "date_time": [value.date for value in results],
                "hurst_exponent": [value.hurst_exponent for value in results],
            }
        )
