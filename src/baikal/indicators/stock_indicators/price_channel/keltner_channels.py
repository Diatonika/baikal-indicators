from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class KeltnerChannelsConfig(BaseModel):
    ema_periods: int = 20
    multiplier: float = 2.0
    atr_periods: int = 10


class KeltnerChannelsModel(TimeSeries):
    keltner_upper: Float64  # ABSOLUTE
    keltner_center: Float64  # ABSOLUTE
    keltner_lower: Float64  # ABSOLUTE
    keltner_width: Float64  # [0; 0.5 ->


class KeltnerChannels(Indicator[KeltnerChannelsConfig, KeltnerChannelsModel]):
    @classmethod
    def model(cls) -> type[KeltnerChannelsModel]:
        return KeltnerChannelsModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "keltner_upper": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "keltner_center": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "keltner_lower": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "keltner_width": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[KeltnerChannelsModel]:
        results = indicators.get_keltner(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[KeltnerChannelsModel](
            {
                "date_time": [value.date for value in results],
                "keltner_upper": [value.upper_band for value in results],
                "keltner_center": [value.center_line for value in results],
                "keltner_lower": [value.lower_band for value in results],
                "keltner_width": [value.width for value in results],
            }
        )
