from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class BollingerBandsConfig(BaseModel):
    lookback_periods: int = 20
    standard_deviations: int = 2


class BollingerBandsModel(TimeSeries):
    bollinger_sma: Float64  # ABSOLUTE
    bollinger_upper_band: Float64  # ABSOLUTE
    bollinger_lower_band: Float64  # ABSOLUTE
    bollinger_percent_band: Float64  # <- -1; 1 ->
    bollinger_z_score: Float64  # <- -3; 3 ->
    bollinger_width: Float64  # [0; 0.3 ->


class BollingerBands(Indicator[BollingerBandsConfig, BollingerBandsModel]):
    @classmethod
    def model(cls) -> type[BollingerBandsModel]:
        return BollingerBandsModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "bollinger_sma": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "bollinger_upper_band": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "bollinger_lower_band": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "bollinger_percent_band": FieldMetadata(range_type=RangeType.BOUNDED),
            "bollinger_z_score": FieldMetadata(range_type=RangeType.BOUNDED),
            "bollinger_width": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[BollingerBandsModel]:
        results = indicators.get_bollinger_bands(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[BollingerBandsModel](
            {
                "date_time": [value.date for value in results],
                "bollinger_sma": [value.sma for value in results],
                "bollinger_upper_band": [value.upper_band for value in results],
                "bollinger_lower_band": [value.lower_band for value in results],
                "bollinger_percent_band": [value.percent_b for value in results],
                "bollinger_z_score": [value.z_score for value in results],
                "bollinger_width": [value.width for value in results],
            }
        )
