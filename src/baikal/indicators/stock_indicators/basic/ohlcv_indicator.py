from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class OHLCVIndicatorConfig(BaseModel):
    pass


class OHLCVIndicatorModel(TimeSeries):
    open: Float64  # ABSOLUTE
    high: Float64  # ABSOLUTE
    low: Float64  # ABSOLUTE
    close: Float64  # ABSOLUTE
    volume: Float64  # ABSOLUTE


class OHLCVIndicator(Indicator[OHLCVIndicatorConfig, OHLCVIndicatorModel]):
    @classmethod
    def model(cls) -> type[OHLCVIndicatorModel]:
        return OHLCVIndicatorModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "open": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "high": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "low": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "close": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "volume": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[OHLCVIndicatorModel]:
        return DataFrame[OHLCVIndicatorModel](
            {
                "date_time": [quote.date for quote in quotes],
                "open": [quote.open for quote in quotes],
                "high": [quote.high for quote in quotes],
                "low": [quote.low for quote in quotes],
                "close": [quote.close for quote in quotes],
                "volume": [quote.volume for quote in quotes],
            }
        )
