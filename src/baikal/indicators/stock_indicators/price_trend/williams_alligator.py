from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class WilliamsAlligatorConfig(BaseModel):
    jaw_periods: int = 13
    jaw_offset: int = 8
    teeth_periods: int = 8
    teeth_offset: int = 5
    lips_periods: int = 5
    lips_offset: int = 3


class WilliamsAlligatorModel(TimeSeries):
    williams_alligator_jaw: Float64  # ABSOLUTE
    williams_alligator_teeth: Float64  # ABSOLUTE
    williams_alligator_lips: Float64  # ABSOLUTE


class WilliamsAlligator(Indicator[WilliamsAlligatorConfig, WilliamsAlligatorModel]):
    @classmethod
    def model(cls) -> type[WilliamsAlligatorModel]:
        return WilliamsAlligatorModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "williams_alligator_jaw": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "williams_alligator_teeth": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "williams_alligator_lips": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[WilliamsAlligatorModel]:
        results = indicators.get_alligator(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[WilliamsAlligatorModel](
            {
                "date_time": [value.date for value in results],
                "williams_alligator_jaw": [value.jaw for value in results],
                "williams_alligator_teeth": [value.teeth for value in results],
                "williams_alligator_lips": [value.lips for value in results],
            }
        )
