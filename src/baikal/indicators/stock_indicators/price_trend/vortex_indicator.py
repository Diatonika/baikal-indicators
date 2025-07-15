from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class VortexIndicatorConfig(BaseModel):
    lookback_periods: int = 14


class VortexIndicatorModel(TimeSeries):
    vortex_pvi: Float64  # [0; 4 ->
    vortex_nvi: Float64  # [0; 4 ->
    vortex_relation: Float64  # [0; 5 ->


class VortexIndicator(Indicator[VortexIndicatorConfig, VortexIndicatorModel]):
    @classmethod
    def model(cls) -> type[VortexIndicatorModel]:
        return VortexIndicatorModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "vortex_pvi": FieldMetadata(range_type=RangeType.BOUNDED),
            "vortex_nvi": FieldMetadata(range_type=RangeType.BOUNDED),
            "vortex_relation": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[VortexIndicatorModel]:
        results = indicators.get_vortex(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[VortexIndicatorModel](
            {
                "date_time": [value.date for value in results],
                "vortex_pvi": [value.pvi for value in results],
                "vortex_nvi": [value.nvi for value in results],
                "vortex_relation": [
                    value.pvi / value.nvi if value.nvi > 1e-5 else 0
                    for value in results
                ],
            }
        )
