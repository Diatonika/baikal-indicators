from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ElderRayConfig(BaseModel):
    lookback_periods: int = 13


class ElderRayModel(TimeSeries):
    elder_ray_ema: Float64  # ABSOLUTE
    elder_ray_bull: Float64  # ABSOLUTE
    elder_ray_bear: Float64  # ABSOLUTE


class ElderRay(Indicator[ElderRayConfig, ElderRayModel]):
    @classmethod
    def model(cls) -> type[ElderRayModel]:
        return ElderRayModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "elder_ray_ema": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "elder_ray_bull": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "elder_ray_bear": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ElderRayModel]:
        results = indicators.get_elder_ray(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ElderRayModel](
            {
                "date_time": [value.date for value in results],
                "elder_ray_ema": [value.ema for value in results],
                "elder_ray_bull": [value.bull_power for value in results],
                "elder_ray_bear": [value.bear_power for value in results],
            }
        )
