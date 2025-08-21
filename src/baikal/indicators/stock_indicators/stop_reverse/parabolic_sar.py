from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float32, Float64
from pydantic import BaseModel, Field as PydanticField
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ParabolicSARConfig(BaseModel):
    acceleration_step: float = 0.02
    max_acceleration_factor: float = 0.2
    initial_factor: float = PydanticField(
        default_factory=lambda data: data["acceleration_step"]
    )


class ParabolicSARModel(TimeSeries):
    sar: Float64  # ABSOLUTE
    sar_reverse: Float32  # 0 / 1


class ParabolicSAR(Indicator[ParabolicSARConfig, ParabolicSARModel]):
    @classmethod
    def model(cls) -> type[ParabolicSARModel]:
        return ParabolicSARModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "sar": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "sar_reverse": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ParabolicSARModel]:
        results = indicators.get_parabolic_sar(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ParabolicSARModel](
            {
                "date_time": [value.date for value in results],
                "sar": [value.sar for value in results],
                "sar_reverse": [
                    None if value.is_reversal is None else int(value.is_reversal)
                    for value in results
                ],
            }
        )
