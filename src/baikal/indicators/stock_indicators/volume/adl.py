from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ADLConfig(BaseModel):
    sma_periods: int = 20


class ADLModel(TimeSeries):
    adl_sma: Float64  # ABSOLUTE


class ADL(Indicator[ADLConfig, ADLModel]):
    @classmethod
    def model(cls) -> type[ADLModel]:
        return ADLModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "adl_sma": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ADLModel]:
        results = indicators.get_adl(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods(self._config.sma_periods)

        return DataFrame[ADLModel](
            {
                "date_time": [value.date for value in results],
                "adl_sma": [value.adl_sma for value in results],
            }
        )
