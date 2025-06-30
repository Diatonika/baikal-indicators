from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class VortexIndicatorConfig(BaseModel):
    lookback_periods: int = 14


class VortexIndicatorModel(TimeSeries):
    vortex_pvi: Float64
    vortex_nvi: Float64


class VortexIndicator(Indicator[VortexIndicatorConfig, VortexIndicatorModel]):
    @classmethod
    def model(cls) -> type[VortexIndicatorModel]:
        return VortexIndicatorModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[VortexIndicatorModel]:
        results = indicators.get_vortex(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[VortexIndicatorModel](
            {
                "date_time": [value.date for value in results],
                "vortex_pvi": [value.pvi for value in results],
                "vortex_nvi": [value.nvi for value in results],
            }
        )
