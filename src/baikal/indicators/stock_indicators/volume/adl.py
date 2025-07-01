from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class ADLConfig(BaseModel):
    sma_periods: int = 20


class ADLModel(TimeSeries):
    # Non-normalized
    adl_sma: Float64


class ADL(Indicator[ADLConfig, ADLModel]):
    @classmethod
    def model(cls) -> type[ADLModel]:
        return ADLModel

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
