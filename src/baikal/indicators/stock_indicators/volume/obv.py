from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class OBVConfig(BaseModel):
    sma_periods: int = 20


class OBVModel(TimeSeries):
    # Non-normalized
    obv_sma: Float64


class OBV(Indicator[OBVConfig, OBVModel]):
    @classmethod
    def model(cls) -> type[OBVModel]:
        return OBVModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[OBVModel]:
        results = indicators.get_obv(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods(self._config.sma_periods)

        return DataFrame[OBVModel](
            {
                "date_time": [value.date for value in results],
                "obv_sma": [value.obv_sma for value in results],
            }
        )
