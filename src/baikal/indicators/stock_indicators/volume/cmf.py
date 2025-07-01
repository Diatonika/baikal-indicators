from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class CMFConfig(BaseModel):
    lookback_periods: int = 20


class CMFModel(TimeSeries):
    money_flow_multiplier: Float64

    # Non-normalized
    money_flow_volume: Float64
    # Non-normalized
    cmf: Float64


class CMF(Indicator[CMFConfig, CMFModel]):
    @classmethod
    def model(cls) -> type[CMFModel]:
        return CMFModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[CMFModel]:
        results = indicators.get_cmf(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods(self._config.lookback_periods)

        return DataFrame[CMFModel](
            {
                "date_time": [value.date for value in results],
                "money_flow_multiplier": [
                    value.money_flow_multiplier for value in results
                ],
                "money_flow_volume": [value.money_flow_volume for value in results],
                "cmf": [value.cmf for value in results],
            }
        )
