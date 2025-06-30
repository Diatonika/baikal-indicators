from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class CMOConfig(BaseModel):
    lookback_periods: int = 20


class CMOModel(TimeSeries):
    cmo: Float64


class CMO(Indicator[CMOConfig, CMOModel]):
    @classmethod
    def model(cls) -> type[CMOModel]:
        return CMOModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[CMOModel]:
        results = indicators.get_cmo(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[CMOModel](
            {
                "date_time": [value.date for value in results],
                "cmo": [value.cmo for value in results],
            }
        )
