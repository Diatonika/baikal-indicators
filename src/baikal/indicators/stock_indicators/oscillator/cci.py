from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class CCIConfig(BaseModel):
    lookback_periods: int = 20


class CCIModel(TimeSeries):
    cci: Float64


class CCI(Indicator[CCIConfig, CCIModel]):
    @classmethod
    def model(cls) -> type[CCIModel]:
        return CCIModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[CCIModel]:
        results = indicators.get_cci(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[CCIModel](
            {
                "date_time": [value.date for value in results],
                "cci": [value.cci for value in results],
            }
        )
