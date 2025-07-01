from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class TSIConfig(BaseModel):
    lookback_periods: int = 25
    smooth_periods: int = 13
    signal_periods: int = 7


class TSIModel(TimeSeries):
    tsi: Float64
    tsi_signal: Float64


class TSI(Indicator[TSIConfig, TSIModel]):
    @classmethod
    def model(cls) -> type[TSIModel]:
        return TSIModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[TSIModel]:
        results = indicators.get_tsi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[TSIModel](
            {
                "date_time": [value.date for value in results],
                "tsi": [value.tsi for value in results],
                "tsi_signal": [value.signal for value in results],
            }
        )
