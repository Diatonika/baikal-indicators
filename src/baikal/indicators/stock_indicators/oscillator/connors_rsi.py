from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class ConnorsRSIConfig(BaseModel):
    rsi_periods: int = 3
    streak_periods: int = 2
    rank_periods: int = 100


class ConnorsRSIModel(TimeSeries):
    connors_rsi_close: Float64
    connors_rsi_streak: Float64
    connors_percent_rank: Float64
    connors_rsi: Float64


class ConnorsRSI(Indicator[ConnorsRSIConfig, ConnorsRSIModel]):
    @classmethod
    def model(cls) -> type[ConnorsRSIModel]:
        return ConnorsRSIModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ConnorsRSIModel]:
        results = indicators.get_connors_rsi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ConnorsRSIModel](
            {
                "date_time": [value.date for value in results],
                "connors_rsi_close": [value.rsi_close for value in results],
                "connors_rsi_streak": [value.rsi_streak for value in results],
                "connors_percent_rank": [value.percent_rank for value in results],
                "connors_rsi": [value.connors_rsi for value in results],
            }
        )
