from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ConnorsRSIConfig(BaseModel):
    rsi_periods: int = 3
    streak_periods: int = 2
    rank_periods: int = 100


class ConnorsRSIModel(TimeSeries):
    connors_rsi_close: Float64  # [0; 1]
    connors_rsi_streak: Float64  # [0; 1]
    connors_percent_rank: Float64  # [0; 1]
    connors_rsi: Float64  # [0; 1]


class ConnorsRSI(Indicator[ConnorsRSIConfig, ConnorsRSIModel]):
    @classmethod
    def model(cls) -> type[ConnorsRSIModel]:
        return ConnorsRSIModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "connors_rsi_close": FieldMetadata(range_type=RangeType.BOUNDED),
            "connors_rsi_streak": FieldMetadata(range_type=RangeType.BOUNDED),
            "connors_percent_rank": FieldMetadata(range_type=RangeType.BOUNDED),
            "connors_rsi": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ConnorsRSIModel]:
        results = indicators.get_connors_rsi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ConnorsRSIModel](
            {
                "date_time": [value.date for value in results],
                "connors_rsi_close": [value.rsi_close / 100 for value in results],
                "connors_rsi_streak": [value.rsi_streak / 100 for value in results],
                "connors_percent_rank": [value.percent_rank / 100 for value in results],
                "connors_rsi": [value.connors_rsi / 100 for value in results],
            }
        )
