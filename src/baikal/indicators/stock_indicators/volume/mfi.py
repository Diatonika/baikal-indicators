from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class MFIConfig(BaseModel):
    lookback_periods: int = 14


class MFIModel(TimeSeries):
    mfi: Float64


class MFI(Indicator[MFIConfig, MFIModel]):
    @classmethod
    def model(cls) -> type[MFIModel]:
        return MFIModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[MFIModel]:
        results = indicators.get_mfi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[MFIModel](
            {
                "date_time": [value.date for value in results],
                "mfi": [value.mfi for value in results],
            }
        )
