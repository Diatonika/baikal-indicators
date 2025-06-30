from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class ADXConfig(BaseModel):
    lookback_periods: int = 14


class ADXModel(TimeSeries):
    adx_pdi: Float64
    adx_mdi: Float64
    adx_adx: Float64
    adx_adxr: Float64


class ADX(Indicator[ADXConfig, ADXModel]):
    @classmethod
    def model(cls) -> type[ADXModel]:
        return ADXModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ADXModel]:
        results = indicators.get_adx(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ADXModel](
            {
                "date_time": [value.date for value in results],
                "adx_pdi": [value.pdi for value in results],
                "adx_mdi": [value.mdi for value in results],
                "adx_adx": [value.adx for value in results],
                "adx_adxr": [value.adxr for value in results],
            }
        )
