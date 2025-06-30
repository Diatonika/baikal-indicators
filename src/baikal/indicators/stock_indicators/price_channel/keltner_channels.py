from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class KeltnerChannelsConfig(BaseModel):
    ema_periods: int = 20
    multiplier: float = 2.0
    atr_periods: int = 10


class KeltnerChannelsModel(TimeSeries):
    keltner_upper: Float64
    keltner_center: Float64
    keltner_lower: Float64
    keltner_width: Float64


class KeltnerChannels(Indicator[KeltnerChannelsConfig, KeltnerChannelsModel]):
    @classmethod
    def model(cls) -> type[KeltnerChannelsModel]:
        return KeltnerChannelsModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[KeltnerChannelsModel]:
        results = indicators.get_keltner(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[KeltnerChannelsModel](
            {
                "date_time": [value.date for value in results],
                "keltner_upper": [value.upper_band for value in results],
                "keltner_center": [value.center_line for value in results],
                "keltner_lower": [value.lower_band for value in results],
                "keltner_width": [value.width for value in results],
            }
        )
