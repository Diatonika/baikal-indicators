from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class DonchianChannelsConfig(BaseModel):
    lookback_periods: int = 20


class DonchianChannelsModel(TimeSeries):
    donchian_upper: Float64
    donchian_center: Float64
    donchian_lower: Float64
    donchian_width: Float64


class DonchianChannels(Indicator[DonchianChannelsConfig, DonchianChannelsModel]):
    @classmethod
    def model(cls) -> type[DonchianChannelsModel]:
        return DonchianChannelsModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[DonchianChannelsModel]:
        results = indicators.get_donchian(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[DonchianChannelsModel](
            {
                "date_time": [value.date for value in results],
                "donchian_upper": [value.upper_band for value in results],
                "donchian_center": [value.center_line for value in results],
                "donchian_lower": [value.lower_band for value in results],
                "donchian_width": [value.width for value in results],
            }
        )
