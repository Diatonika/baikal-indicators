from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float32, Float64
from pydantic import BaseModel
from stock_indicators import EndType, Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ATRTrailingStopConfig(BaseModel):
    lookback_periods: int = 21
    multiplier: float = 3
    end_type: EndType = EndType.CLOSE


class ATRTrailingStopModel(TimeSeries):
    atr_stop: Float64  # ABSOLUTE
    atr_buy_stop: Float32  # 0 / 1
    atr_sell_stop: Float32  # 0 / 1


class ATRTrailingStop(Indicator[ATRTrailingStopConfig, ATRTrailingStopModel]):
    @classmethod
    def model(cls) -> type[ATRTrailingStopModel]:
        return ATRTrailingStopModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "atr_stop": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "atr_buy_stop": FieldMetadata(range_type=RangeType.BOUNDED),
            "atr_sell_stop": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ATRTrailingStopModel]:
        results = indicators.get_atr_stop(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ATRTrailingStopModel](
            {
                "date_time": [value.date for value in results],
                "atr_stop": [value.atr_stop for value in results],
                "atr_buy_stop": [int(value.buy_stop is not None) for value in results],
                "atr_sell_stop": [
                    int(value.sell_stop is not None) for value in results
                ],
            }
        )
