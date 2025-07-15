from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ADXConfig(BaseModel):
    lookback_periods: int = 14


class ADXModel(TimeSeries):
    adx_pdi: Float64  # [-1; 1]
    adx_mdi: Float64  # [-1; 1]
    adx_adx: Float64  # [-1; 1]
    adx_adxr: Float64  # [-1; 1]


class ADX(Indicator[ADXConfig, ADXModel]):
    @classmethod
    def model(cls) -> type[ADXModel]:
        return ADXModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "adx_pdi": FieldMetadata(range_type=RangeType.BOUNDED),
            "adx_mdi": FieldMetadata(range_type=RangeType.BOUNDED),
            "adx_adx": FieldMetadata(range_type=RangeType.BOUNDED),
            "adx_adxr": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ADXModel]:
        results = indicators.get_adx(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[ADXModel](
            {
                "date_time": [value.date for value in results],
                "adx_pdi": [value.pdi / 100 for value in results],
                "adx_mdi": [value.mdi / 100 for value in results],
                "adx_adx": [value.adx / 100 for value in results],
                "adx_adxr": [value.adxr / 100 for value in results],
            }
        )
