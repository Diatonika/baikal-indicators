from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel, Field as PydanticField
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class IchimokuCloudConfig(BaseModel):
    tenkan_periods: int = 9
    kijun_periods: int = 26
    senkou_b_periods: int = 52

    senkou_offset: int = PydanticField(
        default_factory=lambda data: data["kijun_periods"]
    )

    chikou_offset: int = PydanticField(
        default_factory=lambda data: data["kijun_periods"]
    )


class IchimokuCloudModel(TimeSeries):
    tenkan_sen: Float64  # ABSOLUTE
    kijun_sen: Float64  # ABSOLUTE
    senkou_span_a: Float64  # ABSOLUTE
    senkou_span_b: Float64  # ABSOLUTE


class IchimokuCloud(Indicator[IchimokuCloudConfig, IchimokuCloudModel]):
    @classmethod
    def model(cls) -> type[IchimokuCloudModel]:
        return IchimokuCloudModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "tenkan_sen": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "kijun_sen": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "senkou_span_a": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "senkou_span_b": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[IchimokuCloudModel]:
        results = indicators.get_ichimoku(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods(
            max(
                self._config.tenkan_periods,
                self._config.kijun_periods,
                self._config.senkou_b_periods,
            )
            + 150
        )

        return DataFrame[IchimokuCloudModel](
            {
                "date_time": [value.date for value in results],
                "tenkan_sen": [value.tenkan_sen for value in results],
                "kijun_sen": [value.kijun_sen for value in results],
                "senkou_span_a": [value.senkou_span_a for value in results],
                "senkou_span_b": [value.senkou_span_b for value in results],
            }
        )
