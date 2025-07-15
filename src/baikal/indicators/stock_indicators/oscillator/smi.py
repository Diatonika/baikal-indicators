from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class SMIConfig(BaseModel):
    lookback_periods: int = 13
    first_smooth_periods: int = 25
    second_smooth_periods: int = 2
    signal_periods: int = 3


class SMIModel(TimeSeries):
    smi: Float64  # [-1; 1]
    smi_signal: Float64  # [-1; 1]


class SMI(Indicator[SMIConfig, SMIModel]):
    @classmethod
    def model(cls) -> type[SMIModel]:
        return SMIModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "smi": FieldMetadata(range_type=RangeType.BOUNDED),
            "smi_signal": FieldMetadata(range_type=RangeType.BOUNDED),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[SMIModel]:
        results = indicators.get_smi(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        return DataFrame[SMIModel](
            {
                "date_time": [value.date for value in results],
                "smi": [value.smi / 100 for value in results],
                "smi_signal": [value.signal / 100 for value in results],
            }
        )
