from abc import ABC, abstractmethod
from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from pydantic import BaseModel
from stock_indicators import Quote

from baikal.common.trade.models import TimeSeries


class Indicator[C: BaseModel, M: TimeSeries](ABC):
    def __init__(self, config: C) -> None:
        self._config = config

    @classmethod
    @abstractmethod
    def model(cls) -> type[M]: ...

    @abstractmethod
    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[M]: ...
