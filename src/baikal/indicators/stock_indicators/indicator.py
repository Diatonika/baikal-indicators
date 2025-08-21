from abc import ABC, abstractmethod
from collections.abc import Iterable
from enum import StrEnum

from attrs import define
from pandera.typing.polars import DataFrame
from pydantic import BaseModel
from stock_indicators import Quote

from baikal.common.models import TimeSeries


class RangeType(StrEnum):
    BOUNDED = "BOUNDED"
    ABSOLUTE = "ABSOLUTE"


@define
class FieldMetadata:
    range_type: RangeType


class Indicator[C: BaseModel, M: TimeSeries](ABC):
    def __init__(self, config: C) -> None:
        self._config = config

    @classmethod
    @abstractmethod
    def model(cls) -> type[M]: ...

    @classmethod
    @abstractmethod
    def metadata(cls) -> dict[str, FieldMetadata]: ...

    @abstractmethod
    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[M]: ...
