import datetime

from polars import DataFrame as PolarDataFrame


class Assertions:
    @staticmethod
    def day_test_assertions(
        actual: PolarDataFrame,
        warmup: datetime.timedelta = datetime.timedelta(minutes=750),
    ) -> None:
        assert actual.null_count().sum_horizontal().item() == 0
        assert actual["date_time"].len() == 24 * 60 - warmup.total_seconds() // 60

        assert actual["date_time"].first() == datetime.datetime(
            2019, 1, 1, 12, 30, tzinfo=datetime.UTC
        )

        assert actual["date_time"].last() == datetime.datetime(
            2019, 1, 1, 23, 59, tzinfo=datetime.UTC
        )
