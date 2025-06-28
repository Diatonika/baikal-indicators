from baikal.indicators.lean import RelativeStrengthIndex


def test_lean_indicator() -> None:
    _ = RelativeStrengthIndex(20)
