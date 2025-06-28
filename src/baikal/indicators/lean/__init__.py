from pathlib import Path

from baikal.indicators.lean.clr import load_assemblies

load_assemblies(Path(__file__).resolve().parent / "compiled")

from QuantConnect.Indicators import RelativeStrengthIndex  # noqa: E402

__all__ = ["RelativeStrengthIndex"]
