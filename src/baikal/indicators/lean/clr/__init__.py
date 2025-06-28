from pythonnet import load

load("coreclr")

from baikal.indicators.lean.clr.assemblies import load_assemblies

__all__ = [
    "load_assemblies",
]
