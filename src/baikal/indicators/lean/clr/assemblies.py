from pathlib import Path

from clr import AddReference


def load_assemblies(directory: Path) -> None:
    for file in directory.iterdir():
        AddReference(file.with_suffix("").as_posix())
