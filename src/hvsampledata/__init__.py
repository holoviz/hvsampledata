"""
hvsampledata

Shared datasets for the HoloViz projects

Currently available datasets:

| Name             | Type    | Online |
| ---------------- | ------- | ------ |
| penguins         | Tabular | No     |
| large_timeseries | Tabular | Yes    |
| airplane         | Gridded | No     |

"""

from __future__ import annotations

from typing import Any

from .__version import __version__
from ._util import _load_gridded, _load_tabular


# Tabular data
def penguins(
    *,
    engine: str | None = None,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: bool = False,
):
    """This is the penguins dataset"""
    return _load_tabular(
        "penguins.csv",
        engine=engine,
        engine_kwargs=engine_kwargs,
        lazy=lazy,
    )


def large_timeseries(
    *,
    engine: str | None = None,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: bool = False,
):
    """
    This is the large timeseries dataset

    First time running this it will download the data.
    """

    return _load_tabular(
        "https://datasets.holoviz.org/sensor/v1/data.parq",
        format="parquet",
        engine=engine,
        engine_kwargs=engine_kwargs,
        lazy=lazy,
    )


# Gridded data
def airplane(
    *,
    engine: str | None = None,
    engine_kwargs: dict[str, Any] | None = None,
):
    """
    This is the airplane.tif

    First time running this it will download the data.
    """

    return _load_gridded(
        "airplane90.tif",
        format="dataset",
        engine=engine,
        engine_kwargs=engine_kwargs,
    )


__all__ = ("__version__", "airplane", "large_timeseries", "penguins")
