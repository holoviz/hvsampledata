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
    engine: str | None = None,
    *,
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
    engine: str | None = None,
    *,
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
def air_temperature_small(
    engine="xarray",
    *,
    engine_kwargs=None,
):
    """Small Air Temperature gridded dataset.

    Parameters
    ----------
    engine : str, optional
        Engine used to read the dataset, by default 'xarray'.
    engine_kwargs : dict[str, Any], optional
        Additional kwargs to pass to `xarray.open_dataset`, by default None.

    Description
    -----------
    The NCEP/NCAR Reanalysis 1 project is using a state-of-the-art analysis/forecast
    system to perform data assimilation using past data from 1948 to the present.

    This dataset is a temporal and spatial subset of the original data, including
    only the air temperature variable.

    Temporal coverage:
    - Every 6 hours, starting from 00:00
    - 2014-02-24 to 2014-02-28

    Spatial coverage:
    - 2.5 degree x 2.5 degree grid (lon:53xlat:25)
    - ~ North America (lon:[200-330], lat:[15-75])

    Dimensions:
    - lat: float32, 25 values
    - lon: float32, 53 values
    - time: datetime64[ns], 20 values

    Variables:
    - air: [time,lat,lon], float64, air temperature in Kelvin

    Source
    ------
    `air_temperature.nc` dataset from the `xarray-data` Github repository
    https://github.com/pydata/xarray-data, re-sampled to 20 timestamps between
    2014-02-24 and 2014-02-28.

    Original data from:
    https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.html

    License
    -------
    NCEP-NCAR Reanalysis 1 data provided by the NOAA PSL, Boulder, Colorado, USA,
    from their website at https://psl.noaa.gov

    References
    ----------
    Kalnay et al.,The NCEP/NCAR 40-year reanalysis project, Bull. Amer. Meteor. Soc., 77, 437-470, 1996
    """
    return _load_gridded(
        "air_temperature_small.nc",
        format="dataset",
        engine=engine,
        engine_kwargs=engine_kwargs,
    )


def airplane(
    engine: str | None = None,
    *,
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


__all__ = (
    "__version__",
    "air_temperature_small",
    "airplane",
    "large_timeseries",
    "penguins",
)
