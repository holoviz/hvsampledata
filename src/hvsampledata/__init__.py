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
    """Penguins tabular dataset.

    Parameters
    ----------
    engine : str, optional
        Engine used to read the dataset. "pandas" or "polars" for eager dataframes,
        "polars" or "dask" for lazy dataframes (lazy=True). By default None,
        automatically selecting the first library found installed.
    engine_kwargs : dict[str, Any], optional
        Additional kwargs to pass to `read_csv`, by default None.
    lazy : bool, optional
        Whether to load the dataset in a lazy container, by default False.

    Description
    -----------
    Tabular records of morphological measurements and demographic information
    from 344 penguins. There are 3 different species of penguins in this dataset,
    collected from 3 islands in the Palmer Archipelago, Antarctica.

    Data were collected and made available by Dr. Kristen Gorman and the Palmer
    Station, Antarctica LTER, a member of the Long Term Ecological Research Network.

    Schema
    ------
    | name              | type    | description                                                         |
    |:------------------|:--------|:--------------------------------------------------------------------|
    | species           | string  | Penguin species (Adelie, Gentoo, or Chinstrap)                      |
    | island            | string  | Island where the penguin was observed (Torgersen, Biscoe, or Dream) |
    | bill_length_mm    | number  | Bill/Beak length in millimeter                                      |
    | bill_depth_mm     | number  | Bill/Beak depth in millimeters                                      |
    | flipper_length_mm | number* | Flipper length in millimeters                                       |
    | body_mass_g       | number* | Body mass in grams                                                  |
    | sex               | string  | Sex of the penguin (male, female or null)                           |
    | year              | integer | Observation year                                                    |

    * float64 for pandas and dask, int64 for polars

    Source
    ------
    `penguins.csv` dataset from the R `palmerpenguins` package
    https://github.com/allisonhorst/palmerpenguins.

    License
    -------
    Data are available by CC-0 license in accordance with the Palmer Station LTER
    Data Policy and the LTER Data Access Policy for Type I data.

    References
    ----------
    Data originally published in:

    Gorman KB, Williams TD, Fraser WR (2014). Ecological sexual dimorphism and
    environmental variability within a community of Antarctic penguins (genus
    Pygoscelis). PLoS ONE 9(3):e90081. https://doi.org/10.1371/journal.pone.0090081
    """
    if engine == "polars":
        engine_kwargs = {"null_values": "NA"} | (engine_kwargs or {})
    tab = _load_tabular(
        "penguins.csv",
        format="csv",
        engine=engine,
        engine_kwargs=engine_kwargs,
        lazy=lazy,
    )
    return tab


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


__all__ = ("__version__", "airplane", "large_timeseries", "penguins")
