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
    | name              | type     | description                                                         |
    |:------------------|:---------|:--------------------------------------------------------------------|
    | species           | category | Penguin species (Adelie, Gentoo, or Chinstrap)                      |
    | island            | category | Island where the penguin was observed (Torgersen, Biscoe, or Dream) |
    | bill_length_mm    | number   | Bill/Beak length in millimeter                                      |
    | bill_depth_mm     | number   | Bill/Beak depth in millimeters                                      |
    | flipper_length_mm | integer  | Flipper length in millimeters                                       |
    | body_mass_g       | integer  | Body mass in grams                                                  |
    | sex               | category | Sex of the penguin (male, female or null)                           |
    | year              | integer  | Observation year                                                    |

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

    Data citations:
    - Adélie penguins: Palmer Station Antarctica LTER and K. Gorman, 2020.
    Structural size measurements and isotopic signatures of foraging among adult male
    and female Adélie penguins (Pygoscelis adeliae) nesting along the Palmer Archipelago
    near Palmer Station, 2007-2009 ver 5. Environmental Data Initiative.
    https://doi.org/10.6073/pasta/98b16d7d563f265cb52372c8ca99e60f (Accessed 2020-06-08).
    - Gentoo penguins: Palmer Station Antarctica LTER and K. Gorman, 2020.
    Structural size measurements and isotopic signatures of foraging among adult male
    and female Gentoo penguin (Pygoscelis papua) nesting along the Palmer Archipelago
    near Palmer Station, 2007-2009 ver 5. Environmental Data Initiative.
    https://doi.org/10.6073/pasta/7fca67fb28d56ee2ffa3d9370ebda689 (Accessed 2020-06-08).
    - Chinstrap penguins: Palmer Station Antarctica LTER and K. Gorman, 2020.
    Structural size measurements and isotopic signatures of foraging among adult male
    and female Chinstrap penguin (Pygoscelis antarcticus) nesting along the Palmer Archipelago
    near Palmer Station, 2007-2009 ver 6. Environmental Data Initiative.
    https://doi.org/10.6073/pasta/c14dfcfada8ea13a17536e73eb6fbe9e (Accessed 2020-06-08).
    """
    if engine in ["pandas", "dask"]:
        dtype = {
            "species": "category",
            "island": "category",
            "flipper_length_mm": "Int64",
            "body_mass_g": "Int64",
            "sex": "category",
        }
        ekwargs = {"dtype": dtype}
        if engine == "dask":
            # To avoid Warning gzip compression does not support breaking apart files
            ekwargs["blocksize"] = None
        engine_kwargs = ekwargs | (engine_kwargs or {})
    elif engine == "polars":
        import polars as pl

        schema_overrides = {
            "species": pl.Categorical,
            "island": pl.Categorical,
            "sex": pl.Categorical,
        }
        engine_kwargs = {"null_values": "NA", "schema_overrides": schema_overrides} | (
            engine_kwargs or {}
        )
    if engine == "dask":
        engine_kwargs = (engine_kwargs or {}) | {"dtype": dtype}
    tab = _load_tabular(
        "penguins.csv",
        format="csv",
        engine=engine,
        engine_kwargs=engine_kwargs,
        lazy=lazy,
    )
    if engine == "dask":
        # So the categories are known/computed.
        tab = tab.categorize(["species", "island", "sex"])
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
