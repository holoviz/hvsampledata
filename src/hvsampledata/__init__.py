"""hvsampledata: shared datasets for the HoloViz projects.

Currently available datasets:

| Name             | Type    | Included |
| ---------------- | ------- | -------- |
| air_temperature  | Gridded | Yes      |
| earthquakes      | Tabular | Yes      |
| penguins         | Tabular | Yes      |

Use it with:

>>> import hvsampledata
>>> ds = hvsampledata.air_temperature("xarray")
>>> df = hvsampledata.penguins("pandas")

"""

from __future__ import annotations

from typing import Any

from .__version import __version__
from ._util import _load_gridded, _load_tabular

# -----------------------------------------------------------------------------
# Tabular data
# -----------------------------------------------------------------------------


def penguins(
    engine: str,
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: bool = False,
):
    """Penguins tabular dataset.

    Parameters
    ----------
    engine : str
        Engine used to read the dataset. "pandas" or "polars" for eager dataframes,
        "polars" or "dask" for lazy dataframes (lazy=True).
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


def earthquakes(
    engine: str,
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: bool = False,
):
    """Earthquakes tabular dataset.

    Parameters
    ----------
    engine : str
        Engine used to read the dataset. "pandas" or "polars" for eager dataframes,
        "polars" or "dask" for lazy dataframes (lazy=True).
    engine_kwargs : dict[str, Any], optional
        Additional kwargs to pass to `read_csv`, by default None.
    lazy : bool, optional
        Whether to load the dataset in a lazy container, by default False.

    Description
    -----------
    Tabular record of earthquake events from the USGS Earthquake Catalog that provides detailed
    information including parameters such as time, location as latitude/longitude coordinates
    and place name, depth, and magnitude. The dataset contains 596 events.

    Note: The columns `depth_class` and `mag_class` were created by categorizing numerical values from
    the `depth` and `mag` columns in the original dataset using custom-defined binning:

    Depth Classification

    | depth     | depth_class  |
    |-----------|--------------|
    | Below 70  | Shallow      |
    | 70 - 300  | Intermediate |
    | Above 300 | Deep         |

    Magnitude Classification

    | mag         | mag_class |
    |-------------|-----------|
    | 3.9 - <4.9  | Light     |
    | 4.9 - <5.9  | Moderate  |
    | 5.9 - <6.9  | Strong    |
    | 6.9 - <7.9  | Major     |


    Schema
    ------
    | name        | type       | description                                                         |
    |:------------|:-----------|:--------------------------------------------------------------------|
    | time        | datetime   | UTC Time when the event occurred.                                   |
    | lat         | float      | Decimal degrees latitude. Negative values for southern latitudes.   |
    | lon         | float      | Decimal degrees longitude. Negative values for western longitudes   |
    | depth       | float      | Depth of the event in kilometers.                                   |
    | depth_class | category   | The depth category derived from the depth column.                   |
    | mag         | float      | The magnitude for the event.                                        |
    | mag_class   | category   | The magnitude category derived from the mag column.                 |
    | place       | string     | Textual description of named geographic region near to the event.   |

    Source
    ------
    `earthquakes.csv` dataset courtesy of the U.S. Geological Survey
    https://www.usgs.gov/programs/earthquake-hazards, with 4 months of data selected
    from April to July 2024 along the Pacific Ring of Fire region (lat=(-10,10), lon=(110,140))

    License
    -------
    U.S. Public domain
    Data available from U.S. Geological Survey, National Geospatial Program.
    Visit the USGS at https://usgs.gov.

    """
    depth_order = ["Shallow", "Intermediate", "Deep"]
    mag_order = ["Light", "Moderate", "Strong", "Major"]
    engine_kwargs = engine_kwargs or {}

    # convert `time` column to datetime and `mag_class` and `depth_class` to categories
    if engine == "polars":
        import polars as pl

        engine_kwargs = {
            "try_parse_dates": True,
            "schema_overrides": {
                "depth_class": pl.Enum(depth_order),
                "mag_class": pl.Enum(mag_order),
            },
        } | engine_kwargs
    else:
        import pandas as pd

        engine_kwargs = {
            "parse_dates": ["time"],
            "dtype": {
                "depth_class": pd.api.types.CategoricalDtype(categories=depth_order, ordered=True),
                "mag_class": pd.api.types.CategoricalDtype(categories=mag_order, ordered=True),
            },
        } | engine_kwargs
    return _load_tabular(
        "earthquakes.csv",
        format="csv",
        engine=engine,
        engine_kwargs=engine_kwargs,
        lazy=lazy,
    )


# -----------------------------------------------------------------------------
# Gridded data
# -----------------------------------------------------------------------------


def air_temperature(
    engine: str,
    *,
    engine_kwargs=None,
):
    """Air Temperature gridded dataset.

    Parameters
    ----------
    engine : str
        Engine used to read the dataset, "xarray" is the only option available.
    engine_kwargs : dict[str, Any], optional
        Additional kwargs to pass to `xarray.open_dataset`, by default None.

    Description
    -----------
    The NCEP/NCAR Reanalysis 1 project is using a state-of-the-art analysis/forecast
    system to perform data assimilation using past data from 1948 to the present.

    This dataset was created by temporally resampling the `air_temperature` dataset
    made available by xarray-data, itself being a spatial and temporal subset of
    the original data. It only includes the air temperature variable.

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
    - air: [time|lat|lon], float64, air temperature in Kelvin

    Source
    ------
    `air_temperature.nc` dataset from the `xarray-data` Github repository
    https://github.com/pydata/xarray-data, resampled to 20 timestamps between
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
    ds = _load_gridded(
        "air_temperature_small.nc",
        format="dataset",
        engine=engine,
        engine_kwargs=engine_kwargs,
    )
    if str(ds.dtypes["air"]) == "float32":
        # Float32 with older version of xarray/netcdf4.
        ds = ds.astype("float64")
    return ds


__all__ = ("__version__", "air_temperature", "earthquakes", "penguins")
