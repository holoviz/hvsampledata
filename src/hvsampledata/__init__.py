"""hvsampledata: shared datasets for the HoloViz projects.

Currently available datasets:

| Name               | Type    | Included |
| ------------------ | ------- | -------- |
| air_temperature    | Gridded | Yes      |
| apple_stocks       | Tabular | Yes      |
| earthquakes        | Tabular | Yes      |
| landsat_rgb        | Gridded | Yes      |
| penguins           | Tabular | Yes      |
| penguins_rgba      | Gridded | Yes      |
| stocks             | Tabular | Yes      |
| synthetic_clusters | Tabular | Yes      |

Use it with:

>>> import hvsampledata
>>> ds = hvsampledata.air_temperature("xarray")
>>> df = hvsampledata.penguins("pandas")

"""

from __future__ import annotations

from typing import Any

from .__version import __version__
from ._util import _DATAPATH, _load_gridded, _load_tabular

# -----------------------------------------------------------------------------
# Tabular data
# -----------------------------------------------------------------------------


def synthetic_clusters(
    engine: str,
    *,
    lazy: bool = False,
    total_points: int = 1_000_000,
):
    """Large tabular dataset with 5 synthetic clusters generated from a normal
    distribution with a scale distributed roughly according to a power law.

    Parameters
    ----------
    engine : str
        Engine used to read the dataset. "pandas" or "polars" for eager dataframes,
        "polars" or "dask" for lazy dataframes (polars need lazy=True).
    lazy : bool, optional
        Whether to load the dataset in a lazy container, by default False.
    total_points: int, default=1_000_000
        Total number of points in the dataset returned, must be a multiple of 5.

    Description
    -----------
    Synthetic dataset that contains 5 clusters, each cluster generated from a
    normal distribution centered on the first two values of these tuples (e.g.
    `x,y=2,2` for the first cluster) and with a standard deviation equal to
    the third value (e.g. `s=0.03` for the first cluster):

    ```python
    clusters = [
        (2, 2, 0.03, 0, "d1"),
        (2, -2, 0.10, 1, "d2"),
        (-2, -2, 0.50, 2, "d3"),
        (-2, 2, 1.00, 3, "d4"),
        (0, 0, 3.00, 4, "d5"),
    ]
    ```

    The standard deviation / scale is distributed roughly according to a power
    law, with `s ~= 0.005 * x^4`.

    Schema
    ------
    | name | type        | description                                              |
    |:-----|:------------|:---------------------------------------------------------|
    | x    | number      | x coordinate                                             |
    | y    | number      | y coordinate                                             |
    | s    | number      | standard deviation of the distribution                   |
    | val  | integer     | integer value per distribution, one of 0, 1, 2, 3, 4     |
    | cat  | categorical | string value per distribution, one of d1, d2, d3, d4, d5 |
    """

    clusters = [
        (2, 2, 0.03, 0, "d1"),
        (2, -2, 0.10, 1, "d2"),
        (-2, -2, 0.50, 2, "d3"),
        (-2, 2, 1.00, 3, "d4"),
        (0, 0, 3.00, 4, "d5"),
    ]

    if total_points % 5:
        msg = "total_points must be a multiple of 5"
        raise ValueError(msg)
    points_per_cluster = total_points // 5
    cats = ["d1", "d2", "d3", "d4", "d5"]
    if engine in ["pandas", "dask"]:
        import numpy as np
        import pandas as pd

        cat_dtype = pd.CategoricalDtype(categories=cats, ordered=False)

        def create_synthetic_dataset(x, y, s, val, cat, cat_dtype, num, dask=False):
            seed = np.random.default_rng(1)
            df = pd.DataFrame(
                {
                    "x": seed.normal(x, s, num),
                    "y": seed.normal(y, s, num),
                    "s": s,
                    "val": val,
                    "cat": pd.Series([cat] * num, dtype=cat_dtype),
                }
            )
            if dask:
                import dask.dataframe as dd

                return dd.from_pandas(df, npartitions=2)
            return df

        if engine == "pandas":
            func_concat = pd.concat
            kwargs_concat = {"ignore_index": True}
        elif engine == "dask":
            import dask.dataframe as dd

            func_concat = dd.concat
            kwargs_concat = {"axis": 0}  # , "interleave_partitions":  True}
        df = func_concat(
            [
                create_synthetic_dataset(
                    x, y, s, val, cat, cat_dtype, points_per_cluster, dask=engine == "dask"
                )
                for x, y, s, val, cat in clusters
            ],
            **kwargs_concat,
        )
        return df
    elif engine == "polars":
        import random

        import polars as pl

        def create_synthetic_dataset(x, y, s, val, cat, num, lazy=False):
            pdf = pl.DataFrame(
                {
                    "x": [random.gauss(x, s) for _ in range(num)],
                    "y": [random.gauss(y, s) for _ in range(num)],
                    "s": [s] * num,
                    "val": [val] * num,
                    "cat": pl.Series([cat] * num).cast(pl.Enum(cats)),
                }
            )
            if lazy:
                return pdf.lazy()
            return pdf

        # Use a global StringCache so categoricals are shared
        with pl.StringCache():
            dfp = pl.concat(
                [
                    create_synthetic_dataset(x, y, s, val, cat, points_per_cluster, lazy=lazy)
                    for x, y, s, val, cat in clusters
                ],
                how="vertical",
            )
        return dfp


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


def apple_stocks(
    engine: str,
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: bool = False,
):
    """Apple Inc. (AAPL) stocks dataset.

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
    Tabular record of Apple Inc. (AAPL) daily stock trading data from the U.S. stock market
    from January 2019 to December 2024.
    Each row represents a single trading day with pricing and volume information.

    This dataset contains 1509 rows and was collected from public news sources.

    Schema
    ------
    | name       | type     | description                                               |
    |:-----------|:-------- |:----------------------------------------------------------|
    | date       | datetime | The trading date                                          |
    | open       | float    | Opening price of the stock on that day                    |
    | high       | float    | Highest price of the stock during the trading day         |
    | low        | float    | Lowest price of the stock during the trading day          |
    | close      | float    | Closing price of the stock on that day                    |
    | volume     | integer  | Number of shares traded                                   |
    | adj_close  | float    | Adjusted closing price reflecting splits and dividends    |

    Source
    ------
    `apple_stocks.csv` dataset generated from historical data for Apple Inc. (AAPL) sourced from Yahoo Finance.

    License
    -------
    Data provided for demonstration and educational purposes only.
    Users must ensure compliance with the original data providers terms of use.
    See https://legal.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.html
    """
    engine_kwargs = engine_kwargs or {}
    # convert `date` column to datetime object
    if engine == "polars":
        engine_kwargs = {
            "try_parse_dates": True,
        } | engine_kwargs
    else:
        engine_kwargs = {
            "parse_dates": ["date"],
        } | engine_kwargs
    return _load_tabular(
        "apple_stocks.csv",
        format="csv",
        engine=engine,
        engine_kwargs=engine_kwargs,
        lazy=lazy,
    )


def stocks(
    engine: str,
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: bool = False,
):
    """Selected stocks dataset.

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
    Tabular dataset containing weekly rebased stock prices for selected Tech companies:
    Apple, Amazon, Google, Meta, Microsoft, and Netflix from January 2019 to December 2023.
    The stock prices have been rebased to start at 1.0 from the first row.

    This dataset contains 261 rows and can be used to compare the relative performance of
    each company's stock over that time period.

    Schema
    ------
    | name       | type     | description                            |
    |:-----------|:---------|:---------------------------------------|
    | date       | datetime | The trading date (weekly interval)     |
    | Apple      | float    | Normalized price of Google stock       |
    | Amazon     | float    | Normalized price of Apple stock        |
    | Google     | float    | Normalized price of Amazon stock       |
    | Meta       | float    | Normalized price of Facebook stock     |
    | Microsoft  | float    | Normalized price of Netflix stock      |
    | Netflix    | float    | Normalized price of Microsoft stock    |

    Source
    ------
    `stocks.csv` dataset derived from historical stock prices of selected Tech companies,
    sourced from Yahoo Finance and rebased for comparative analysis.

    License
    -------
    Data provided for educational and demonstration purposes only.
    Users must ensure compliance with the original data providers terms of use.
    See https://legal.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.html
    """
    engine_kwargs = engine_kwargs or {}
    # convert `date` column to datetime object
    if engine == "polars":
        engine_kwargs = {
            "try_parse_dates": True,
        } | engine_kwargs
    else:
        engine_kwargs = {
            "parse_dates": ["date"],
        } | engine_kwargs
    return _load_tabular(
        "stocks.csv",
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


def penguins_rgba(
    engine: str,
):
    """Penguins RGBA image.

    Parameters
    ----------
    engine : str
        Engine used to read the dataset, only `xarray` is available.

    Description
    -----------
    This dataset is an image of two adult Emperor penguins with a juvenile in
    Antarctica, read from a 100x100 PNG file into an xarray Dataset object.

    Dimensions:
    - y: int64, 100 values
    - x: int64, 100 values
    - channel: U1, 4 values

    Variables:
    - rgba: [y|x|channel]: uint8

    Source
    ------
    Original PNG file obtained from
    https://en.wikipedia.org/wiki/Emperor_penguin#/media/File:Aptenodytes_forsteri_-Snow_Hill_Island,_Antarctica_-adults_and_juvenile-8.jpg
    Original picture from Ian Duffy from UK - Animal Portraits.
    The original file was reduced to 100x100.

    License
    -------
    Creative Commons Attribution 2.0 Generic
    https://creativecommons.org/licenses/by/2.0/deed.en
    """

    if engine != "xarray":
        msg = "xarray is the only supported engine"
        raise ValueError(msg)

    import numpy as np
    import xarray as xr
    from PIL import Image

    with Image.open(_DATAPATH / "penguins.png") as img:
        img_array = np.array(img)

    return xr.DataArray(
        img_array,
        dims=["y", "x", "channel"],
        coords={
            "y": np.arange(img_array.shape[0]),
            "x": np.arange(img_array.shape[1]),
            "channel": ["R", "G", "B", "A"],
        },
        name="rgba",
    ).to_dataset()


def landsat_rgb(
    engine: str,
    *,
    engine_kwargs=None,
):
    """Landsat 7 RGB tile.

    Parameters
    ----------
    engine : str
        Engine used to read the dataset, only `rioxarray` is available.
    engine_kwargs : dict[str, Any], optional
        Additional kwargs to pass to `rioxarray.open_rasterio`, by default None.

    Description
    -----------
    This dataset is a satellite image from the USGS Landsat 7 mission, saved
    as RGB GeoTIFF file. The area covers part of the Bahama, with pixel size of
    ~1 km and a grid of 237x215. The nodata value is 0. Its CRS is WGS 84 / UTM
    zone 18N (EPSG:32618).

    Dimensions:
    - band: int64, 3 values
    - x: float64, 237 values
    - y: float64, 215 values

    Extra coords:
    - spatial_ref: int64

    Variables:
    - rgb: [band|y|x]: uint8

    Source
    ------
    Original GeoTIFF file obtained from the `rioxarray` Github repository
    https://github.com/rasterio/rasterio/blob/95f1f5fb55be6763bf987f42eb54c644604c6d3d/tests/data/RGB.byte.tif

    The original file was downsampled with this GDAL command to make it smaller:

        gdal_translate -outsize 30% 30% RGB.byte.tif landsat_rgb.tif

    Original data from the USGS Landsat satellite 7 mission.

    License
    -------
    rasterio image RGB.byte.tif is licensed under the CC0 1.0 Universal (CC0 1.0)
    Public Domain Dedication: http://creativecommons.org/publicdomain/zero/1.0/.
    """
    if engine != "rioxarray":
        msg = "rioxarray is the only supported engine"
        raise ValueError(msg)

    import rioxarray

    fp = _DATAPATH / "landsat_rgb.tif"
    engine_kwargs = {} if engine_kwargs is None else engine_kwargs

    return rioxarray.open_rasterio(fp, **engine_kwargs).to_dataset(name="rgb")


__all__ = (
    "__version__",
    "air_temperature",
    "apple_stocks",
    "earthquakes",
    "landsat_rgb",
    "penguins",
    "penguins_rgba",
    "stocks",
    "synthetic_clusters",
)
