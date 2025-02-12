# !!! THIS FILE IS AUTOGENERATED !!!
# For changes see scripts/generate_typehints.py

from __future__ import annotations

from typing import Any, Literal, overload

import dask.dataframe as dd
import pandas as pd
import polars as pl
import xarray as xr

__version__: str
__all__: tuple[str, ...]

@overload
def penguins(
    engine: None = None,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pd.DataFrame | pl.DataFrame: ...
@overload
def penguins(
    engine: None = None,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> pl.LazyFrame | dd.DataFrame: ...
@overload
def penguins(
    engine: Literal["pandas"] = "pandas",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pd.DataFrame: ...
@overload
def penguins(
    engine: Literal["polars"] = "polars",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pl.DataFrame: ...
@overload
def penguins(
    engine: Literal["polars"] = "polars",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> pl.LazyFrame: ...
@overload
def penguins(
    engine: Literal["dask"] = "dask",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> dd.DataFrame: ...
@overload
def large_time_series(
    engine: None = None,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pd.DataFrame | pl.DataFrame: ...
@overload
def large_time_series(
    engine: None = None,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> pl.LazyFrame | dd.DataFrame: ...
@overload
def large_time_series(
    engine: Literal["pandas"] = "pandas",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pd.DataFrame: ...
@overload
def large_time_series(
    engine: Literal["polars"] = "polars",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pl.DataFrame: ...
@overload
def large_time_series(
    engine: Literal["polars"] = "polars",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> pl.LazyFrame: ...
@overload
def large_time_series(
    engine: Literal["dask"] = "dask",
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> dd.DataFrame: ...
@overload
def airplane(
    engine: None = None,
    engine_kwargs: dict[str, Any] | None = None,
    # lazy: Literal[False] = False,
) -> xr.Dataset: ...
@overload
def airplane(
    engine: Literal["dataset"] = "dataset",
    engine_kwargs: dict[str, Any] | None = None,
    # lazy: Literal[False] = False,
) -> xr.Dataset: ...
