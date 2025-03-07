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
    engine: Literal["pandas"],
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pd.DataFrame: ...
@overload
def penguins(
    engine: Literal["polars"],
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[False] = False,
) -> pl.DataFrame: ...
@overload
def penguins(
    engine: Literal["polars"],
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> pl.LazyFrame: ...
@overload
def penguins(
    engine: Literal["dask"],
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: Literal[True] = True,
) -> dd.DataFrame: ...
@overload
def air_temperature(
    engine: Literal["xarray"],
    *,
    engine_kwargs: dict[str, Any] | None = None,
    # lazy: Literal[False] = False,
) -> xr.Dataset: ...
