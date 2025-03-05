from __future__ import annotations


def generate_tabular_overloads(func_name: str) -> str:
    """Generates overloads for a function with the specified engines and lazy options."""
    overloads = []
    ENGINES = [None, "pandas", "polars", "dask"]
    LAZY_OPTIONS = [False, True]

    for engine in ENGINES:
        for lazy in LAZY_OPTIONS:
            if engine is None:
                return_type = (
                    "pl.LazyFrame | dd.DataFrame" if lazy else "pd.DataFrame | pl.DataFrame"
                )
            elif engine == "pandas" and not lazy:
                return_type = "pd.DataFrame"
            elif engine == "polars":
                return_type = "pl.LazyFrame" if lazy else "pl.DataFrame"
            elif engine == "dask" and lazy:
                return_type = "dd.DataFrame"
            else:
                continue

            # Build the overload definition using .format
            overload_def = """\
@overload
def {func_name}(
    engine: {engine_literal} = {engine_repr},
    *,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: {lazy_literal} = {lazy_repr},
) -> {return_type}: ...""".format(
                func_name=func_name,
                engine_literal=f'Literal["{engine}"]' if engine else "None",
                engine_repr=f'"{engine}"' if engine else "None",
                lazy_literal=f"Literal[{lazy}]",
                lazy_repr=lazy,
                return_type=return_type,
            )
            overloads.append(overload_def)

    return "\n".join(overloads)


def generate_gridded_overloads(func_name: str) -> str:
    """Generates overloads for a function with the specified engines and lazy options."""
    overloads = []
    ENGINES = ["xarray"]
    LAZY_OPTIONS = [False]

    for engine in ENGINES:
        for lazy in LAZY_OPTIONS:
            if engine is None:
                return_type = "" if lazy else "xr.Dataset"
            elif engine == "xarray":
                return_type = "xr.Dataset"
            else:
                continue

            # Build the overload definition using .format
            overload_def = """\
@overload
def {func_name}(
    engine: {engine_literal} = {engine_repr},
    *,
    engine_kwargs: dict[str, Any] | None = None,
    # lazy: {lazy_literal} = {lazy_repr},
) -> {return_type}: ...""".format(
                func_name=func_name,
                engine_literal=f'Literal["{engine}"]' if engine else "None",
                engine_repr=f'"{engine}"' if engine else "None",
                lazy_literal=f"Literal[{lazy}]",
                lazy_repr=lazy,
                return_type=return_type,
            )
            overloads.append(overload_def)

    return "\n".join(overloads)


def main():
    header = """\
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
"""

    return "\n".join(
        [
            header,
            generate_tabular_overloads("penguins"),
            generate_gridded_overloads("air_temperature"),
            "",
        ]
    )


if __name__ == "__main__":
    with open("src/hvsampledata/__init__.pyi", "w") as f:
        f.write(main())
