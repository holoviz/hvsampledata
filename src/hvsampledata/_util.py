from __future__ import annotations

import importlib
import os
from functools import reduce
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from platformdirs import user_cache_path

_DATAPATH = Path(__file__).parent / "_data"
_CACHEPATH = user_cache_path() / "hvsampledata"

_EAGER_TABULAR_LOOKUP = {
    "pandas": {"csv": "read_csv", "parquet": "read_parquet"},
    "polars": {"csv": "read_csv", "parquet": "read_parquet"},
}
_LAZY_TABULAR_LOOKUP = {
    "polars": {"csv": "scan_csv", "parquet": "scan_parquet"},
    "dask": {"csv": "dataframe.read_csv", "parquet": "dataframe.read_parquet"},
}
_EAGER_GRIDDED_LOOKUP = {
    "xarray": {"dataset": "open_dataset", "dataarray": "open_dataarray"},
}


def _get_path(dataset: str) -> Path:
    if dataset.startswith("http"):
        dataset_name = urlsplit(dataset).path.lstrip("/")
        path = _CACHEPATH / dataset_name
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            _download_data(url=dataset, path=path)
        return path
    else:
        path = _DATAPATH / dataset
        return path


def _download_data(*, url, path):
    from urllib3 import PoolManager

    from hvsampledata import __version__

    headers = {"User-Agent": f"hvsampledata {__version__}"}

    try:
        http = PoolManager()
        response = http.request("GET", url, preload_content=False, headers=headers)

        if response.status == 200:
            with open(path, "wb") as f:
                for chunk in response.stream(1024):
                    f.write(chunk)
            print(f"File saved to {path}")
        else:
            print(f"Failed to download file. HTTP Status: {response.status}")
    except Exception:
        print("Failed to download file")
        os.remove(path)
    finally:
        response.release_conn()


def _get_method(*, engine: str | None, format: str, engine_lookups: dict[str, dict[str, str]]):
    # TODO: Should also work with .tar.gz like files
    if isinstance(engine, str):
        mod = importlib.import_module(engine)
        attr = engine_lookups[engine][format]
        if attr.count("."):
            importlib.import_module(".".join([engine, *attr.split(".")[:-1]]))
        return reduce(getattr, attr.split("."), mod)
    else:
        from importlib.util import find_spec

        for tab_engine in engine_lookups:
            if find_spec(tab_engine):
                return _get_method(engine=tab_engine, format=format, engine_lookups=engine_lookups)
        print("No available engines can be imported")


def _load_tabular(
    dataset: str,
    *,
    format: str | None = None,
    engine: str | None = None,
    engine_kwargs: dict[str, Any] | None = None,
    lazy: bool = False,
):
    path = _get_path(dataset)
    format = format or os.fspath(dataset).split(".")[-1]
    engine_lookup = _LAZY_TABULAR_LOOKUP if lazy else _EAGER_TABULAR_LOOKUP
    engine_function = _get_method(engine=engine, format=format, engine_lookups=engine_lookup)
    data = engine_function(path, **(engine_kwargs or {}))
    return data


def _load_gridded(
    dataset: str,
    *,
    format: str | None = None,
    engine: str | None = None,
    engine_kwargs: dict[str, Any] | None = None,
    # lazy=False,  # TODO: Add support for lazy
):
    path = _get_path(dataset)
    format = format or os.fspath(dataset).split(".")[-1]
    engine_lookup = _EAGER_GRIDDED_LOOKUP
    engine_function = _get_method(engine=engine, format=format, engine_lookups=engine_lookup)
    data = engine_function(path, **(engine_kwargs or {}))
    return data
