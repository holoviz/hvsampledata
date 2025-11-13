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

# Add new entries when adding remote datasets
_KNOWN_HASHES = {
    "https://datasets.holoviz.org/nyc_taxi/v2/nyc_taxi_wide.parq": "984e130de1b2b679f46c79f5263851e8abde13c5becef5d5e0545e5dd61555be",
}

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


class HashMismatchError(RuntimeError):
    """Raised when a file's hash doesn't match the expected value."""


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

    http = PoolManager()
    response = None

    try:
        response = http.request("GET", url, preload_content=False, headers=headers)

        if response.status == 200:
            # Verify hash if available
            expected_hash = _KNOWN_HASHES.get(url)
            sha256_hash = None
            if expected_hash:
                import hashlib

                sha256_hash = hashlib.sha256()

            with open(path, "wb") as f:
                for chunk in response.stream(1024):
                    f.write(chunk)
                    if sha256_hash is not None:
                        sha256_hash.update(chunk)

            if sha256_hash is not None:
                actual_hash = sha256_hash.hexdigest()
                if actual_hash != expected_hash:
                    os.remove(path)
                    msg = (
                        f"Hash mismatch for {url}. "
                        f"Expected: {expected_hash}, Got: {actual_hash}. "
                        f"File may be corrupted."
                    )
                    raise HashMismatchError(msg)

            print(f"File saved to {path}")
        else:
            print(f"Failed to download file. HTTP Status: {response.status}")
    except Exception:
        print("Failed to download file")
        if path.exists():
            os.remove(path)
        raise
    finally:
        if response is not None:
            response.release_conn()
        http.clear()


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
    download_only: bool = False,
):
    # Handle download-only mode for remote files
    if download_only:
        return _get_path(dataset)

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
