from __future__ import annotations

import pytest

import hvsampledata as hvs
from hvsampledata._util import _EAGER_GRIDDED_LOOKUP

datasets = [hvs.airplane]


@pytest.mark.parametrize("dataset", datasets)
@pytest.mark.parametrize("engine", list(_EAGER_GRIDDED_LOOKUP))
def test_eager_load(dataset, engine):
    pytest.importorskip("xarray")
    df = dataset(engine=engine)
    if engine == "xarray":
        import xarray as xr

        assert isinstance(df, xr.Dataset)
    else:
        msg = f"Not valid engine {engine}"
        raise ValueError(msg)
