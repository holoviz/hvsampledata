from __future__ import annotations

import pytest

import hvsampledata as hvs
from hvsampledata._util import _EAGER_GRIDDED_LOOKUP

datasets = [hvs.air_temperature]


@pytest.mark.parametrize("dataset", datasets)
@pytest.mark.parametrize("engine", list(_EAGER_GRIDDED_LOOKUP))
def test_eager_load(dataset, engine):
    pytest.importorskip("xarray")
    pytest.importorskip("netCDF4")
    df = dataset(engine=engine)
    if engine == "xarray":
        import xarray as xr

        assert isinstance(df, xr.Dataset)
    else:
        msg = f"Not valid engine {engine}"
        raise ValueError(msg)


def test_air_temperature():
    pytest.importorskip("xarray")
    pytest.importorskip("netCDF4")
    import numpy as np

    ds = hvs.air_temperature("xarray")
    assert ds.air.shape == (20, 25, 53)
    assert ds.attrs == {
        "Conventions": "COARDS",
        "title": "4x daily NMC reanalysis (1948)",
        "description": "Data is from NMC initialized reanalysis\n(4x/day).  These are the 0.9950 sigma level values.",
        "platform": "Model",
        "references": "http://www.esrl.noaa.gov/psd/data/gridded/data.ncep.reanalysis.html",
    }
    assert ds.coords.dtypes == {
        "lat": np.dtype("float32"),
        "lon": np.dtype("float32"),
        "time": np.dtype("datetime64[ns]"),
    }
    assert ds.dtypes == {"air": np.dtype("float64")}
    assert not ds.air.isnull().any().item()
