from __future__ import annotations

import pytest

import hvsampledata as hvs
from hvsampledata._util import _EAGER_TABULAR_LOOKUP, _LAZY_TABULAR_LOOKUP

datasets = [hvs.penguins, hvs.earthquake]


@pytest.mark.parametrize("dataset", datasets)
@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_eager_load(dataset, engine):
    pytest.importorskip(engine)
    df = dataset(engine=engine)
    if engine == "pandas":
        import pandas as pd

        assert isinstance(df, pd.DataFrame)
    elif engine == "polars":
        import polars as pl

        assert isinstance(df, pl.DataFrame)
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("dataset", datasets)
@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_lazy_load(dataset, engine):
    pytest.importorskip(engine)
    df = dataset(engine=engine, lazy=True)
    if engine == "polars":
        import polars as pl

        assert isinstance(df, pl.LazyFrame)
    elif engine == "dask":
        import dask.dataframe as dd

        assert isinstance(df, dd.DataFrame)
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_penguins_schema(engine):
    pytest.importorskip(engine)
    df = hvs.penguins(engine=engine)
    if engine == "pandas":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "species": np.dtype("O"),
                "island": np.dtype("O"),
                "bill_length_mm": np.dtype("float64"),
                "bill_depth_mm": np.dtype("float64"),
                "flipper_length_mm": np.dtype("float64"),
                "body_mass_g": np.dtype("float64"),
                "sex": np.dtype("O"),
                "year": np.dtype("int64"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.schema == {
            "species": pl.String,
            "island": pl.String,
            "bill_length_mm": pl.Float64,
            "bill_depth_mm": pl.Float64,
            "flipper_length_mm": pl.Int64,
            "body_mass_g": pl.Int64,
            "sex": pl.String,
            "year": pl.Int64,
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_penguins_schema_lazy(engine):
    pytest.importorskip(engine)
    df = hvs.penguins(engine=engine, lazy=True)
    if engine == "dask":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "species": pd.StringDtype("pyarrow"),
                "island": pd.StringDtype("pyarrow"),
                "bill_length_mm": np.dtype("float64"),
                "bill_depth_mm": np.dtype("float64"),
                "flipper_length_mm": np.dtype("float64"),
                "body_mass_g": np.dtype("float64"),
                "sex": pd.StringDtype("pyarrow"),
                "year": np.dtype("int64"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.collect_schema() == {
            "species": pl.String,
            "island": pl.String,
            "bill_length_mm": pl.Float64,
            "bill_depth_mm": pl.Float64,
            "flipper_length_mm": pl.Int64,
            "body_mass_g": pl.Int64,
            "sex": pl.String,
            "year": pl.Int64,
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_eager_load_earthquake(engine):
    pytest.importorskip(engine)
    df = hvs.earthquake(engine=engine)
    if engine == "pandas":
        import pandas as pd

        assert isinstance(df, pd.DataFrame)
    elif engine == "polars":
        import polars as pl

        assert isinstance(df, pl.DataFrame)
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_lazy_load_earthquake(engine):
    pytest.importorskip(engine)
    df = hvs.earthquake(engine=engine, lazy=True)
    if engine == "polars":
        import polars as pl

        assert isinstance(df, pl.LazyFrame)
    elif engine == "dask":
        import dask.dataframe as dd

        assert isinstance(df, dd.DataFrame)
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_earthquake_schema(engine):
    pytest.importorskip(engine)
    df = hvs.earthquake(engine=engine)
    if engine == "pandas":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "time": pd.DatetimeTZDtype("ns", "UTC"),
                "latitude": np.dtype("float64"),
                "longitude": np.dtype("float64"),
                "depth": np.dtype("float64"),
                "depth_class": np.dtype("O"),
                "mag": np.dtype("float64"),
                "mag_class": np.dtype("O"),
                "place": np.dtype("O"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        expected_schema = {
            "time": pl.Date,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "depth": pl.Float64,
            "depth_class": pl.String,
            "mag": pl.Float64,
            "mag_class": pl.String,
            "place": pl.String,
        }
        assert df.schema == expected_schema
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_earthquake_schema_lazy(engine):
    pytest.importorskip(engine)
    df = hvs.earthquake(engine=engine, lazy=True)
    if engine == "dask":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "time": pd.DatetimeTZDtype("ns", "UTC"),
                "latitude": np.dtype("float64"),
                "longitude": np.dtype("float64"),
                "depth": np.dtype("float64"),
                "depth_class": pd.StringDtype("pyarrow"),
                "mag": np.dtype("float64"),
                "mag_class": pd.StringDtype("pyarrow"),
                "place": pd.StringDtype("pyarrow"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        expected_schema = {
            "time": pl.Date,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "depth": pl.Float64,
            "depth_class": pl.String,
            "mag": pl.Float64,
            "mag_class": pl.String,
            "place": pl.String,
        }
        assert df.collect_schema() == expected_schema
    else:
        msg = "Not valid engine"
        raise ValueError(msg)
