from __future__ import annotations

import pytest

import hvsampledata as hvs
from hvsampledata._util import _EAGER_TABULAR_LOOKUP, _LAZY_TABULAR_LOOKUP

datasets = [hvs.apple_stocks, hvs.earthquakes, hvs.synthetic_clusters, hvs.penguins, hvs.stocks]


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
def test_eager_load_earthquakes(engine):
    pytest.importorskip(engine)
    df = hvs.earthquakes(engine=engine)
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
    df = hvs.earthquakes(engine=engine, lazy=True)
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
def test_earthquakes_schema(engine):
    pytest.importorskip(engine)
    df = hvs.earthquakes(engine=engine)
    if engine == "pandas":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "time": np.dtype("datetime64[ns]"),
                "lat": np.dtype("float64"),
                "lon": np.dtype("float64"),
                "depth": np.dtype("float64"),
                "depth_class": pd.CategoricalDtype(
                    categories=["Shallow", "Intermediate", "Deep"], ordered=True
                ),
                "mag": np.dtype("float64"),
                "mag_class": pd.CategoricalDtype(
                    categories=["Light", "Moderate", "Strong", "Major"], ordered=True
                ),
                "place": np.dtype("O"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        expected_schema = {
            "time": pl.Datetime(time_unit="us", time_zone=None),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "depth": pl.Float64,
            "depth_class": pl.Enum(categories=["Shallow", "Intermediate", "Deep"]),
            "mag": pl.Float64,
            "mag_class": pl.Enum(categories=["Light", "Moderate", "Strong", "Major"]),
            "place": pl.String,
        }
        assert df.schema == expected_schema
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_earthquakes_schema_lazy(engine):
    pytest.importorskip(engine)
    df = hvs.earthquakes(engine=engine, lazy=True)
    if engine == "dask":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "time": np.dtype("datetime64[ns]"),
                "lat": np.dtype("float64"),
                "lon": np.dtype("float64"),
                "depth": np.dtype("float64"),
                "depth_class": pd.CategoricalDtype(
                    categories=["Shallow", "Intermediate", "Deep"], ordered=True
                ),
                "mag": np.dtype("float64"),
                "mag_class": pd.CategoricalDtype(
                    categories=["Light", "Moderate", "Strong", "Major"], ordered=True
                ),
                "place": pd.StringDtype("pyarrow"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        expected_schema = {
            "time": pl.Datetime(time_unit="us", time_zone=None),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "depth": pl.Float64,
            "depth_class": pl.Enum(categories=["Shallow", "Intermediate", "Deep"]),
            "mag": pl.Float64,
            "mag_class": pl.Enum(categories=["Light", "Moderate", "Strong", "Major"]),
            "place": pl.String,
        }
        assert df.collect_schema() == expected_schema
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", ["pandas", "polars"])  # dask test keep failing
def test_earthquakes_category_ordering(engine):
    pytest.importorskip(engine)
    df = hvs.earthquakes(engine=engine)
    if engine == "pandas":
        import pandas as pd

        assert isinstance(df["depth_class"].dtype, pd.CategoricalDtype)
        cat_depth = df["depth_class"].cat
        assert cat_depth.ordered
        assert list(cat_depth.categories) == ["Shallow", "Intermediate", "Deep"]

        assert isinstance(df["mag_class"].dtype, pd.CategoricalDtype)
        cat_mag = df["mag_class"].cat
        assert cat_mag.ordered
        assert list(cat_mag.categories) == ["Light", "Moderate", "Strong", "Major"]
    else:
        pytest.importorskip(engine)
        import polars as pl

        schema = df.schema
        expected_depth_type_str = str(pl.Enum(["Shallow", "Intermediate", "Deep"]))
        expected_mag_type_str = str(pl.Enum(["Light", "Moderate", "Strong", "Major"]))
        assert str(schema["depth_class"]) == expected_depth_type_str
        assert str(schema["mag_class"]) == expected_mag_type_str


@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_apple_stocks_schema(engine):
    pytest.importorskip(engine)
    df = hvs.apple_stocks(engine=engine)
    if engine == "pandas":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "date": np.dtype("datetime64[ns]"),
                "open": np.dtype("float64"),
                "high": np.dtype("float64"),
                "low": np.dtype("float64"),
                "close": np.dtype("float64"),
                "volume": np.dtype("int64"),
                "adj_close": np.dtype("float64"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.schema == {
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "adj_close": pl.Float64,
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_apple_stocks_schema_lazy(engine):
    pytest.importorskip(engine)
    df = hvs.apple_stocks(engine=engine, lazy=True)
    if engine == "dask":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "date": np.dtype("datetime64[ns]"),
                "open": np.dtype("float64"),
                "high": np.dtype("float64"),
                "low": np.dtype("float64"),
                "close": np.dtype("float64"),
                "volume": np.dtype("int64"),
                "adj_close": np.dtype("float64"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.collect_schema() == {
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "adj_close": pl.Float64,
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_stocks_schema(engine):
    pytest.importorskip(engine)
    df = hvs.stocks(engine=engine)
    if engine == "pandas":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "date": np.dtype("datetime64[ns]"),
                "Apple": np.dtype("float64"),
                "Amazon": np.dtype("float64"),
                "Google": np.dtype("float64"),
                "Meta": np.dtype("float64"),
                "Microsoft": np.dtype("float64"),
                "Netflix": np.dtype("float64"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.schema == {
            "date": pl.Date,
            "Apple": pl.Float64,
            "Amazon": pl.Float64,
            "Google": pl.Float64,
            "Meta": pl.Float64,
            "Microsoft": pl.Float64,
            "Netflix": pl.Float64,
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_stocks_schema_lazy(engine):
    pytest.importorskip(engine)
    df = hvs.stocks(engine=engine, lazy=True)
    if engine == "dask":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "date": np.dtype("datetime64[ns]"),
                "Apple": np.dtype("float64"),
                "Amazon": np.dtype("float64"),
                "Google": np.dtype("float64"),
                "Meta": np.dtype("float64"),
                "Microsoft": np.dtype("float64"),
                "Netflix": np.dtype("float64"),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.collect_schema() == {
            "date": pl.Date,
            "Apple": pl.Float64,
            "Amazon": pl.Float64,
            "Google": pl.Float64,
            "Meta": pl.Float64,
            "Microsoft": pl.Float64,
            "Netflix": pl.Float64,
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_synthetic_clusters_schema(engine):
    pytest.importorskip(engine)
    df = hvs.synthetic_clusters(engine=engine)
    cats = ["d1", "d2", "d3", "d4", "d5"]
    if engine == "pandas":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "x": np.dtype("float64"),
                "y": np.dtype("float64"),
                "s": np.dtype("float64"),
                "val": np.dtype("int64"),
                "cat": pd.CategoricalDtype(categories=cats, ordered=False),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.schema == {
            "x": pl.Float64,
            "y": pl.Float64,
            "s": pl.Float64,
            "val": pl.Int64,
            "cat": pl.Enum(categories=cats),
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_synthetic_clusters_schema_lazy(engine):
    pytest.importorskip(engine)
    df = hvs.synthetic_clusters(engine=engine, lazy=True)
    cats = ["d1", "d2", "d3", "d4", "d5"]
    if engine == "dask":
        import numpy as np
        import pandas as pd

        expected_dtypes = pd.Series(
            {
                "x": np.dtype("float64"),
                "y": np.dtype("float64"),
                "s": np.dtype("float64"),
                "val": np.dtype("int64"),
                "cat": pd.CategoricalDtype(categories=cats, ordered=False),
            }
        )
        pd.testing.assert_series_equal(df.dtypes, expected_dtypes)
    elif engine == "polars":
        import polars as pl

        assert df.collect_schema() == {
            "x": pl.Float64,
            "y": pl.Float64,
            "s": pl.Float64,
            "val": pl.Int64,
            "cat": pl.Enum(categories=cats),
        }
    else:
        msg = "Not valid engine"
        raise ValueError(msg)


@pytest.mark.parametrize("engine", list(_EAGER_TABULAR_LOOKUP))
def test_synthetic_clusters_total_points(engine):
    pytest.importorskip(engine)
    df = hvs.synthetic_clusters(engine=engine, total_points=10)
    assert len(df) == 10
    with pytest.raises(ValueError, match="total_points must be a multiple of 5"):
        hvs.synthetic_clusters(engine=engine, total_points=11)


@pytest.mark.parametrize("engine", list(_LAZY_TABULAR_LOOKUP))
def test_synthetic_clusters_lazy_total_points(engine):
    pytest.importorskip(engine)
    df = hvs.synthetic_clusters(engine=engine, lazy=True, total_points=10)
    if engine == "polars":
        df = df.collect()
    assert len(df) == 10
    with pytest.raises(ValueError, match="total_points must be a multiple of 5"):
        hvs.synthetic_clusters(engine=engine, lazy=True, total_points=11)
