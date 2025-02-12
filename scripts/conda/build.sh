#!/usr/bin/env bash

set -euxo pipefail

PACKAGE="hvsampledata"

python -m build --sdist .

VERSION=$(python -c "from src.$PACKAGE import _version; print(_version.__version__)")
export VERSION

conda build scripts/conda/recipe --no-anaconda-upload --no-verify -c conda-forge --package-format 2

mv "$CONDA_PREFIX/conda-bld/noarch/$PACKAGE-$VERSION-py_0.conda" dist
