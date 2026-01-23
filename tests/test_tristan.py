"""
tests.test_tristan
==================

Minimal integration test and usage documentation for TRISTAN data access.

This module verifies that TRISTAN preclinical rat MRI studies can be
downloaded programmatically from Zenodo and saved locally as compressed
ZIP archives. The test itself exercises only ZIP download behaviour.

The accompanying examples document common user workflows, including:
- downloading a single study
- downloading a selected subset of studies
- downloading the full dataset
- optionally extracting DICOM files and converting to NIfTI format

Core design principle
---------------------
This package is *data-first*. The primary responsibility is to provide
reliable, reproducible access to the archived dataset. Extraction and
format conversion are optional downstream steps provided for user
convenience and are not required for basic data access.

Examples
--------
Single-study download (ZIP only):

>>> from pathlib import Path
>>> from miblab_data.tristan import rat_fetch
>>> out_dir = Path.home() / "Downloads" / "TRISTAN_rat"
>>> rat_fetch(
...     dataset="S01",
...     folder=out_dir,
...     unzip=False,
...     convert=False,
... )

Result:
    ~/Downloads/TRISTAN_rat/S01.zip


Download a selected subset of studies (e.g. S02 and S04):

>>> from pathlib import Path
>>> from miblab_data.tristan import rat_fetch
>>> out_dir = Path.home() / "Downloads" / "TRISTAN_rat"
>>> for study in ["S02", "S04"]:
...     rat_fetch(
...         dataset=study,
...         folder=out_dir,
...         unzip=False,
...         convert=False,
...     )

Result:
    ~/Downloads/TRISTAN_rat/
        ├── S02.zip
        └── S04.zip


Download the full dataset (all studies):

>>> from pathlib import Path
>>> from miblab_data.tristan import rat_fetch
>>> out_dir = Path.home() / "Downloads" / "TRISTAN_rat"
>>> rat_fetch(
...     dataset="all",
...     folder=out_dir,
...     unzip=False,
...     convert=False,
... )

Result:
    ~/Downloads/TRISTAN_rat/
        ├── S01.zip
        ├── S02.zip
        ├── ...
        └── S15.zip


Optional: DICOM extraction and NIfTI conversion
-----------------------------------------------
Downloaded archives contain DICOM files. Users who wish to extract and
convert these data to NIfTI format may enable optional processing using
the ``dicom2nifti`` package.

This functionality is optional, requires ``dicom2nifti`` to be installed
separately, and is provided for convenience only.

>>> from pathlib import Path
>>> from miblab_data.tristan import rat_fetch
>>> out_dir = Path.home() / "Downloads" / "TRISTAN_rat"
>>> rat_fetch(
...     dataset="S01",
...     folder=out_dir,
...     unzip=True,
...     convert=True,
... )

Result:
    - DICOM files extracted under ``out_dir/S01/``
    - Converted NIfTI files written to ``out_dir_nifti/S01/``

Notes:
    - Users may perform extraction or conversion using alternative tools.
    - Conversion is not required for dataset access or reproducibility.
"""

from pathlib import Path

import miblab_data.tristan as tristan


def test_tristan_zip_download(tmp_path: Path) -> None:
    """
    Download a single TRISTAN study ZIP archive.

    This test verifies that:
    - a study archive can be downloaded programmatically
    - repeated calls are safe and idempotent
    - the expected ZIP file exists and is non-empty

    Only a single study (S01) is downloaded to avoid unnecessary network
    load while still validating DOI-based data access.
    """

    download_dir = tmp_path / "tristan_data"

    # First download
    returned = tristan.rat_fetch(
        dataset="S01",
        folder=download_dir,
        unzip=False,
        convert=False,
    )

    # Repeat download (should not fail)
    returned_again = tristan.rat_fetch(
        dataset="S01",
        folder=download_dir,
        unzip=False,
        convert=False,
    )

    # Assertions
    zip_path = download_dir / "S01.zip"

    assert zip_path.exists(), "S01.zip was not downloaded"
    assert zip_path.stat().st_size > 0, "Downloaded ZIP file is empty"
    assert returned, "rat_fetch returned an empty list"
    assert returned_again, "Repeated rat_fetch returned an empty list"