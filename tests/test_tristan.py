"""
tests.test_tristan
==================

Minimal integration test for TRISTAN dataset access.

This module verifies that TRISTAN preclinical rat MRI studies can be
downloaded programmatically from Zenodo as study-level ZIP archives.

Scope
-----
This test validates:
• DOI-based retrieval via ``rat_fetch()``
• idempotent behaviour on repeated calls
• presence and non-zero size of downloaded ZIP files

Out of scope
------------
• ZIP extraction
• DICOM parsing
• NIfTI conversion

These behaviours are intentionally excluded to keep tests fast,
deterministic, and network-light.
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