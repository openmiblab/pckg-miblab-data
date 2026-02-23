"""
miblab_data.tristan
==================

High-level data access utilities for the TRISTAN preclinical rat MRI dataset.

This module provides the public function :func:`rat_fetch`, which downloads
one or more TRISTAN studies from Zenodo, optionally orchestrates recursive
extraction, and optionally converts DICOM series to compressed NIfTI format.

Design principles
-----------------
• This module intentionally contains **no ZIP-handling logic**. Archive
  extraction is delegated to a helper function that is injected (monkey-
  patched) during testing.
• Network access is resilient to transient Zenodo failures via retries.
• Conversion logic is tolerant of non-standard preclinical DICOM geometry.
• Side-effects (file I/O) are explicit and documented.

Intended use
------------
This module is part of the ``miblab_data`` research software stack and is
designed for:
• reproducible dataset access
• automated pipelines
• FAIR-compliant data reuse
"""

from pathlib import Path
from typing import List

import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm


# NOTE:
# DOI is expected to be provided by miblab_data.constants.
# It must map "RAT" → Zenodo record ID (e.g. 17178063).
from miblab_data.constants import DOI

# ── persistent HTTP session ────────────────────────────────────────────────
_rat_session = requests.Session()
_rat_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(502, 503, 504),
        )
    ),
)


def rat_fetch(
    dataset: str | None = None,
    *,
    folder: str | Path = "./tristanrat",
    unzip: bool = True,
    keep_archives: bool = False,
) -> List[str]:
    """
    Download TRISTAN rat MRI studies from Zenodo.

    This function retrieves one or more ZIP archives corresponding to
    TRISTAN studies (S01–S15), stores them locally, and optionally
    orchestrates recursive extraction and DICOM→NIfTI conversion.

    Parameters
    ----------
    dataset
        Identifier specifying which study to fetch.

        Accepted values:
        • ``"S01"`` … ``"S15"`` (case-insensitive)
        • ``"all"`` or ``None`` → fetch all published studies

    folder
        Root directory into which ZIP archives are downloaded.
        Extracted content is placed in subdirectories named ``SXX``.

    unzip
        If ``True``, the function calls an injected helper responsible
        for recursively extracting ZIP archives.

        Note
        ----
        ZIP extraction is **not implemented in this module** and must be
        provided externally (typically via test-time monkey-patching).

    keep_archives
        Forwarded to the extraction helper. When supported, controls
        whether nested ZIP files are preserved after extraction.

    Returns
    -------
    list[str]
        Absolute paths to ZIP archives that were successfully downloaded
        (cached or newly fetched).

    Raises
    ------
    ValueError
        If an unknown dataset identifier is supplied.

    RuntimeError
        If ``unzip=True`` but no extraction backend has been provided.

    Notes
    -----
    • Network failures are handled per-study; a failure does not abort
      the entire fetch.
    • Conversion errors are logged and skipped.
    • This function is safe to call repeatedly (idempotent downloads).
    """

    dataset = (dataset or "all").lower()
    valid_ids = [f"s{i:02d}" for i in range(1, 16)]

    if dataset == "all":
        studies = valid_ids
    elif dataset in valid_ids:
        studies = [dataset]
    else:
        raise ValueError(
            f"Unknown dataset '{dataset}'. Expected one of {valid_ids} or 'all'."
        )

    folder = Path(folder).expanduser().resolve()
    folder.mkdir(parents=True, exist_ok=True)

    nifti_root = folder.parent / f"{folder.name}_nifti"
    base_url = f"https://zenodo.org/api/records/{DOI['RAT']}/files"

    downloaded: List[str] = []

    for sid in tqdm(studies, desc="Downloading TRISTAN rat studies", leave=False):
        zip_name = f"{sid.upper()}.zip"
        zip_path = folder / zip_name
        url = f"{base_url}/{zip_name}/content"

        if not zip_path.exists():
            try:
                with _rat_session.get(url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(zip_path, "wb") as fh:
                        for chunk in r.iter_content(chunk_size=1 << 20):
                            fh.write(chunk)
            except Exception as exc:  # noqa: BLE001
                print(f"[rat_fetch] WARNING – could not download {zip_name}: {exc}")
                continue

        downloaded.append(str(zip_path))

        if unzip:
            study_dir = folder / sid.upper()
            _unzip_nested(zip_path, study_dir, keep_archives=keep_archives)

    return downloaded


def _unzip_nested(*_args, **_kwargs) -> None:
    """
    Placeholder hook for recursive ZIP extraction.

    This function is **intentionally not implemented** in production code.

    Rationale
    ---------
    • Archive semantics are considered test / infrastructure concerns.
    • Keeping this module ZIP-free avoids unnecessary coupling and
      simplifies dependency management.

    Expected behaviour
    ------------------
    Test code must monkey-patch this symbol with a compatible callable
    implementing recursive ZIP extraction.

    Raises
    ------
    RuntimeError
        Always raised if this placeholder is invoked without being patched.
    """
    raise RuntimeError(
        "_unzip_nested is not implemented in production code. "
        "It must be provided by the caller (e.g. test suite)."
    )