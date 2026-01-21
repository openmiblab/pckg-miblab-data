import zipfile
from pathlib import Path
from typing import List

import dicom2nifti
from tqdm import tqdm
import requests       
from requests.adapters import HTTPAdapter, Retry


_rat_session = requests.Session()
_rat_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,             # 1 s → 2 s → 4 s
            status_forcelist=(502, 503, 504),
        )
    ),
)


#  Public TRISTAN RAT Download Zenodo API
def rat_fetch(
    dataset: str | None = None,
    *,
    folder: str | Path = "./tristanrat",
    unzip: bool  = True,
    convert: bool = False,
    keep_archives: bool = False,
) -> List[str]:
    """
    Download, recursively extract, and (optionally) convert TRISTAN rat
    MRI studies from Zenodo (record **15747417**).

    The helper understands the 15 published studies **S01 … S15**.  
    Pass ``dataset="all"`` (or leave *dataset* empty) to fetch every
    archive in one go.

    Parameters
    ----------
    dataset
        ``"S01" … "S15"`` to grab a single study  
        ``"all"`` or *None* to fetch them all.
    folder
        Root directory that will hold the ``SXX.zip`` files and the
        extracted DICOM tree.  A sibling directory
        ``<folder>_nifti/`` is used for conversion output.
    unzip
        If *True*, each ZIP is unpacked **recursively** (handles inner
        ZIP-in-ZIP structures).
    convert
        If *True*, every DICOM folder is converted to compressed NIfTI
        (_requires the **dicom2nifti** wheel and ``unzip=True``_).
    keep_archives
        Forwarded to :func:`_unzip_nested`; set *True* to retain each
        inner ZIP after extraction (useful for auditing).

    Returns
    -------
    list[str]
        Absolute paths to every ``SXX.zip`` that was downloaded
        (whether new or cached).

    Examples
    --------
    **Download a single study and leave it zipped**

    >>> from miblab_data.tristan import rat_fetch
    >>> rat_fetch("S01", folder="~/tristanrat", unzip=False)
    ['/home/you/tristanrat/S01.zip']

    **Fetch the entire collection, unzip, but skip conversion**

    >>> rat_fetch(dataset="all",
    ...           folder="./rat_data",
    ...           unzip=True,
    ...           convert=False)

    **Full end-to-end pipeline (requires dicom2nifti)**

    >>> rat_fetch("S03",
    ...           folder="./rat_data",
    ...           unzip=True,
    ...           convert=True)

    The call returns the list of ZIP paths; side-effects are files
    extracted (and optionally NIfTI volumes) under *folder*.
    """

    # ── resolve study IDs ───────────────────────────────────────────────────
    dataset = (dataset or "all").lower()
    valid_ids = [f"s{i:02d}" for i in range(1, 16)]   # S01 … S15 only
    if dataset == "all":
        studies = valid_ids
    elif dataset in valid_ids:
        studies = [dataset]
    else:
        raise ValueError(
            f"Unknown study '{dataset}'. Choose one of "
            f"{', '.join(valid_ids)} or 'all'."
        )

    # ── local paths & URL template ──────────────────────────────────────────
    folder     = Path(folder).expanduser().resolve()
    folder.mkdir(parents=True, exist_ok=True)
    nifti_root = folder.parent / f"{folder.name}_nifti"
    base_url   = f"https://zenodo.org/api/records/{DOI['RAT']}/files"

    downloaded: List[str] = []

    # ── download loop ───────────────────────────────────────────────────────
    desc = "Downloading TRISTAN rat studies"
    it   = tqdm(studies, desc=desc, leave=False) 

    for sid in it:
        zip_name = f"{sid.upper()}.zip"
        zip_path = folder / zip_name
        url      = f"{base_url}/{zip_name}/content"

        # skip if already present
        if not zip_path.exists():
            try:
                with _rat_session.get(url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(zip_path, "wb") as fh:
                        for chunk in r.iter_content(chunk_size=1 << 20):
                            fh.write(chunk)
            except Exception as exc:                   # noqa: BLE001
                print(f"[rat_fetch] WARNING – could not download {zip_name}: {exc}")
                continue
        downloaded.append(str(zip_path))

        # ── extraction ───────────────────────────────────────
        if unzip:
            study_dir = folder / sid.upper()
            _unzip_nested(zip_path, study_dir, keep_archives=keep_archives)

            # ── optional DICOM ➜ NIfTI ──────────────────────
            if convert:
                _relax_dicom2nifti_validators()
                for dcm_dir in study_dir.rglob("*"):
                    if not dcm_dir.is_dir():
                        continue
                    if any(p.suffix.lower() == ".dcm" for p in dcm_dir.iterdir()):
                        rel_out = dcm_dir.relative_to(folder)
                        _convert_dicom_to_nifti(
                            dcm_dir,
                            nifti_root / rel_out,
                        )

    return downloaded

#  Utilities
def _unzip_nested(zip_path: str | Path, extract_to: str | Path,
                  *, keep_archives: bool = False) -> None:
    """
    Recursively extract *every* ZIP found inside *zip_path*.

    Parameters
    ----------
    zip_path
        Path to the outer **.zip** file downloaded from Zenodo.
    extract_to
        Target directory.  It is created if it does not exist.
    keep_archives
        • *False* (default) → **delete** each inner archive after it has
          been unpacked, leaving only the extracted folders/files.  
        • *True*  → preserve the nested ``.zip`` files for checksum /
          forensic work.

    Notes
    -----
    * The routine is **pure-Python** (built-in ``zipfile``); no external
      7-Zip dependency.  
    * Extraction is breadth-first: after the outer ZIP is unpacked, the
      function scans the new tree for ``*.zip`` and repeats until none
      remain.  
    * Corrupt inner archives are caught and logged to *stdout* but do
      **not** abort the entire operation.

    Examples
    --------
    >>> _unzip_nested("S03.zip", "S03_unzipped", keep_archives=True)
    """

    zip_path, extract_to = Path(zip_path), Path(extract_to)
    extract_to.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_to)

    while True:
        inners = list(extract_to.rglob("*.zip"))
        if not inners:
            break
        for inner in inners:
            dest = inner.with_suffix("")      # “…/file.zip” → “…/file/”
            dest.mkdir(exist_ok=True)
            try:
                with zipfile.ZipFile(inner) as izf:
                    izf.extractall(dest)
                if not keep_archives:
                    inner.unlink()
            except zipfile.BadZipFile as exc:         # noqa: BLE001
                print(f"[rat_fetch] WARNING – cannot unzip {inner}: {exc}")

def _convert_dicom_to_nifti(source_dir: Path, output_dir: Path) -> None:
    """
    Convert *all* DICOM series found in *source_dir* to compressed NIfTI.

    A thin, tolerant wrapper around
    :pyfunc:`dicom2nifti.convert_directory`.  Any conversion error
    (corrupt slice, unsupported orientation, etc.) is printed and the
    function returns so the calling loop can continue with the next
    subject / day.

    Parameters
    ----------
    source_dir
        Directory that contains one or more DICOM series.
    output_dir
        Destination directory.  Created if missing.
        Each converted series is written as ``series_<UID>.nii.gz``.
    
    Examples
    --------
    >>> from pathlib import Path
    >>> _convert_dicom_to_nifti(Path("S01/Rat03/Day1/dicom"), Path("S01_nifti/Rat03/Day1"))
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        dicom2nifti.convert_directory(
            str(source_dir), str(output_dir), reorient=True
        )
    except Exception as exc:                          # noqa: BLE001
        print(f"[rat_fetch] ERROR – conversion failed for {source_dir}: {exc}")

def _relax_dicom2nifti_validators() -> None:
    """
    Disable dicom2nifti's strict slice-geometry validators.

    Pre-clinical (small-animal) scanners often produce DICOMs that fail
    dicom2nifti’s default **orthogonality** / **slice-increment** checks
    even though the data reconstructs fine.  This helper tries to import
    ``dicom2nifti.settings`` and, if present, toggles every
    *disable_validate_* flag known across versions 2 → 3.

    The call is **idempotent** – safe to invoke multiple times.

    No error is raised when *dicom2nifti* is not installed; the caller
    should already have checked the `_have_dicom2nifti` feature-flag.
    """
    import dicom2nifti.settings as _dset          # type: ignore

    for fn in ("disable_validate_orthogonal",
               "disable_validate_sliceincrement",
               "disable_validate_slice_increment",
               "disable_validate_dimensions",
               "disable_validate_dimension"):
        if hasattr(_dset, fn):
            getattr(_dset, fn)()

