
import os
import zipfile
import requests
from pathlib import Path
 

import os
import zipfile
import requests
from pathlib import Path


def _get_zenodo_files(record_id, token=None):
    url = f"https://zenodo.org/api/records/{record_id}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["files"]


def _download_file(url, out_path, token=None):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/octet-stream",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, headers=headers, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def download(
    record_id,
    filename,
    out_dir,
    *,
    token=None,
    extract=False,
):
    """
    Download a file from Zenodo and optionally extract it.

    Parameters
    ----------
    record_id : str or int
        Zenodo record ID (e.g. 15489381)
    filename : str
        Name of the file in the record (e.g. "data.zip")
    out_dir : str or Path
        Directory where the file (and extracted contents) will be stored
    token : str, optional
        Zenodo API token for restricted records
    extract : bool, default=False
        If True and the file is a .zip archive, it will be extracted

    Returns
    -------
    Path
        Path to the downloaded file or extracted folder
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = _get_zenodo_files(record_id, token)

    for f in files:
        if f["key"] == filename:
            url = f["links"]["content"]
            file_path = out_dir / filename

            # Cache: don't re-download if already exists
            if not file_path.exists():
                _download_file(url, file_path, token)

            # Optional extraction
            if extract and filename.lower().endswith(".zip"):
                extract_dir = out_dir / file_path.stem
                if not extract_dir.exists():
                    with zipfile.ZipFile(file_path, "r") as z:
                        z.extractall(extract_dir)
                return extract_dir

            return file_path

    raise ValueError(f"File '{filename}' not found in Zenodo record {record_id}")






# Zenodo DOI of the repository
DOI = {
    'MRR': "15285017",    
    'TRISTAN': "15301607",
    'RAT': "15747417",
}

# miblab datasets
DATASETS = {
    'KRUK.dmr.zip': {'doi': DOI['MRR']},
    'tristan_humans_healthy_controls.dmr.zip': {'doi': DOI['TRISTAN']},
    'tristan_humans_healthy_ciclosporin.dmr.zip': {'doi': DOI['TRISTAN']},
    'tristan_humans_healthy_metformin.dmr.zip': {'doi': DOI['TRISTAN']},
    'tristan_humans_healthy_rifampicin.dmr.zip': {'doi': DOI['TRISTAN']},
    'tristan_humans_patients_rifampicin.dmr.zip': {'doi': DOI['TRISTAN']},
    'tristan_rats_healthy_multiple_dosing.dmr.zip': {'doi': DOI['TRISTAN']},
    'tristan_rats_healthy_reproducibility.dmr.zip': {'doi': DOI['TRISTAN']},
    'tristan_rats_healthy_six_drugs.dmr.zip': {'doi': DOI['TRISTAN']},
}


def fetch(dataset: str, folder: str, doi: str = None, filename: str = None,
                 extract: bool = False, verbose: bool = False):
    """Download a dataset from Zenodo.

    Note if a dataset already exists locally it will not be downloaded 
    again and the existing file will be returned. 

    Args:
        dataset (str): Name of the dataset
        folder (str): Local folder where the result is to be saved
        doi (str, optional): Digital object identifier (DOI) of the 
          Zenodo repository where the dataset is uploaded. If this 
          is not provided, the function will look for the dataset in
          miblab's own Zenodo repositories.
        filename (str, optional): Filename of the downloaded dataset. 
          If this is not provided, then *dataset* is used as filename.
        extract (bool): Whether to automatically extract downloaded ZIP files. 
        verbose (bool): If True, prints logging messages.

    Raises:
        NotImplementedError: If miblab is not installed with the data
          option.
        requests.exceptions.ConnectionError: If the connection to 
          Zenodo cannot be made.

    Returns:
        str: Full path to the downloaded datafile.
    """
 
    # Create filename 
    if filename is None:
        file = os.path.join(folder, dataset)
    else:
        file = os.path.join(folder, filename)

    # If it is not already downloaded, download it.
    if os.path.exists(file):
        if verbose:
            print(f"Skipping {dataset} download, file {file} already exists.")
    else:
        # Get DOI
        if doi is None:
            if dataset in DATASETS:
                doi = DATASETS[dataset]['doi']
            else:
                raise ValueError(
                    f"{dataset} does not exist in one of the miblab "
                    f"repositories on Zenodo. If you want to fetch " 
                    f"a dataset in an external Zenodo repository, please "
                    f"provide the doi of the repository."
                )
        
        # Dataset download link
        file_url = f"https://zenodo.org/records/{doi}/files/{filename or dataset}"

        # Make the request and check for connection error
        try:
            file_response = requests.get(file_url)
        except requests.exceptions.ConnectionError as err:
            raise requests.exceptions.ConnectionError(
                f"\n\n"
                f"A connection error occurred trying to download {dataset} "
                f"from Zenodo. This usually happens if you are offline. "
                f"The detailed error message is here: {err}"
            ) 
        
        # Check for other errors
        file_response.raise_for_status()

        # Create the folder if needed
        if not os.path.exists(folder):
            os.makedirs(folder)

        # Save the file
        with open(file, 'wb') as f:
            f.write(file_response.content)

    # If the zip file is requested we are done
    if not extract:
        return file
    
    # If extraction requested, returned extracted
    if file[-4:] == '.zip':
        extract_to = file[:-4]
    else:
        extract_to = file + '_unzip'

    # Skip extraction if the folder already exists
    if os.path.exists(extract_to):
        if verbose:
            print(f"Skipping {file} extraction, folder {extract_to} already exists.")
        return extract_to

    # Perform extraction
    os.makedirs(extract_to)
    with zipfile.ZipFile(file, 'r') as zip_ref:
        bad_file = zip_ref.testzip()
        if bad_file:
            raise zipfile.BadZipFile(
                f"Cannot extract: corrupt file {bad_file}."
            )
        zip_ref.extractall(extract_to)

    return extract_to

    
