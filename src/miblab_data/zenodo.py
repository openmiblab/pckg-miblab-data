
import os
import zipfile
import requests
from pathlib import Path


def download(
    record_id,
    filename,
    save_dir,
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
    save_dir : str or Path
        Directory where the file (and extracted contents) will be stored
    extract : bool, default=False
        If True and the file is a .zip archive, it will be extracted

    Returns
    -------
    Path
        Path to the downloaded file or extracted folder
    """
    filepath = os.path.join(save_dir, filename)
    if os.path.exists(filepath):
        print(f"Skipping download, file {filepath} already exists.")
        return filepath

    # Dataset download link
    file_url = f"https://zenodo.org/records/{record_id}/files/{filename}"

    # Make the request and check for connection error
    try:
        file_response = requests.get(file_url)
    except requests.exceptions.ConnectionError as err:
        raise requests.exceptions.ConnectionError(
            f"\n\n"
            f"A connection error occurred trying to download {filename} "
            f"from Zenodo. This usually happens if you are offline. "
            f"The detailed error message is here: {err}"
        ) 
    
    # Check for other errors
    file_response.raise_for_status()

    # Save the file
    os.makedirs(save_dir, exist_ok=True)
    with open(filepath, 'wb') as f:
        f.write(file_response.content)

    # If the zip file is requested we are done
    if not extract:
        return filepath
    
    # If extraction requested, returned extracted
    if filepath[-4:] == '.zip':
        extract_to = filepath[:-4]
    else:
        extract_to = filepath + '_unzip'

    # Skip extraction if the folder already exists
    if os.path.exists(extract_to):
        return extract_to

    # Perform extraction
    os.makedirs(extract_to)
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        bad_file = zip_ref.testzip()
        if bad_file:
            raise zipfile.BadZipFile(
                f"Cannot extract: corrupt file {bad_file}."
            )
        zip_ref.extractall(extract_to)

    return extract_to





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

    
