
import os
import zipfile

from tqdm import tqdm
from osfclient.api import OSF


def fetch(dataset: str, folder: str, project: str = "un5ct", token: str = None, extract: bool = True, verbose: bool = True):
    """
    Download a dataset from OSF (Open Science Framework).

    This function downloads a specific dataset (folder or subfolder) from a public or private OSF project.
    Files are saved into the specified local directory. If a zip file is found, it will be extracted by default.

    Args:
        dataset (str): Subfolder path inside the OSF project. If an empty string, all files in the root will be downloaded (use with caution).
        folder (str): Local folder where the dataset will be saved.
        project (str, optional): OSF project ID (default is "un5ct").
        token (str, optional): Personal OSF token for accessing private projects. Read from OSF_TOKEN environment variable if needed.
        extract (bool, optional): Whether to automatically unzip downloaded .zip files (default is True).
        verbose (bool, optional): Whether to print progress messages (default is True).

    Raises:
        FileNotFoundError: If the specified dataset path does not exist in the OSF project.
        NotImplementedError: If required packages are not installed.

    Returns:
        str: Path to the local folder containing the downloaded data.

    Example:
        >>> from miblab.osf import fetch
        >>> fetch('TRISTAN/RAT/bosentan_highdose/Sanofi', 'test_download')
    """

    # Prepare local folder
    os.makedirs(folder, exist_ok=True)

    # Connect to OSF and locate project storage
    osf = OSF(token=token)  #osf = OSF()  for public projects
    project = osf.project(project)
    storage = project.storage('osfstorage')

    # Navigate the dataset folder if provided
    current = storage
    if dataset:
        parts = dataset.strip('/').split('/')
        for part in parts:
            for f in current.folders:
                if f.name == part:
                    current = f
                    break
            else:
                raise FileNotFoundError(f"Folder '{part}' not found when navigating path '{dataset}'.")

    # Recursive download of all files and folders
    def download(current_folder, local_folder):
        os.makedirs(local_folder, exist_ok=True)
        files = list(current_folder.files)
        iterator = tqdm(files, desc=f"Downloading to {local_folder}") if verbose and files else files
        for file in iterator:
            local_file = os.path.join(local_folder, file.name)
            try:
                with open(local_file, 'wb') as f:
                    file.write_to(f)
            except Exception as e:
                if verbose:
                    print(f"Warning downloading {file.name}: {e}")

        for subfolder in current_folder.folders:
            download(subfolder, os.path.join(local_folder, subfolder.name))

    download(current, folder)

    # Extract all downloaded zip files if needed
    if extract:
        for dirpath, _, filenames in os.walk(folder):
            for filename in filenames:
                if filename.lower().endswith('.zip'):
                    zip_path = os.path.join(dirpath, filename)
                    extract_to = os.path.join(dirpath, filename[:-4])
                    os.makedirs(extract_to, exist_ok=True)
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            bad_file = zip_ref.testzip()
                            if bad_file:
                                raise zipfile.BadZipFile(f"Corrupt file {bad_file} inside {zip_path}")
                            zip_ref.extractall(extract_to)
                        os.remove(zip_path)
                        if verbose:
                            print(f"Unzipped and deleted {zip_path}")
                    except Exception as e:
                        if verbose:
                            print(f"Warning unzipping {zip_path}: {e}")
    return folder


def upload(folder: str, dataset: str, project: str = "un5ct", token: str = None, verbose: bool = True, overwrite: bool = True):
    """
    Upload a file to OSF (Open Science Framework) using osfclient.

    This function uploads a single local file to a specified path inside an OSF project.
    Intermediate folders must already exist in the OSF project; osfclient does not create them.
    If the file already exists, it can be overwritten or skipped.

    Args:
        folder (str): Path to the local file to upload.
        dataset (str): OSF path where the file should be placed (e.g., "Testing/filename.txt").
        project (str): OSF project ID (default: "un5ct").
        token (str): OSF personal token for private/write access.
        verbose (bool): Whether to print progress messages (default True).
        overwrite (bool): Whether to replace an existing file if it already exists (default True).

    Raises:
        FileNotFoundError: If the file does not exist.
        NotImplementedError: If osfclient is not installed.
        RuntimeError: If upload fails for any reason.

    Example:
        >>> from miblab.osf import upload
        >>> upload(
        ...     folder='data/results.csv',
        ...     dataset='Testing/results.csv',
        ...     project='un5ct',
        ...     token='your-osf-token',
        ...     verbose=True,
        ...     overwrite=True
        ... )
    """


    # Check that the specified local file exists
    if not os.path.isfile(folder):
        raise FileNotFoundError(f"Local file not found: {folder}")

    # Authenticate and connect to the OSF project
    osf = OSF(token=token)
    project = osf.project(project)
    storage = project.storage("osfstorage")

    # Clean and prepare the remote dataset path
    full_path = dataset.strip("/")

    # Check if the file already exists on OSF
    existing = next((f for f in storage.files if f.path == "/" + full_path), None)
    if existing:
        if overwrite:
            if verbose:
                print(f"File '{full_path}' already exists. Deleting before re-upload...")
            try:
                existing.remove()
            except Exception as e:
                raise RuntimeError(f"Failed to delete existing file before overwrite: {e}")
        else:
            if verbose:
                print(f"File '{full_path}' already exists. Skipping (overwrite=False).")
            return

    # Upload the file
    size_mb = os.path.getsize(folder) / 1e6
    with open(folder, "rb") as f:
        if verbose:
            print(f"Uploading '{os.path.basename(folder)}' ({size_mb:.2f} MB) to '{full_path}'...")
        try:
            storage.create_file(full_path, f)
            if verbose:
                print("Upload complete.")
        except Exception as e:
            raise RuntimeError(f"Failed to upload file: {e}")

