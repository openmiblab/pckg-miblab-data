
import os
import shutil


def clear_cache_datafiles(directory: str, verbose: bool = True):
    """
    Delete all files and subdirectories in the specified cache directory,
    except for '__init__' files.

    Args:
        directory (str): Path to the directory to clear.
        verbose (bool): If True, prints names of deleted items.

    Raises:
        FileNotFoundError: If the directory does not exist.
        OSError: If a file or folder cannot be deleted.
    """
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Directory not found: {directory}")

    deleted = []
    for item in os.listdir(directory):
        path = os.path.join(directory, item)

        # Skip __init__ files (e.g., __init__.py, __init__.pyc)
        if os.path.isfile(path) and os.path.splitext(item)[0] == '__init__':
            continue

        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)
                deleted.append(path)
                if verbose:
                    print(f"Deleted file: {path}")
            elif os.path.isdir(path):
                shutil.rmtree(path)
                deleted.append(path)
                if verbose:
                    print(f"Deleted folder: {path}")
        except Exception as e:
            print(f"Error deleting {path}: {e}")

    if verbose and not deleted:
        print("Directory is already clean.")