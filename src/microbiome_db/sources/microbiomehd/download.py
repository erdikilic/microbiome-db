import logging
import tarfile

import requests

from microbiome_db.sources._common import download_file
from microbiome_db.sources.microbiomehd.config import INTERMEDIATE_DIR, RAW_DIR, ZENODO_API

logger = logging.getLogger(__name__)


def download_all(force: bool = False) -> None:
    """Download all 28 study archives from Zenodo and extract them."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching file list from Zenodo...")
    resp = requests.get(ZENODO_API, timeout=30)
    resp.raise_for_status()
    files = resp.json()["files"]

    logger.info("Found %d study archives", len(files))

    for entry in files:
        filename = entry["key"]
        url = entry["links"]["self"]
        dest = RAW_DIR / filename

        print(f"Downloading {filename}...")
        download_file(url, dest, force=force)

        # Extract to intermediate dir
        study_name = filename.replace("_results.tar.gz", "")
        extract_dir = INTERMEDIATE_DIR / study_name
        if extract_dir.exists() and not force:
            logger.info("  Already extracted: %s", study_name)
            continue

        logger.info("  Extracting %s...", filename)
        with tarfile.open(dest, "r:gz") as tar:
            tar.extractall(INTERMEDIATE_DIR, filter="data")

    print(f"All {len(files)} study archives downloaded and extracted.")
