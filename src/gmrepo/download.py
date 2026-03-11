import gzip
import time

import requests
from tqdm import tqdm

from gmrepo.config import FILES, GMREPO_BASE, RAW_DIR


def download_file(url: str, dest, force: bool = False, retries: int = 3) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not force:
        print(f"  Skipping {dest.name} (exists, use --force to re-download)")
        return

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))

            with open(dest, "wb") as f, tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                desc=dest.name,
                disable=total == 0,
            ) as bar:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))

            _verify_gzip(dest)
            return

        except (requests.RequestException, OSError) as e:
            if attempt < retries:
                wait = 2**attempt
                print(f"  Retry {attempt}/{retries} for {dest.name} in {wait}s: {e}")
                time.sleep(wait)
            else:
                dest.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Failed to download {dest.name} after {retries} attempts: {e}"
                ) from e


def _verify_gzip(path) -> None:
    try:
        with gzip.open(path, "rb") as f:
            f.read(1)
    except gzip.BadGzipFile as e:
        path.unlink(missing_ok=True)
        raise RuntimeError(f"{path.name} is not a valid gzip file") from e


def download_all(force: bool = False) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for name, filename in FILES.items():
        url = f"{GMREPO_BASE}/{filename}"
        dest = RAW_DIR / filename
        print(f"Downloading {name} ({filename})...")
        download_file(url, dest, force=force)
    print("All downloads complete.")
