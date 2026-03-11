import time
from pathlib import Path

import requests
from tqdm import tqdm

# _common.py -> sources/ -> microbiome_db/ -> src/ -> project root
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent


def source_dirs(source_name: str) -> tuple[Path, Path, Path]:
    """Return (raw_dir, intermediate_dir, processed_dir) for a source."""
    base = ROOT_DIR / "data" / "sources" / source_name
    return base / "raw", base / "intermediate", base / "processed"


def download_file(url: str, dest: Path, force: bool = False, retries: int = 3) -> None:
    """Download a file with progress bar and retry logic."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not force:
        print(f"  Skipping {dest.name} (exists)")
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
