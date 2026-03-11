import logging
from pathlib import Path

import pandas as pd

from microbiome_db.sources.gmrepo.config import FILES, INTERMEDIATE_DIR, RAW_DIR

logger = logging.getLogger(__name__)


def parse_file(name: str, raw_path: Path) -> pd.DataFrame:
    """Parse a .txt.gz TSV file into a DataFrame."""
    logger.info("Parsing %s from %s", name, raw_path.name)

    for encoding in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(
                raw_path,
                sep="\t",
                compression="gzip",
                encoding=encoding,
                quotechar='"',
                low_memory=False,
            )
            break
        except UnicodeDecodeError:
            if encoding == "latin-1":
                raise
            logger.warning("UTF-8 failed for %s, trying latin-1", name)
    else:
        raise RuntimeError(f"Could not decode {raw_path}")

    # Strip quotes from column names if present
    df.columns = [c.strip('"') for c in df.columns]

    logger.info(
        "  %s: %d rows x %d cols — columns: %s",
        name,
        len(df),
        len(df.columns),
        list(df.columns),
    )
    return df


def parse_all() -> dict[str, pd.DataFrame]:
    """Parse all raw files and save as intermediate Parquet."""
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    for name, filename in FILES.items():
        raw_path = RAW_DIR / filename
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw file not found: {raw_path}. Run download first.")

        df = parse_file(name, raw_path)
        out_path = INTERMEDIATE_DIR / f"{name}.parquet"
        df.to_parquet(out_path, index=False)
        logger.info("  Saved %s → %s", name, out_path)
        results[name] = df

    print("All files parsed and saved as intermediate Parquet.")
    return results
