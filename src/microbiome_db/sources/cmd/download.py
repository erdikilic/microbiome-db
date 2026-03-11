import logging
import shutil
import subprocess

from microbiome_db.sources.cmd.config import EXPORT_SCRIPT, RAW_DIR, RSCRIPT

logger = logging.getLogger(__name__)


def download_all(force: bool = False) -> None:
    """Run the R export script to extract data from curatedMetagenomicData."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    abundance_csv = RAW_DIR / "relative_abundance.csv.gz"
    metadata_csv = RAW_DIR / "metadata.csv.gz"

    if abundance_csv.exists() and metadata_csv.exists() and not force:
        print("  Skipping R export (files exist, use --force to re-export)")
        return

    # Check Rscript is available
    rscript = shutil.which(RSCRIPT) or shutil.which("Rscript")
    if not rscript:
        raise RuntimeError(
            f"Rscript not found at {RSCRIPT} or on PATH. "
            "Install R and curatedMetagenomicData, or set RSCRIPT path in config.py"
        )

    # Check the package is installed
    result = subprocess.run(
        [rscript, "-e", "library(curatedMetagenomicData)"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "curatedMetagenomicData R package not installed. Install with:\n"
            f"  {rscript} -e 'BiocManager::install(\"curatedMetagenomicData\")'"
        )

    # Run export script
    print(f"Running R export script (this may take several minutes)...")
    logger.info("Rscript: %s", rscript)
    logger.info("Export script: %s", EXPORT_SCRIPT)

    result = subprocess.run(
        [rscript, str(EXPORT_SCRIPT), str(RAW_DIR)],
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"R export script failed with exit code {result.returncode}")

    # Verify outputs exist
    for f in (abundance_csv, metadata_csv):
        if not f.exists():
            raise RuntimeError(f"Expected output not found: {f}")

    print("R export complete.")
