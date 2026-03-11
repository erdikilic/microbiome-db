import logging

import pandas as pd

from microbiome_db.sources.cmd.config import PROCESSED_DIR, RAW_DIR

logger = logging.getLogger(__name__)


def build_all() -> None:
    """Build Parquet abundance matrices and metadata from R-exported CSVs."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    abundance_csv = RAW_DIR / "relative_abundance.csv.gz"
    metadata_csv = RAW_DIR / "metadata.csv.gz"

    for f in (abundance_csv, metadata_csv):
        if not f.exists():
            raise FileNotFoundError(f"{f} not found. Run download (R export) first.")

    # Load abundance (samples x taxa, first column is sample ID index)
    logger.info("Loading abundance CSV...")
    abundance = pd.read_csv(abundance_csv, index_col=0, compression="gzip")
    abundance.index.name = "sample_id"
    logger.info("  Raw abundance: %s", abundance.shape)

    # Split into species and genus level based on taxonomy names
    # cMD uses MetaPhlAn format: species names contain "s__", genus names don't
    # Column names are short species names like "Escherichia coli"
    # We keep all taxa as-is — they're species-level from MetaPhlAn
    species_cols = [c for c in abundance.columns if not c.startswith("k__")]
    abundance = abundance[species_cols]

    # Convert to percentage (cMD exports as proportions 0-1)
    if abundance.max().max() <= 1.0:
        abundance = abundance * 100.0

    # Build genus-level by collapsing species to genus
    # Species names are like "Genus species" — take first word
    genus_map = {}
    for col in abundance.columns:
        genus = col.split(" ")[0] if " " in col else col
        genus_map.setdefault(genus, []).append(col)

    genus_series = {}
    for genus, cols in sorted(genus_map.items()):
        genus_series[genus] = abundance[cols].sum(axis=1)
    genus_abundance = pd.DataFrame(genus_series, index=abundance.index)
    genus_abundance.index.name = "sample_id"

    # Fill NaN
    abundance = abundance.fillna(0.0)
    genus_abundance = genus_abundance.fillna(0.0)

    # Load metadata
    logger.info("Loading metadata CSV...")
    meta = pd.read_csv(metadata_csv, index_col=0, compression="gzip")
    meta.index.name = "sample_id"

    # Filter metadata to samples in abundance
    meta = meta[meta.index.isin(abundance.index)].copy()
    meta = meta[~meta.index.duplicated(keep="first")]

    # Save
    species_path = PROCESSED_DIR / "species_abundance.parquet"
    genus_path = PROCESSED_DIR / "genus_abundance.parquet"
    meta_path = PROCESSED_DIR / "metadata.parquet"

    abundance.to_parquet(species_path)
    genus_abundance.to_parquet(genus_path)
    meta.to_parquet(meta_path)

    sp_sparsity = (abundance.values == 0).sum() / abundance.size * 100
    g_sparsity = (genus_abundance.values == 0).sum() / genus_abundance.size * 100
    print(f"  species: {abundance.shape[0]} samples x {abundance.shape[1]} taxa, {sp_sparsity:.1f}% sparse")
    print(f"  genus: {genus_abundance.shape[0]} samples x {genus_abundance.shape[1]} genera, {g_sparsity:.1f}% sparse")
    print(f"  metadata: {meta.shape[0]} samples x {meta.shape[1]} columns")
    print(f"  studies: {meta['study_name'].nunique() if 'study_name' in meta.columns else 'N/A'}")
