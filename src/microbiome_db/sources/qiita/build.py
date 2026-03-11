import logging
from pathlib import Path

import biom
import pandas as pd

from microbiome_db.sources.qiita.config import PROCESSED_DIR, RAW_DIR

logger = logging.getLogger(__name__)


def _load_and_merge_biom(biom_dir: Path) -> biom.Table:
    """Load and merge all BIOM batch files."""
    batch_files = sorted(biom_dir.glob("batch_*.biom"))
    if not batch_files:
        raise FileNotFoundError(f"No BIOM batch files in {biom_dir}")

    logger.info("Loading %d BIOM batch files...", len(batch_files))
    tables = []
    for bf in batch_files:
        t = biom.load_table(str(bf))
        tables.append(t)
        logger.info("  %s: %d samples x %d features", bf.name, t.shape[1], t.shape[0])

    merged = tables[0]
    for t in tables[1:]:
        merged = merged.merge(t)
    logger.info("Merged: %d samples x %d features", merged.shape[1], merged.shape[0])
    return merged


def _extract_genus_map(table: biom.Table) -> dict[str, str]:
    """Extract OTU ID -> genus name mapping from observation metadata.

    Handles both split-column format (taxonomy_0..taxonomy_6) and
    single-column format (taxonomy as list or semicolon-separated string).
    """
    obs_md = table.metadata_to_dataframe("observation")
    if obs_md is None or obs_md.empty:
        return {}

    genus_map = {}

    # Check for split taxonomy columns (taxonomy_0 .. taxonomy_5 = genus)
    if "taxonomy_5" in obs_md.columns:
        for otu_id, row in obs_md.iterrows():
            genus_val = row["taxonomy_5"]
            if isinstance(genus_val, str) and genus_val.startswith("g__"):
                genus = genus_val[3:].strip()
                if genus:
                    genus_map[str(otu_id)] = genus
        return genus_map

    # Fallback: single "taxonomy" column
    if "taxonomy" not in obs_md.columns:
        return {}

    for otu_id, row in obs_md.iterrows():
        tax = row["taxonomy"]
        if isinstance(tax, (list, tuple)):
            for level in tax:
                if isinstance(level, str) and level.startswith("g__"):
                    genus = level[3:].strip()
                    if genus:
                        genus_map[str(otu_id)] = genus
                    break
        elif isinstance(tax, str):
            for level in tax.split(";"):
                level = level.strip()
                if level.startswith("g__"):
                    genus = level[3:].strip()
                    if genus:
                        genus_map[str(otu_id)] = genus
                    break

    return genus_map


def _strip_artifact_suffix(sample_id: str) -> str:
    """Strip the artifact ID suffix that redbiom appends.

    redbiom returns IDs like '10057.193.1.47285' where '.47285' is the
    artifact ID. The metadata uses '10057.193.1'.
    """
    parts = sample_id.rsplit(".", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    return sample_id


def build_all() -> None:
    """Build genus abundance Parquet from BIOM batch files."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    biom_dir = RAW_DIR / "biom_batches"
    metadata_file = RAW_DIR / "metadata.tsv"

    # Load and merge BIOM tables
    table = _load_and_merge_biom(biom_dir)

    # Convert to pandas (samples x features)
    logger.info("Converting to DataFrame...")
    df = pd.DataFrame(table.to_dataframe(dense=True).T)
    df.index.name = "sample_id"
    logger.info("Raw matrix: %s", df.shape)

    # Strip artifact suffixes from sample IDs
    df.index = [_strip_artifact_suffix(s) for s in df.index]
    # Drop duplicates that may arise from multiple artifacts
    df = df[~df.index.duplicated(keep="first")]
    df.index.name = "sample_id"

    # Map OTUs to genus names
    genus_map = _extract_genus_map(table)
    logger.info("Mapped %d/%d OTUs to genus names", len(genus_map), len(df.columns))

    if genus_map:
        mapped_cols = [c for c in df.columns if str(c) in genus_map]
        df_mapped = df[mapped_cols].copy()
        df_mapped.columns = [genus_map[str(c)] for c in mapped_cols]

        # Sum duplicate genus columns
        genus_abundance = df_mapped.T.groupby(level=0).sum().T
    else:
        logger.warning("No genus mappings found, keeping OTU IDs")
        genus_abundance = df

    genus_abundance.index.name = "sample_id"

    # Convert to relative abundance (%) if counts
    row_sums = genus_abundance.sum(axis=1)
    if row_sums.max() > 100:
        logger.info("Converting counts to relative abundance (%%)")
        genus_abundance = genus_abundance.div(row_sums, axis=0) * 100.0

    genus_abundance = genus_abundance.fillna(0.0)

    # Load and process metadata
    if metadata_file.exists():
        logger.info("Loading metadata...")
        meta = pd.read_csv(metadata_file, sep="\t", index_col=0, low_memory=False)
        meta.index.name = "sample_id"
        # Convert mixed-type columns to string to avoid Parquet errors
        for col in meta.columns:
            if meta[col].dtype == object:
                meta[col] = meta[col].astype(str)
        meta = meta[meta.index.isin(genus_abundance.index)].copy()
        meta = meta[~meta.index.duplicated(keep="first")]
    else:
        logger.warning("No metadata file found, creating minimal metadata")
        meta = pd.DataFrame(index=genus_abundance.index)
        meta.index.name = "sample_id"

    # Save
    genus_path = PROCESSED_DIR / "genus_abundance.parquet"
    meta_path = PROCESSED_DIR / "metadata.parquet"

    genus_abundance.to_parquet(genus_path)
    meta.to_parquet(meta_path)

    sparsity = (genus_abundance.values == 0).sum() / genus_abundance.size * 100
    print(f"  genus: {genus_abundance.shape[0]} samples x {genus_abundance.shape[1]} genera, {sparsity:.1f}% sparse")
    print(f"  metadata: {meta.shape[0]} samples x {meta.shape[1]} columns")
