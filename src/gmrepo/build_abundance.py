import logging

import pandas as pd

from gmrepo.config import CHUNK_SIZE, INTERMEDIATE_DIR, PROCESSED_DIR

logger = logging.getLogger(__name__)


def build_abundance() -> None:
    """Build wide sparse abundance matrices for species and genus."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Loading abundance data...")
    abundance = pd.read_parquet(INTERMEDIATE_DIR / "abundance.parquet")

    # Keep only needed columns, drop internal IDs
    abundance = abundance[["accession_id", "ncbi_taxon_id", "taxon_rank_level", "relative_abundance"]].copy()

    # Clean types
    abundance["ncbi_taxon_id"] = abundance["ncbi_taxon_id"].astype(str).str.strip()
    abundance["relative_abundance"] = pd.to_numeric(abundance["relative_abundance"], errors="coerce")
    abundance["taxon_rank_level"] = abundance["taxon_rank_level"].str.strip().str.lower()

    # Drop rows with invalid abundance
    before = len(abundance)
    abundance = abundance.dropna(subset=["relative_abundance"])
    if len(abundance) < before:
        logger.warning("Dropped %d rows with invalid abundance values", before - len(abundance))

    for rank in ("species", "genus"):
        _build_rank(abundance, rank)


def _build_rank(abundance: pd.DataFrame, rank: str) -> None:
    """Pivot abundance data for a single taxonomic rank into a wide matrix."""
    logger.info("Building %s abundance matrix...", rank)

    rank_data = abundance[abundance["taxon_rank_level"] == rank].copy()
    if rank_data.empty:
        logger.warning("No %s data found!", rank)
        return

    # Drop duplicates: keep first if same sample + taxon appears twice
    rank_data = rank_data.drop_duplicates(subset=["accession_id", "ncbi_taxon_id"], keep="first")

    sample_ids = rank_data["accession_id"].unique()
    n_samples = len(sample_ids)
    n_taxa = rank_data["ncbi_taxon_id"].nunique()
    logger.info("  %d samples x %d %s taxa", n_samples, n_taxa, rank)

    # Chunked pivot to manage memory
    chunks = []
    for i in range(0, n_samples, CHUNK_SIZE):
        chunk_samples = sample_ids[i : i + CHUNK_SIZE]
        chunk_data = rank_data[rank_data["accession_id"].isin(chunk_samples)]

        pivoted = chunk_data.pivot_table(
            index="accession_id",
            columns="ncbi_taxon_id",
            values="relative_abundance",
            aggfunc="first",
        )
        chunks.append(pivoted)
        logger.info("  Pivoted chunk %d/%d (%d samples)", i // CHUNK_SIZE + 1, (n_samples + CHUNK_SIZE - 1) // CHUNK_SIZE, len(pivoted))

    # Concatenate all chunks
    result = pd.concat(chunks, axis=0)

    # Fill NaN with 0.0, sort columns
    result = result.fillna(0.0)
    result = result.sort_index(axis=1)
    result.index.name = "sample_id"

    # Validate
    _validate_abundance(result, rank)

    # Save
    out_path = PROCESSED_DIR / f"{rank}_abundance.parquet"
    result.to_parquet(out_path)
    sparsity = (result == 0).values.sum() / result.size * 100
    logger.info(
        "  Saved %s: %s, sparsity=%.1f%%, mem=%.1fMB",
        out_path.name,
        result.shape,
        sparsity,
        result.memory_usage(deep=True).sum() / 1e6,
    )
    print(f"  {rank}: {result.shape[0]} samples x {result.shape[1]} taxa, {sparsity:.1f}% sparse")


def _validate_abundance(df: pd.DataFrame, rank: str) -> None:
    """Validate abundance matrix values."""
    vals = df.values
    if vals.min() < 0:
        raise ValueError(f"{rank} abundance has negative values: min={vals.min()}")
    if vals.max() > 100:
        raise ValueError(f"{rank} abundance has values > 100: max={vals.max()}")
    if pd.isna(vals).any():
        raise ValueError(f"{rank} abundance still has NaN values after fill")
    if df.index.duplicated().any():
        raise ValueError(f"{rank} abundance has duplicate sample IDs")
