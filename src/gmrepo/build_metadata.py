import logging

import pandas as pd

from gmrepo.config import INTERMEDIATE_DIR, PROCESSED_DIR

logger = logging.getLogger(__name__)

# Columns to keep from sample_to_run_info and how to rename them
SAMPLE_COLUMNS = {
    "run_id": "run_id",
    "sample_id": "sample_id",
    "project_id": "project_id",
    "experiment_type": "experiment_type",
    "nr_reads_sequenced": "num_reads",
    "disease": "disease_mesh_id",
    "phenotype": "phenotype",
    "country": "country",
    "sex": "sex",
    "host_age": "age",
    "BMI": "bmi",
    "diet": "diet",
    "Recent_Antibiotics_Use": "recent_antibiotics",
}


def build_metadata() -> None:
    """Build curated metadata Parquet from sample, project, and MeSH data."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Loading intermediate data...")
    samples = pd.read_parquet(INTERMEDIATE_DIR / "samples.parquet")
    mesh = pd.read_parquet(INTERMEDIATE_DIR / "mesh.parquet")

    # Also load abundance to get the set of valid accession_ids
    abundance = pd.read_parquet(INTERMEDIATE_DIR / "abundance.parquet")
    valid_accessions = set(abundance["accession_id"].unique())
    logger.info("  %d unique accession IDs in abundance data", len(valid_accessions))

    # Use run_id as the sample identifier (matches accession_id in abundance)
    # The field is called "run_id" in samples
    # Select and rename columns
    available = [c for c in SAMPLE_COLUMNS if c in samples.columns]
    meta = samples[available].rename(columns=SAMPLE_COLUMNS).copy()

    # The key linking column: run_id matches accession_id in abundance
    # Filter to only samples present in abundance data
    meta = meta[meta["run_id"].isin(valid_accessions)].copy()
    logger.info("  %d metadata rows matching abundance accessions", len(meta))

    # Set run_id as index (matching abundance sample_id index)
    meta = meta.set_index("run_id")
    meta.index.name = "sample_id"

    # Drop duplicates (some run_ids may appear multiple times)
    meta = meta[~meta.index.duplicated(keep="first")]

    # Build MeSH disease name lookup
    if "uid" in mesh.columns and "term" in mesh.columns:
        mesh_lookup = mesh.set_index("uid")["term"].to_dict()
        meta["disease_name"] = meta["disease_mesh_id"].map(mesh_lookup)
    else:
        logger.warning("MeSH data missing uid/term columns, skipping disease name lookup")

    # Clean categorical columns
    for col in ("phenotype", "country", "sex", "experiment_type", "diet", "recent_antibiotics"):
        if col in meta.columns:
            meta[col] = meta[col].astype(str).str.strip()
            meta[col] = meta[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    # Clean numeric columns
    for col in ("num_reads", "age", "bmi"):
        if col in meta.columns:
            meta[col] = pd.to_numeric(meta[col], errors="coerce")

    # Save
    out_path = PROCESSED_DIR / f"metadata.parquet"
    meta.to_parquet(out_path)
    logger.info("  Saved metadata: %s", meta.shape)
    print(f"  metadata: {meta.shape[0]} samples x {meta.shape[1]} columns")
    print(f"  columns: {list(meta.columns)}")


def build_taxonomy() -> None:
    """Build taxonomy mapping Parquet."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Loading taxonomy...")
    tax = pd.read_parquet(INTERMEDIATE_DIR / "taxonomy.parquet")

    # Select relevant columns
    cols = {}
    if "ncbi_taxon_id" in tax.columns:
        cols["ncbi_taxon_id"] = "ncbi_id"
    if "scientific_name" in tax.columns:
        cols["scientific_name"] = "name"
    if "node_rank" in tax.columns:
        cols["node_rank"] = "rank"
    if "superkingdom" in tax.columns:
        cols["superkingdom"] = "superkingdom"

    taxonomy = tax[list(cols.keys())].rename(columns=cols).copy()
    taxonomy["ncbi_id"] = taxonomy["ncbi_id"].astype(str).str.strip()
    taxonomy = taxonomy.drop_duplicates(subset=["ncbi_id"], keep="first")
    taxonomy = taxonomy.set_index("ncbi_id")

    out_path = PROCESSED_DIR / "taxonomy.parquet"
    taxonomy.to_parquet(out_path)
    logger.info("  Saved taxonomy: %s", taxonomy.shape)
    print(f"  taxonomy: {taxonomy.shape[0]} taxa")
