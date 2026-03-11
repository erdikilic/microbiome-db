import logging
from pathlib import Path

import pandas as pd

from microbiome_db.sources.microbiomehd.config import INTERMEDIATE_DIR, PROCESSED_DIR

logger = logging.getLogger(__name__)


def _parse_rdp_taxonomy(rdp_path: Path) -> dict[str, str]:
    """Parse RDP taxonomy file to map OTU IDs to genus names.

    RDP format: OTU_ID\t\tRoot\trootrank\t1.0\tBacteria\tdomain\t...genus\tconfidence
    """
    otu_to_genus = {}
    with open(rdp_path) as f:
        for line in f:
            parts = line.strip().split("\t")
            otu_id = parts[0]
            # Walk through taxonomy fields to find genus
            genus = "unclassified"
            for i in range(len(parts) - 1):
                if parts[i] == "genus" and i > 0:
                    genus = parts[i - 1]
                    break
            otu_to_genus[otu_id] = genus
    return otu_to_genus


def _parse_study(study_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Parse a single study directory into genus abundance + metadata.

    Returns (genus_abundance_df, metadata_df) or None if files missing.
    """
    study_name = study_dir.name.replace("_results", "")

    # Find files — naming pattern: {study_name}.{ext}
    metadata_files = list(study_dir.glob("*.metadata.txt"))
    otu_files = list(study_dir.glob("*.otu_table.100.denovo"))
    rdp_files = list(study_dir.glob("RDP/RDP_classifications.denovo.txt"))

    if not metadata_files or not otu_files or not rdp_files:
        logger.warning("Skipping %s — missing files (meta=%d, otu=%d, rdp=%d)",
                        study_name, len(metadata_files), len(otu_files), len(rdp_files))
        return None

    disease_abbrev = study_name.split("_")[0].upper()

    # Parse OTU table (OTUs as rows, samples as columns)
    otu_table = pd.read_csv(otu_files[0], sep="\t", index_col=0)

    # Parse RDP taxonomy
    otu_to_genus = _parse_rdp_taxonomy(rdp_files[0])

    # Map OTU IDs to genus names
    otu_table.index = otu_table.index.map(lambda x: otu_to_genus.get(x, "unclassified"))

    # Collapse by genus (sum counts for OTUs mapping to same genus)
    genus_table = otu_table.groupby(otu_table.index).sum()

    # Transpose to samples x genera
    genus_table = genus_table.T
    genus_table.index.name = "sample_id"

    # Convert to relative abundance (%)
    row_sums = genus_table.sum(axis=1)
    row_sums = row_sums.replace(0, 1)  # avoid division by zero
    genus_table = genus_table.div(row_sums, axis=0) * 100.0

    # Prefix sample IDs with study name to avoid cross-study collisions
    genus_table.index = study_name + ":" + genus_table.index.astype(str)

    # Parse metadata — first column is always sample ID but header varies
    try:
        raw_meta = pd.read_csv(metadata_files[0], sep="\t", index_col=0)
    except UnicodeDecodeError:
        raw_meta = pd.read_csv(metadata_files[0], sep="\t", index_col=0, encoding="latin-1")

    # Build metadata from OTU table sample IDs (source of truth)
    # Map original sample IDs to metadata disease states
    disease_state_map = {}
    if "DiseaseState" in raw_meta.columns:
        disease_state_map = raw_meta["DiseaseState"].to_dict()

    meta = pd.DataFrame(index=genus_table.index)
    meta.index.name = "sample_id"
    # Map disease state using original (unprefixed) sample IDs
    original_ids = [s.split(":", 1)[1] for s in genus_table.index]
    meta["disease_state"] = [disease_state_map.get(s, pd.NA) for s in original_ids]
    meta["study_id"] = study_name
    meta["disease"] = disease_abbrev

    logger.info("  %s: %d samples, %d genera, disease=%s",
                study_name, len(genus_table), len(genus_table.columns), disease_abbrev)

    return genus_table, meta


def build_all() -> None:
    """Build unified genus abundance matrix and metadata from all studies."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Find all extracted study directories
    study_dirs = sorted([
        d for d in INTERMEDIATE_DIR.iterdir()
        if d.is_dir() and d.name.endswith("_results")
    ])

    if not study_dirs:
        raise FileNotFoundError(
            f"No extracted study directories found in {INTERMEDIATE_DIR}. Run download first."
        )

    logger.info("Found %d study directories", len(study_dirs))

    all_abundance = []
    all_metadata = []

    for study_dir in study_dirs:
        result = _parse_study(study_dir)
        if result is None:
            continue
        genus_table, meta = result
        all_abundance.append(genus_table)
        all_metadata.append(meta)

    # Merge all studies
    logger.info("Merging %d studies...", len(all_abundance))

    abundance = pd.concat(all_abundance, axis=0, sort=True).fillna(0.0)
    abundance = abundance.sort_index(axis=1)
    abundance.index.name = "sample_id"

    # Drop "unclassified" column if present
    if "unclassified" in abundance.columns:
        abundance = abundance.drop(columns=["unclassified"])

    metadata = pd.concat(all_metadata, axis=0)
    metadata.index.name = "sample_id"

    # Save
    abundance_path = PROCESSED_DIR / "genus_abundance.parquet"
    metadata_path = PROCESSED_DIR / "metadata.parquet"

    abundance.to_parquet(abundance_path)
    metadata.to_parquet(metadata_path)

    sparsity = (abundance.values == 0).sum() / abundance.size * 100
    print(f"  genus: {abundance.shape[0]} samples x {abundance.shape[1]} genera, {sparsity:.1f}% sparse")
    print(f"  metadata: {metadata.shape[0]} samples x {metadata.shape[1]} columns")
    print(f"  studies: {metadata['study_id'].nunique()}")
    print(f"  diseases: {sorted(metadata['disease'].unique())}")
