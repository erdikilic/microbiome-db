import logging

import pandas as pd

from microbiome_db.sources.cmd.config import PROCESSED_DIR

logger = logging.getLogger(__name__)


def validate() -> bool:
    """Validate curatedMetagenomicData processed outputs."""
    ok = True
    print("\n=== curatedMetagenomicData Validation ===\n")

    species_path = PROCESSED_DIR / "species_abundance.parquet"
    genus_path = PROCESSED_DIR / "genus_abundance.parquet"
    meta_path = PROCESSED_DIR / "metadata.parquet"

    for p in (species_path, genus_path, meta_path):
        if not p.exists():
            print(f"FAIL: {p.name} not found")
            ok = False

    if not ok:
        return False

    species = pd.read_parquet(species_path)
    genus = pd.read_parquet(genus_path)
    meta = pd.read_parquet(meta_path)

    for name, df in [("species", species), ("genus", genus)]:
        print(f"--- {name}_abundance ---")
        print(f"  Shape: {df.shape}")

        vmin, vmax = df.values.min(), df.values.max()
        print(f"  Range: [{vmin:.4f}, {vmax:.4f}]")
        if vmin < 0:
            print(f"  FAIL: negative values (min={vmin})")
            ok = False
        if vmax > 100.01:
            print(f"  FAIL: values > 100 (max={vmax})")
            ok = False

        nan_count = pd.isna(df.values).sum()
        if nan_count > 0:
            print(f"  FAIL: {nan_count} NaN values")
            ok = False
        else:
            print(f"  NaN: none")

        if df.index.duplicated().any():
            print(f"  FAIL: {df.index.duplicated().sum()} duplicate sample IDs")
            ok = False
        else:
            print(f"  Duplicates: none")

        sparsity = (df.values == 0).sum() / df.size * 100
        print(f"  Sparsity: {sparsity:.1f}%")
        print()

    print(f"--- metadata ---")
    print(f"  Shape: {meta.shape}")
    print(f"  Columns: {list(meta.columns)}")
    if "study_name" in meta.columns:
        print(f"  Studies: {meta['study_name'].nunique()}")
    print()

    status = "PASSED" if ok else "FAILED"
    print(f"=== Validation {status} ===")
    return ok
