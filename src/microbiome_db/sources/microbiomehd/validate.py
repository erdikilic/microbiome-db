import logging

import pandas as pd

from microbiome_db.sources.microbiomehd.config import PROCESSED_DIR

logger = logging.getLogger(__name__)


def validate() -> bool:
    """Validate MicrobiomeHD processed outputs."""
    ok = True
    print("\n=== MicrobiomeHD Validation ===\n")

    genus_path = PROCESSED_DIR / "genus_abundance.parquet"
    meta_path = PROCESSED_DIR / "metadata.parquet"

    for p in (genus_path, meta_path):
        if not p.exists():
            print(f"FAIL: {p.name} not found")
            ok = False

    if not ok:
        return False

    genus = pd.read_parquet(genus_path)
    meta = pd.read_parquet(meta_path)

    # Abundance checks
    print(f"--- genus_abundance ---")
    print(f"  Shape: {genus.shape}")

    vmin, vmax = genus.values.min(), genus.values.max()
    print(f"  Range: [{vmin:.4f}, {vmax:.4f}]")
    if vmin < 0:
        print(f"  FAIL: negative values (min={vmin})")
        ok = False
    if vmax > 100.01:
        print(f"  FAIL: values > 100 (max={vmax})")
        ok = False

    nan_count = pd.isna(genus.values).sum()
    if nan_count > 0:
        print(f"  FAIL: {nan_count} NaN values")
        ok = False
    else:
        print(f"  NaN: none")

    if genus.index.duplicated().any():
        print(f"  FAIL: {genus.index.duplicated().sum()} duplicate sample IDs")
        ok = False
    else:
        print(f"  Duplicates: none")

    sparsity = (genus.values == 0).sum() / genus.size * 100
    print(f"  Sparsity: {sparsity:.1f}%")

    sums = genus.sum(axis=1)
    print(f"  Per-sample sum: mean={sums.mean():.2f}, min={sums.min():.2f}, max={sums.max():.2f}")
    print()

    # Metadata checks
    print(f"--- metadata ---")
    print(f"  Shape: {meta.shape}")
    print(f"  Columns: {list(meta.columns)}")
    print(f"  Studies: {meta['study_id'].nunique()}")
    print(f"  Diseases: {sorted(meta['disease'].unique())}")
    print()

    # Cross-file consistency
    genus_ids = set(genus.index)
    meta_ids = set(meta.index)
    missing = genus_ids - meta_ids
    if missing:
        print(f"  WARNING: {len(missing)} abundance samples missing from metadata")
    print()

    status = "PASSED" if ok else "FAILED"
    print(f"=== Validation {status} ===")
    return ok
