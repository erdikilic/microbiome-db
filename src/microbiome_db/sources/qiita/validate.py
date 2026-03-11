import logging

import pandas as pd

from microbiome_db.sources.qiita.config import PROCESSED_DIR

logger = logging.getLogger(__name__)


def validate() -> bool:
    """Validate QIITA processed outputs."""
    ok = True
    print("\n=== QIITA Validation ===\n")

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
    print()

    print(f"--- metadata ---")
    print(f"  Shape: {meta.shape}")
    print(f"  Columns: {list(meta.columns)[:10]}{'...' if len(meta.columns) > 10 else ''}")
    print()

    status = "PASSED" if ok else "FAILED"
    print(f"=== Validation {status} ===")
    return ok
