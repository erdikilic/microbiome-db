import logging

import pandas as pd

from gmrepo.config import PROCESSED_DIR

logger = logging.getLogger(__name__)


def validate() -> bool:
    """Validate all processed outputs. Returns True if all checks pass."""
    ok = True
    print("\n=== Validation Report ===\n")

    # Load all outputs
    species_path = PROCESSED_DIR / "species_abundance.parquet"
    genus_path = PROCESSED_DIR / "genus_abundance.parquet"
    meta_path = PROCESSED_DIR / "metadata.parquet"
    tax_path = PROCESSED_DIR / "taxonomy.parquet"

    for p in (species_path, genus_path, meta_path, tax_path):
        if not p.exists():
            print(f"FAIL: {p.name} not found")
            ok = False

    if not ok:
        return False

    species = pd.read_parquet(species_path)
    genus = pd.read_parquet(genus_path)
    meta = pd.read_parquet(meta_path)
    tax = pd.read_parquet(tax_path)

    # --- Abundance checks ---
    for name, df in [("species", species), ("genus", genus)]:
        print(f"--- {name}_abundance ---")
        print(f"  Shape: {df.shape}")

        # Range check
        vmin, vmax = df.values.min(), df.values.max()
        print(f"  Range: [{vmin:.4f}, {vmax:.4f}]")
        if vmin < 0:
            print(f"  FAIL: negative values found (min={vmin})")
            ok = False
        if vmax > 100:
            print(f"  FAIL: values > 100 found (max={vmax})")
            ok = False

        # NaN check
        nan_count = pd.isna(df.values).sum()
        if nan_count > 0:
            print(f"  FAIL: {nan_count} NaN values found")
            ok = False
        else:
            print(f"  NaN: none")

        # Duplicate check
        if df.index.duplicated().any():
            n_dup = df.index.duplicated().sum()
            print(f"  FAIL: {n_dup} duplicate sample IDs")
            ok = False
        else:
            print(f"  Duplicates: none")

        # Sparsity
        sparsity = (df.values == 0).sum() / df.size * 100
        print(f"  Sparsity: {sparsity:.1f}%")

        # Per-sample sum
        sums = df.sum(axis=1)
        print(f"  Per-sample sum: mean={sums.mean():.2f}, min={sums.min():.2f}, max={sums.max():.2f}")
        print()

    # --- Metadata checks ---
    print("--- metadata ---")
    print(f"  Shape: {meta.shape}")
    print(f"  Columns: {list(meta.columns)}")
    if meta.index.duplicated().any():
        n_dup = meta.index.duplicated().sum()
        print(f"  FAIL: {n_dup} duplicate sample IDs")
        ok = False
    else:
        print(f"  Duplicates: none")
    print()

    # --- Cross-file checks ---
    print("--- Cross-file consistency ---")
    species_ids = set(species.index)
    genus_ids = set(genus.index)
    meta_ids = set(meta.index)

    sp_not_meta = species_ids - meta_ids
    gen_not_meta = genus_ids - meta_ids
    meta_not_sp = meta_ids - species_ids

    print(f"  Species samples: {len(species_ids)}")
    print(f"  Genus samples: {len(genus_ids)}")
    print(f"  Metadata samples: {len(meta_ids)}")

    if sp_not_meta:
        print(f"  WARNING: {len(sp_not_meta)} species samples missing from metadata")
    if gen_not_meta:
        print(f"  WARNING: {len(gen_not_meta)} genus samples missing from metadata")
    if meta_not_sp:
        print(f"  INFO: {len(meta_not_sp)} metadata samples not in species abundance")

    # Taxonomy coverage
    species_cols = set(species.columns)
    genus_cols = set(genus.columns)
    tax_ids = set(tax.index)

    sp_not_tax = species_cols - tax_ids
    gen_not_tax = genus_cols - tax_ids
    if sp_not_tax:
        print(f"  WARNING: {len(sp_not_tax)} species IDs not in taxonomy")
    if gen_not_tax:
        print(f"  WARNING: {len(gen_not_tax)} genus IDs not in taxonomy")

    print()

    # --- Taxonomy ---
    print("--- taxonomy ---")
    print(f"  Shape: {tax.shape}")
    if "rank" in tax.columns:
        print(f"  Ranks: {tax['rank'].value_counts().to_dict()}")
    print()

    # --- Summary ---
    status = "PASSED" if ok else "FAILED"
    print(f"=== Validation {status} ===")
    return ok
