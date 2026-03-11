import json
import logging

import biom.util
import redbiom
import redbiom._requests
import redbiom.fetch
import redbiom.search

from microbiome_db.sources.qiita.config import (
    BATCH_SIZE,
    DEFAULT_CONTEXT,
    DEFAULT_MAX_SAMPLES,
    METADATA_QUERY,
    RAW_DIR,
)

logger = logging.getLogger(__name__)


def _find_samples(context: str, query: str, max_samples: int) -> list[str]:
    """Search for matching sample IDs via redbiom metadata search."""
    cfg = redbiom.get_config()
    get = redbiom._requests.make_get(cfg)

    # metadata_full searches across all metadata, not context-specific
    logger.info("Searching for samples: %s", query)
    samples = redbiom.search.metadata_full(query, get=get)
    logger.info("Found %d samples matching metadata query", len(samples))

    # Get samples that have data in our target context (ambiguous IDs
    # match the metadata format; unambiguous adds artifact prefix)
    samples_in_ctx = set(
        redbiom.fetch.samples_in_context(context, unambiguous=False, get=get)
    )
    logger.info("Context %s has %d samples", context, len(samples_in_ctx))

    # Intersect: samples matching metadata AND having data in context
    valid = sorted(samples & samples_in_ctx)
    logger.info("Samples with both metadata match and data: %d", len(valid))

    if len(valid) > max_samples:
        logger.info("Limiting to %d samples", max_samples)
        valid = valid[:max_samples]

    return valid


def _fetch_biom_batched(
    samples: list[str], context: str, output_dir, batch_size: int
) -> list:
    """Fetch BIOM tables in batches to manage memory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_files = []

    for i in range(0, len(samples), batch_size):
        batch = samples[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(samples) + batch_size - 1) // batch_size
        batch_file = output_dir / f"batch_{batch_num:04d}.biom"

        if batch_file.exists():
            logger.info("Batch %d/%d already exists, skipping", batch_num, total_batches)
            batch_files.append(batch_file)
            continue

        logger.info(
            "Fetching batch %d/%d (%d samples)...",
            batch_num, total_batches, len(batch),
        )

        try:
            table, ambig_map = redbiom.fetch.data_from_samples(
                context, batch, skip_taxonomy=False
            )
            if table is not None and table.shape[0] > 0 and table.shape[1] > 0:
                with biom.util.biom_open(str(batch_file), "w") as f:
                    table.to_hdf5(f, "redbiom")
                batch_files.append(batch_file)
                logger.info("  Saved: %d samples x %d features", table.shape[1], table.shape[0])
            else:
                logger.warning("  Batch %d returned empty table, skipping", batch_num)
        except Exception as e:
            logger.warning("  Batch %d failed: %s", batch_num, e)

    return batch_files


def download_all(
    force: bool = False,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    context: str = DEFAULT_CONTEXT,
) -> None:
    """Download human stool abundance data from QIITA via redbiom."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    samples_file = RAW_DIR / "sample_ids.json"
    biom_dir = RAW_DIR / "biom_batches"

    # Step 1: Find samples
    if samples_file.exists() and not force:
        with open(samples_file) as f:
            samples = json.load(f)
        print(f"  Loaded {len(samples)} cached sample IDs")
    else:
        samples = _find_samples(context, METADATA_QUERY, max_samples)
        with open(samples_file, "w") as f:
            json.dump(samples, f)
        print(f"  Found {len(samples)} human stool samples")

    if not samples:
        raise RuntimeError("No samples found matching query")

    # Step 2: Fetch abundance data in batches
    print(f"  Fetching abundance data ({len(samples)} samples in batches of {BATCH_SIZE})...")
    batch_files = _fetch_biom_batched(samples, context, biom_dir, BATCH_SIZE)
    print(f"  Downloaded {len(batch_files)} batch files")

    # Step 3: Fetch metadata
    metadata_file = RAW_DIR / "metadata.tsv"
    if metadata_file.exists() and not force:
        print("  Metadata already exists, skipping")
    else:
        print("  Fetching sample metadata...")
        import subprocess

        sample_file_txt = RAW_DIR / "sample_ids.txt"
        with open(sample_file_txt, "w") as f:
            f.write("\n".join(samples))

        result = subprocess.run(
            [
                "redbiom", "fetch", "sample-metadata",
                "--from", str(sample_file_txt),
                "--context", context,
                "--all-columns",
                "--resolve-ambiguities",
                "--output", str(metadata_file),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.warning("Metadata fetch stderr: %s", result.stderr)
        if metadata_file.exists():
            print(f"  Metadata saved to {metadata_file}")
        else:
            logger.warning("Metadata fetch produced no output")

    print("Download complete.")
