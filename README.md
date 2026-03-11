# microbiome-db

Multi-source pipeline for building sparse abundance matrices and sample metadata from public human gut microbiome databases.

## Sources

| Source | Samples | Genus | Species | Metadata | Status |
|--------|---------|-------|---------|----------|--------|
| [GMrepo](https://gmrepo.humangut.info/) | 68,723 | 2,214 | 2,894 (WGS only) | 13 columns | Done |
| [curatedMetagenomicData](https://waldronlab.io/curatedMetagenomicData/) | ~20,000 | Yes | Yes | Curated | Planned |
| [MicrobiomeHD](https://zenodo.org/records/569601) | ~3,500 | Yes | No (16S) | Case/control | Planned |
| [QIITA](https://qiita.ucsd.edu/) | 460,000+ | Yes | Varies | Rich | Planned |
| [gutMEGA](https://gutmega.omicsbio.info/) | 776 phenotypes | Yes | Yes | Phenotype-level | Planned |

## Install

```bash
pip install -e .
```

## Usage

Each source has its own subcommand with `download`, `parse`, `build`, `validate`, and `run` steps:

```bash
microbiome-db gmrepo run           # GMrepo full pipeline
microbiome-db gmrepo validate      # validate outputs only
microbiome-db run-all              # all sources
```

## Data layout

```
data/sources/
  gmrepo/{raw,intermediate,processed}/
  cmd/{raw,processed}/
  microbiomehd/{raw,intermediate,processed}/
  qiita/{raw,processed}/
  gutmega/{raw,processed}/
```

Each source outputs Parquet files independently. No cross-source taxonomy harmonization.

## Requirements

Python >= 3.10, pandas, pyarrow, requests, click, tqdm.

Optional: `pip install -e ".[qiita]"` for QIITA/redbiom support.
