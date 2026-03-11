# microbiome-db

Multi-source pipeline for building sparse abundance matrices and sample metadata from public human gut microbiome databases.

## Sources

| Source | Samples | Genus | Species | Metadata | Status |
|--------|---------|-------|---------|----------|--------|
| [GMrepo](https://gmrepo.humangut.info/) | 68,723 | 2,214 | 2,894 (WGS only) | 13 columns | Done |
| [MicrobiomeHD](https://zenodo.org/records/569601) | 5,343 | 1,143 | No (16S) | 29 studies, 13 diseases | Done |
| [curatedMetagenomicData](https://waldronlab.io/curatedMetagenomicData/) | 21,030 | 493 | 1,645 (WGS) | 17 columns, 86 studies | Done |
| [QIITA](https://qiita.ucsd.edu/) | 7,244 | 818 | No (16S) | 593 columns | Done |

## Install

```bash
pip install -e .
```

## Usage

Each source has its own subcommand with `download`, `parse`, `build`, `validate`, and `run` steps:

```bash
microbiome-db gmrepo run           # GMrepo full pipeline
microbiome-db microbiomehd run     # MicrobiomeHD full pipeline
microbiome-db cmd run              # curatedMetagenomicData (requires R)
microbiome-db qiita run            # QIITA via redbiom (requires redbiom)
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
```

Each source outputs Parquet files independently. No cross-source taxonomy harmonization.

## Requirements

Python >= 3.10, pandas, pyarrow, requests, click, tqdm.

curatedMetagenomicData requires R with the Bioconductor package installed (`conda install -c bioconda bioconductor-curatedmetagenomicdata`).

Optional: `pip install -e ".[qiita]"` for QIITA/redbiom support.
