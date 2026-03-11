# GMrepo v3

Pipeline for building sparse abundance matrices and sample metadata from [GMrepo](https://gmrepo.humangut.info/) SQL dumps.

## What it produces

| Output | Description |
|--------|-------------|
| `genus_abundance.parquet` | 68,723 samples x 2,214 genera (WGS + 16S) |
| `species_abundance.parquet` | 26,993 samples x 2,894 species (WGS only) |
| `metadata.parquet` | 68,653 samples x 13 clinical/technical columns |
| `taxonomy.parquet` | 5,191 taxa with full lineage |

Species-level data is only available for whole-genome sequencing (WGS) samples. 16S amplicon samples are limited to genus-level resolution due to insufficient sequence variation in V3-V4 regions.

## Install

```bash
pip install -e .
```

## Usage

Run the full pipeline (download, parse, build, validate):

```bash
gmrepo run
```

Or run steps individually:

```bash
gmrepo download        # fetch SQL dumps from GMrepo
gmrepo parse           # convert to intermediate Parquet
gmrepo build           # build abundance matrices + metadata
gmrepo validate        # check output integrity
```

## Data layout

```
data/
  raw/                  # downloaded .txt.gz files
  intermediate/         # parsed Parquet (one per source file)
  processed/            # final outputs
```

## Requirements

Python >= 3.10, pandas, pyarrow, requests, click, tqdm.
