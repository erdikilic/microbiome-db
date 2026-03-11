from pathlib import Path

GMREPO_BASE = "https://gmrepo.humangut.info/Downloads/SQLDumps"

FILES = {
    "abundance": "species_abundance.txt.gz",
    "samples": "sample_to_run_info.txt.gz",
    "projects": "projects.txt.gz",
    "mesh": "mesh_data.txt.gz",
    "taxonomy": "superkingdom2descendents.txt.gz",
}

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent
RAW_DIR = ROOT_DIR / "data" / "sources" / "gmrepo" / "raw"
INTERMEDIATE_DIR = ROOT_DIR / "data" / "sources" / "gmrepo" / "intermediate"
PROCESSED_DIR = ROOT_DIR / "data" / "sources" / "gmrepo" / "processed"

CHUNK_SIZE = 10_000
