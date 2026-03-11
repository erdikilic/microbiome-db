from pathlib import Path

from microbiome_db.sources._common import source_dirs

SOURCE_NAME = "cmd"
RSCRIPT = "/home/fox/miniforge3/envs/rlang/bin/Rscript"
EXPORT_SCRIPT = Path(__file__).parent / "export.R"

RAW_DIR, INTERMEDIATE_DIR, PROCESSED_DIR = source_dirs(SOURCE_NAME)
