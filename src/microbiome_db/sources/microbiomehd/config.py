from microbiome_db.sources._common import source_dirs

SOURCE_NAME = "microbiomehd"
ZENODO_API = "https://zenodo.org/api/records/569601"

RAW_DIR, INTERMEDIATE_DIR, PROCESSED_DIR = source_dirs(SOURCE_NAME)
