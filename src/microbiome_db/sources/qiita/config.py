from microbiome_db.sources._common import source_dirs

SOURCE_NAME = "qiita"

# The largest 16S V4 context with OTU-level data (Greengenes 97%)
DEFAULT_CONTEXT = (
    "Pick_closed-reference_OTUs-Greengenes-Illumina-16S-V4-150nt-bd7d4d"
)

# redbiom metadata query to find human stool samples
METADATA_QUERY = 'where sample_type == "stool" and host_taxid == 9606'

# How many samples to fetch per batch (memory management)
BATCH_SIZE = 5000

# Default max samples to download (configurable via CLI)
DEFAULT_MAX_SAMPLES = 50000

RAW_DIR, INTERMEDIATE_DIR, PROCESSED_DIR = source_dirs(SOURCE_NAME)
