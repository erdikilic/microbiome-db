#!/usr/bin/env Rscript
# Export curatedMetagenomicData relative abundance + metadata to CSV
# Usage: Rscript export.R <output_dir>

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript export.R <output_dir>")
}
output_dir <- args[1]
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

library(curatedMetagenomicData)

cat("Loading sample metadata...\n")
meta <- sampleMetadata

# Filter to stool/gut samples only
meta <- meta[meta$body_site == "stool", ]
cat(sprintf("  %d stool samples from %d studies\n", nrow(meta), length(unique(meta$study_name))))

# Get relative abundance for all stool samples
cat("Fetching relative abundance data (this may take a while)...\n")
tse <- returnSamples(meta, "relative_abundance", rownames = "short")

cat("Extracting abundance matrix...\n")
abundance <- as.data.frame(as.matrix(SummarizedExperiment::assay(tse)))

# Transpose: rows = samples, columns = taxa
abundance <- as.data.frame(t(abundance))

cat(sprintf("  Abundance matrix: %d samples x %d taxa\n", nrow(abundance), ncol(abundance)))

# Extract metadata
cat("Extracting metadata...\n")
sample_meta <- as.data.frame(SummarizedExperiment::colData(tse))

# Select key metadata columns
keep_cols <- c(
  "study_name", "sample_id", "subject_id", "body_site",
  "study_condition", "disease", "age", "age_category", "gender",
  "country", "non_westernized", "sequencing_platform",
  "number_reads", "number_bases", "BMI", "antibiotics_current_use",
  "infant_age", "NCBI_accession"
)
keep_cols <- keep_cols[keep_cols %in% colnames(sample_meta)]
sample_meta <- sample_meta[, keep_cols]

cat(sprintf("  Metadata: %d samples x %d columns\n", nrow(sample_meta), ncol(sample_meta)))

# Save
abundance_path <- file.path(output_dir, "relative_abundance.csv.gz")
metadata_path <- file.path(output_dir, "metadata.csv.gz")

cat(sprintf("Writing %s...\n", abundance_path))
gz_ab <- gzfile(abundance_path, "w")
write.csv(abundance, gz_ab)
close(gz_ab)

cat(sprintf("Writing %s...\n", metadata_path))
gz_meta <- gzfile(metadata_path, "w")
write.csv(sample_meta, gz_meta)
close(gz_meta)

cat("Export complete.\n")
