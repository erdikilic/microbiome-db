import click


@click.group()
def cli():
    """curatedMetagenomicData — curated WGS metagenomic abundance profiles."""
    pass


@cli.command()
@click.option("--force", is_flag=True, help="Re-export even if files exist")
def download(force):
    """Export abundance data from R package via Rscript."""
    from microbiome_db.sources.cmd.download import download_all

    download_all(force=force)


@cli.command()
def build():
    """Build abundance matrices and metadata Parquet from exported CSVs."""
    from microbiome_db.sources.cmd.build import build_all

    build_all()


@cli.command()
def validate():
    """Validate processed outputs."""
    from microbiome_db.sources.cmd.validate import validate

    ok = validate()
    raise SystemExit(0 if ok else 1)


@cli.command()
@click.option("--force", is_flag=True, help="Re-export even if files exist")
def run(force):
    """Run full pipeline: download (R export) -> build -> validate."""
    from microbiome_db.sources.cmd.build import build_all
    from microbiome_db.sources.cmd.download import download_all
    from microbiome_db.sources.cmd.validate import validate

    print("=== Step 1/3: Export from R ===")
    download_all(force=force)

    print("\n=== Step 2/3: Build ===")
    build_all()

    print("\n=== Step 3/3: Validate ===")
    ok = validate()
    raise SystemExit(0 if ok else 1)
