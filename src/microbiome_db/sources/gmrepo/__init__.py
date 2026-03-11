import click


@click.group()
def cli():
    """GMrepo v3 — human gut metagenome abundance database."""
    pass


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
def download(force):
    """Download raw SQL dumps from GMrepo."""
    from microbiome_db.sources.gmrepo.download import download_all

    download_all(force=force)


@cli.command()
def parse():
    """Parse raw .txt.gz files into intermediate Parquet."""
    from microbiome_db.sources.gmrepo.parse import parse_all

    parse_all()


@cli.command()
def build():
    """Build abundance matrices and metadata."""
    from microbiome_db.sources.gmrepo.build_abundance import build_abundance
    from microbiome_db.sources.gmrepo.build_metadata import build_metadata, build_taxonomy

    build_taxonomy()
    build_abundance()
    build_metadata()


@cli.command()
def validate():
    """Validate all processed outputs."""
    from microbiome_db.sources.gmrepo.validate import validate as run_validate

    ok = run_validate()
    raise SystemExit(0 if ok else 1)


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
def run(force):
    """Run full pipeline: download -> parse -> build -> validate."""
    from microbiome_db.sources.gmrepo.build_abundance import build_abundance
    from microbiome_db.sources.gmrepo.build_metadata import build_metadata, build_taxonomy
    from microbiome_db.sources.gmrepo.download import download_all
    from microbiome_db.sources.gmrepo.parse import parse_all
    from microbiome_db.sources.gmrepo.validate import validate as run_validate

    print("=== Step 1/4: Download ===")
    download_all(force=force)

    print("\n=== Step 2/4: Parse ===")
    parse_all()

    print("\n=== Step 3/4: Build ===")
    build_taxonomy()
    build_abundance()
    build_metadata()

    print("\n=== Step 4/4: Validate ===")
    ok = run_validate()
    raise SystemExit(0 if ok else 1)
