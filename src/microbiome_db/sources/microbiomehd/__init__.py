import click


@click.group()
def cli():
    """MicrobiomeHD — 28 case-control 16S gut microbiome studies."""
    pass


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
def download(force):
    """Download study archives from Zenodo."""
    from microbiome_db.sources.microbiomehd.download import download_all

    download_all(force=force)


@cli.command()
def build():
    """Build genus abundance matrix and metadata from OTU tables."""
    from microbiome_db.sources.microbiomehd.build import build_all

    build_all()


@cli.command()
def validate():
    """Validate processed outputs."""
    from microbiome_db.sources.microbiomehd.validate import validate

    ok = validate()
    raise SystemExit(0 if ok else 1)


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
def run(force):
    """Run full pipeline: download -> build -> validate."""
    from microbiome_db.sources.microbiomehd.build import build_all
    from microbiome_db.sources.microbiomehd.download import download_all
    from microbiome_db.sources.microbiomehd.validate import validate

    print("=== Step 1/3: Download ===")
    download_all(force=force)

    print("\n=== Step 2/3: Build ===")
    build_all()

    print("\n=== Step 3/3: Validate ===")
    ok = validate()
    raise SystemExit(0 if ok else 1)
