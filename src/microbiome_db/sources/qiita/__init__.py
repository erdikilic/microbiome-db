import click


@click.group()
def cli():
    """QIITA — large-scale microbiome abundance data via redbiom."""
    pass


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
@click.option("--max-samples", default=50000, help="Max samples to download")
def download(force, max_samples):
    """Download human stool abundance data from QIITA via redbiom."""
    from microbiome_db.sources.qiita.download import download_all

    download_all(force=force, max_samples=max_samples)


@cli.command()
def build():
    """Build genus abundance matrix from BIOM files."""
    from microbiome_db.sources.qiita.build import build_all

    build_all()


@cli.command()
def validate():
    """Validate processed outputs."""
    from microbiome_db.sources.qiita.validate import validate

    ok = validate()
    raise SystemExit(0 if ok else 1)


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
@click.option("--max-samples", default=50000, help="Max samples to download")
def run(force, max_samples):
    """Run full pipeline: download -> build -> validate."""
    from microbiome_db.sources.qiita.build import build_all
    from microbiome_db.sources.qiita.download import download_all
    from microbiome_db.sources.qiita.validate import validate

    print("=== Step 1/3: Download ===")
    download_all(force=force, max_samples=max_samples)

    print("\n=== Step 2/3: Build ===")
    build_all()

    print("\n=== Step 3/3: Validate ===")
    ok = validate()
    raise SystemExit(0 if ok else 1)
