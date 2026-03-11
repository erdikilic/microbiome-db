import logging

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


@click.group()
def cli():
    """GMrepo v3 — abundance & metadata pipeline."""
    pass


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
def download(force):
    """Download raw data from GMrepo."""
    from gmrepo.download import download_all

    download_all(force=force)


@cli.command()
def parse():
    """Parse raw .txt.gz files into intermediate Parquet."""
    from gmrepo.parse import parse_all

    parse_all()


@cli.command()
def build():
    """Build abundance matrices and metadata from intermediate data."""
    from gmrepo.build_abundance import build_abundance
    from gmrepo.build_metadata import build_metadata, build_taxonomy

    build_taxonomy()
    build_abundance()
    build_metadata()


@cli.command()
def validate():
    """Validate all processed outputs."""
    from gmrepo.validate import validate as run_validate

    ok = run_validate()
    raise SystemExit(0 if ok else 1)


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
def run(force):
    """Run full pipeline: download → parse → build → validate."""
    from gmrepo.build_abundance import build_abundance
    from gmrepo.build_metadata import build_metadata, build_taxonomy
    from gmrepo.download import download_all
    from gmrepo.parse import parse_all
    from gmrepo.validate import validate as run_validate

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


if __name__ == "__main__":
    cli()
