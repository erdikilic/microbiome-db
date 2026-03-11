import logging

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


@click.group()
def cli():
    """microbiome-db — multi-source microbiome abundance pipeline."""
    pass


def _register_sources():
    from microbiome_db.sources.cmd import cli as cmd_cli
    from microbiome_db.sources.gmrepo import cli as gmrepo_cli
    from microbiome_db.sources.microbiomehd import cli as mhd_cli

    cli.add_command(gmrepo_cli, "gmrepo")
    cli.add_command(mhd_cli, "microbiomehd")
    cli.add_command(cmd_cli, "cmd")


_register_sources()


@cli.command()
@click.option("--force", is_flag=True, help="Re-download even if files exist")
def run_all(force):
    """Run all source pipelines."""
    from microbiome_db.sources.gmrepo.build_abundance import build_abundance
    from microbiome_db.sources.gmrepo.build_metadata import build_metadata, build_taxonomy
    from microbiome_db.sources.gmrepo.download import download_all
    from microbiome_db.sources.gmrepo.parse import parse_all
    from microbiome_db.sources.gmrepo.validate import validate as run_validate

    print("=== GMrepo ===")
    download_all(force=force)
    parse_all()
    build_taxonomy()
    build_abundance()
    build_metadata()
    ok = run_validate()
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    cli()
