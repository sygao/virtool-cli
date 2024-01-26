import shutil
from pathlib import Path
import structlog
import click

from virtool_cli.utils.logging import configure_logger

base_logger = structlog.get_logger()


def run(repo_path: Path, debugging: bool = False):
    click.echo("Cleaning cache files...")
    cache_path = repo_path / ".cache"
    clean_cache(cache_path)


def clean_cache(cache_path):
    if cache_path.exists():
        shutil.rmtree(cache_path / 'updates')
