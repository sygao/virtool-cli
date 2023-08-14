from pathlib import Path
import structlog
import click
from logging import INFO, DEBUG

from virtool_cli.accessions.checkup import run as run_checkup
from virtool_cli.accessions.initialize import run as run_init
from virtool_cli.accessions.update import run as run_update

logger = structlog.get_logger()


@click.group("acc")
def acc():
    """
    Commands related to accession catalogues.
    """
    pass

@acc.command()
@click.option(
    "-src",
    "--src_path",
    required=True,
    type=str,
    help="the path to a reference directory",
)
@click.option(
    "-cat",
    "--catalog_path",
    required=True,
    type=str,
    default='.cache/catalog',
    help="the path to a catalog directory",
)
@click.option('--debug/--no-debug', default=False)
def init(src_path, catalog_path, debug):
    """Generate a catalog of all included accessions in a src directory"""
    if not Path(src_path).exists():
        logger.critical('Source directory does not exist')
        return
    
    catalog_dir = Path(catalog_path)
    if not catalog_dir.exists():
        catalog_dir.mkdir(parents=True)

    try:
        run_init(
            src_path=Path(src_path),
            catalog_path=Path(catalog_path),
            debugging=debug
        )
    except (FileNotFoundError, NotADirectoryError) as e:
        logger.critical("Not a valid reference directory")


@acc.command()
@click.option(
    "-cat",
    "--catalog_path",
    required=True,
    type=str,
    default='.cache/catalog',
    help="the path to a catalog directory",
)
@click.option(
    "-src",
    "--src_path",
    required=True,
    type=str,
    help="the path to a reference directory",
)
@click.option('--debug/--no-debug', default=False)
def update(catalog_path, src_path, debug):
    """Generate a catalog of all included accessions in a src directory"""
    if not Path(src_path).exists():
        logger.critical('Source directory does not exist')
        return
    
    catalog_dir = Path(catalog_path)
    if not catalog_dir.exists():
        logger.critical('Catalog directory does not exist')
        return

    try:
        run_update(
            src_path=Path(src_path),
            catalog_path=Path(catalog_path),
            debugging=debug
        )
    except (FileNotFoundError, NotADirectoryError) as e:
        logger.critical("Not a valid reference directory")

@acc.command()
@click.option(
    "-cat",
    "--catalog_path",
    required=True,
    type=str,
    help="the path to a catalog directory",
)
@click.option('--debug/--no-debug', default=False)
def checkup(catalog_path, debug):
    """Run a check on the accession catalogue for outstanding issues."""

    if not Path(catalog_path).exists():
        logger.critical('Source directory does not exist')

    try:
        run_checkup(Path(catalog_path), debugging=debug)
    except (FileNotFoundError, NotADirectoryError) as e:
        logger.critical('Not a valid reference directory')