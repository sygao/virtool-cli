import shutil
from pathlib import Path
import structlog

from virtool_cli.utils.logging import configure_logger


def run(repo_path: Path, debugging: bool = False):
    cache_path = repo_path / ".cache"

    if cache_path.exists():
        shutil.rmtree(cache_path / 'updates')
