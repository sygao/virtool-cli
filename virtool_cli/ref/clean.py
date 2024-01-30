import shutil
from pathlib import Path
import structlog

base_logger = structlog.get_logger()


def run(repo_path: Path):
    logger = base_logger.bind(path=str(repo_path))
    logger.info("Cleaning cache files...")

    cache_path = repo_path / ".cache"
    clean_cache(cache_path)


def clean_cache(cache_path):
    logger = base_logger.bind(cache_path=str(cache_path))
    cache_folders = ["updates", "logs"]

    try:
        for p in cache_path.iterdir():
            if p.is_dir() and p.name in cache_folders:
                shutil.rmtree(p)
            else:
                p.unlink(missing_ok=True)
    finally:
        logger.info("Cache cleared.")
