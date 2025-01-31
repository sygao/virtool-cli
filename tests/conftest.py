import shutil
from pathlib import Path

import pytest

from virtool_cli.legacy.utils import build_legacy_otu
from virtool_cli.ncbi.cache import NCBICache
from virtool_cli.ncbi.client import NCBIClient
from virtool_cli.ref.repo import EventSourcedRepo
from virtool_cli.ref.utils import DataType


@pytest.fixture()
def test_files_path():
    return Path(__file__).parent / "files"


@pytest.fixture()
def scratch_path(scratch_repo: EventSourcedRepo) -> Path:
    """The path to a scratch reference repository."""
    return scratch_repo.path


@pytest.fixture()
def scratch_repo(test_files_path: Path, tmp_path: Path) -> EventSourcedRepo:
    """A prepared scratch repository."""
    path = tmp_path / "scratch_repo"

    shutil.copytree(test_files_path / "src_test", path / "src")
    shutil.copytree(test_files_path / "cache_test", path / ".cache")

    return EventSourcedRepo(path)


@pytest.fixture()
def scratch_ncbi_cache_path(
    scratch_path: Path,
) -> Path:
    """The path to a scratch NCBI client cache."""
    return scratch_path / ".cache"


@pytest.fixture()
def scratch_ncbi_cache(scratch_ncbi_cache_path: Path):
    """A scratch NCBI cache with preloaded data."""
    return NCBICache(scratch_ncbi_cache_path)


@pytest.fixture()
def scratch_ncbi_client(scratch_ncbi_cache_path: Path):
    """A scratch NCBI client with a preloaded cache."""
    return NCBIClient(scratch_ncbi_cache_path, ignore_cache=False)


@pytest.fixture()
def empty_repo(tmp_path: Path) -> EventSourcedRepo:
    return EventSourcedRepo.new(
        DataType.GENOME,
        "Generic Viruses",
        tmp_path / "test_repo",
        "virus",
    )


@pytest.fixture()
def legacy_repo_path(
    test_files_path: Path,
    tmp_path: Path,
) -> Path:
    """The path to a scratch legacy reference."""
    path = tmp_path / "legacy_repo"

    shutil.copytree(test_files_path / "src_v1", path / "src", dirs_exist_ok=True)

    return path


@pytest.fixture()
def legacy_otu(legacy_repo_path: Path) -> dict:
    return build_legacy_otu(
        legacy_repo_path / "src" / "a" / "abaca_bunchy_top_virus",
    )


@pytest.fixture()
def cache_example_path(test_files_path):
    return test_files_path / "cache_test"


@pytest.fixture()
def precached_repo(cache_example_path: Path, tmp_path: Path) -> EventSourcedRepo:
    path = tmp_path / "precached_repo"

    repo = EventSourcedRepo.new(DataType.GENOME, "Empty", path, "virus")

    shutil.copytree(cache_example_path, path / ".cache" / "ncbi")

    return repo
