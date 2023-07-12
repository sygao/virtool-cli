import pytest
import os
import tarfile
from pathlib import Path

from paths import TEST_FILES_PATH

TEST_PATH = TEST_FILES_PATH / "new_repo"

cache_path = TEST_FILES_PATH / '.cache'
cache_path.mkdir(exist_ok=True)

from virtool_cli import init

def test_fetch_callers_tarfile():
    caller_path = init.fetch_callers(
        api_url=os.environ['CALLER_API_URL'],
        cache_path=cache_path,
        gh_token=os.environ['GH_PAT'])
    
    assert cache_path.is_dir()
    
    assert tarfile.is_tarfile(caller_path)

def test_init_run():
    init.run(repo_path=TEST_PATH,
        api_url=os.environ['CALLER_API_URL'],
        cache_path=cache_path,
        gh_token=os.environ['GH_PAT'])
    
    assert (TEST_PATH / 'src').is_dir()

    assert (TEST_PATH / '.github/workflows').is_dir()
