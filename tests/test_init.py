import pytest
import os
import tarfile
from pathlib import Path
# from dotenv import load_dotenv
from requests import JSONDecodeError

from paths import TEST_FILES_PATH

# env = load_dotenv(TEST_FILES_PATH / 'secrets.env')

TEST_PATH = TEST_FILES_PATH / "new_repo"

cache_path = TEST_FILES_PATH / '.cache'
cache_path.mkdir(exist_ok=True)

from virtool_cli import init

def test_fetch_callers_tarfile():
    caller_path = init.fetch_callers(
        api_url=os.environ['CALLER_API_URL'],
        cache_path=cache_path,
        gh_token=os.environ['GH_PAT'])
    
    assert tarfile.is_tarfile(caller_path)

def test_bad_extant_url():
    with pytest.raises(KeyError):
        caller_tarfile = init.fetch_callers(
            api_url='https://api.github.com',
            cache_path=cache_path)

# if __name__ == '__main__':
#     # env = load_dotenv(TEST_FILES_PATH / 'secrets.env')

#     test_fetch_callers_tarfile()