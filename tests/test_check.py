import pytest
import subprocess
import json
import shutil
from paths import TEST_FILES_PATH

from virtool_cli.utils.reference import get_isolate_paths

TEST_SRC_PATH = TEST_FILES_PATH / "src_test"
# TEST_BAD_SRC_PATH = TEST_FILES_PATH / "src_malformed"

TEST_DIRS = [
    "abaca_bunchy_top_virus--c93ec9a9",
    "babaco_mosaic_virus--xcl20vqt",
    "cabbage_leaf_curl_jamaica_virus--d226290f",
    "faba_bean_necrotic_stunt_alphasatellite_1--6444acf3",
]


def delete_isolates(otu_path):
    for isolate_path in get_isolate_paths(otu_path):
        shutil.rmtree(isolate_path)


def delete_isolate_metadata(otu_path):
    for isolate_path in get_isolate_paths(otu_path):
        (isolate_path / "isolate.json").unlink()


def remove_otu_id(otu_path):
    otu = json.loads((otu_path / 'otu.json').read_text())
    otu["_id"] = ""
    with open(otu_path / 'otu.json', "w") as f:
        json.dump(otu, f, indent=4)


def remove_isolate_id(otu_path):
    for isolate_path in get_isolate_paths(otu_path):
        isolate = json.loads((isolate_path / 'isolate.json').read_text())
        isolate["id"] = ""
        with open(isolate_path / 'isolate.json', "w") as f:
            json.dump(isolate, f, indent=4)


@pytest.fixture()
def malformed_src(tmp_path):
    test_src_path = tmp_path / "src"
    shutil.copytree(TEST_SRC_PATH, test_src_path)

    delete_isolates(test_src_path / TEST_DIRS[0])

    remove_otu_id(test_src_path / TEST_DIRS[1])

    delete_isolate_metadata(test_src_path / TEST_DIRS[2])

    remove_isolate_id(test_src_path / TEST_DIRS[3])

    return test_src_path


@pytest.fixture()
def command(otu_path):
    return ["virtool", "ref", "check", "otu", "--otu_path", str(otu_path)]


@pytest.mark.parametrize("otu_dir", TEST_DIRS)
def test_otu_check_success(otu_dir):
    otu_path = TEST_SRC_PATH / otu_dir
    output = subprocess.check_output(
        ["virtool", "ref", "check", "otu", "--otu_path", str(otu_path)]
    ).decode("utf-8")

    assert output.strip() == "True"


@pytest.mark.parametrize("otu_dir", TEST_DIRS)
def test_otu_check_fail(otu_dir, malformed_src):
    # otu_path = TEST_BAD_SRC_PATH / otu_dir
    otu_path = malformed_src / otu_dir
    output = subprocess.check_output(
        ["virtool", "ref", "check", "otu", "--otu_path", str(otu_path)]
    ).decode("utf-8")

    assert output.strip() == "False"
