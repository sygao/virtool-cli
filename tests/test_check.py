from pathlib import Path
import pytest
import subprocess
import shutil
import json
from paths import TEST_FILES_PATH

from virtool_cli.utils.reference import get_isolate_paths
from virtool_cli.utils.storage import read_otu

TEST_SRC_PATH = TEST_FILES_PATH / "src_test"
TEST_BAD_SRC_PATH = TEST_FILES_PATH / "src_malformed"

TEST_DIRS = [
    "abaca_bunchy_top_virus--c93ec9a9",
    "babaco_mosaic_virus--xcl20vqt",
    "cabbage_leaf_curl_jamaica_virus--d226290f",
    "faba_bean_necrotic_stunt_alphasatellite_1--6444acf3",
]


class OTUCheck:
    @pytest.fixture()
    def malformed_src(self, tmp_path):
        test_src_path = tmp_path / "src"
        shutil.copytree(TEST_SRC_PATH, test_src_path)

        abaca_path = test_src_path / "abaca_bunchy_top_virus--c93ec9a9/"
        for isolate_path in get_isolate_paths(abaca_path):
            shutil.rmtree(isolate_path)

        babaco_path = test_src_path / "babaco_mosaic_virus--xcl20vqt"
        otu = json.loads((babaco_path / 'otu.json').read_text())
        with open(babaco_path / 'otu.json', "w") as f:
            json.dump(otu, f, indent=4)

        return test_src_path

    @pytest.fixture()
    def command(self, otu_path):
        return ["virtool", "ref", "check", "otu", "--otu_path", str(otu_path)]

    @pytest.mark.parametrize("otu_dir", TEST_DIRS)
    def test_success(self, otu_dir):
        otu_path = TEST_SRC_PATH / otu_dir
        output = subprocess.check_output(
            self.command(otu_path)
        ).decode("utf-8")

        assert output.strip() == "True"

    @pytest.mark.parametrize("otu_dir", TEST_DIRS)
    def test_otu_check_fail(self, otu_dir):
        otu_path = self.malformed_src / otu_dir
        output = subprocess.check_output(
            ["virtool", "ref", "check", "otu", "--otu_path", str(otu_path)]
        ).decode("utf-8")

        print(output)

        assert output.strip() == "False"
