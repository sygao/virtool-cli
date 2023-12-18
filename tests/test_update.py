import pytest
import shutil
import json
import subprocess
from subprocess import CompletedProcess

from paths import TEST_FILES_PATH

SOURCE_PATH = TEST_FILES_PATH / "src_test"
TEST_ACCLOG_PATH = TEST_FILES_PATH / "catalog"


class TestEmptyRepo:
    def test_empty_success(self, empty_repo_path):
        subprocess.run(["virtool", "ref", "init", "-repo", str(empty_repo_path)])

        completed_process = run_update(
            src_path=empty_repo_path / "src", catalog_path=TEST_ACCLOG_PATH
        )

        assert completed_process.returncode == 0

    def test_empty_fail(self, empty_repo_path):
        subprocess.run(["virtool", "ref", "init", "-repo", str(empty_repo_path)])

        completed_process = run_update(
            src_path=empty_repo_path / "src", catalog_path=empty_repo_path / "catalog"
        )

        assert completed_process.returncode != 0


class TestPartialRepo:
    @pytest.fixture()
    def partial_src(self, tmp_path):
        src_partial_path = tmp_path / "src_partial"

        otu_subset = [
            'gaillardia_latent_virus--f8a56910',
            'habenaria_mosaic_virus--a89b6529',
            'impatiens_flower_break_potyvirus--e7wkndjc'
        ]

        for otu_filename in otu_subset:
            shutil.copytree(
                SOURCE_PATH / otu_filename,
                src_partial_path / otu_filename
            )

        return src_partial_path

    @pytest.fixture()
    def updated_src(self, tmp_path):
        return tmp_path / "src"

    @pytest.fixture()
    def pre_reference(self, tmp_path):
        return tmp_path / "reference_pre.json"

    @pytest.fixture()
    def post_reference(self, tmp_path):
        return tmp_path / "reference_post.json"

    def test_update_success(self, partial_src, updated_src, pre_reference, post_reference):
        """
        Test that updates actually pull something.
        """
        src_partial = partial_src
        fetch_path = updated_src
        pre_update_ref_path = pre_reference
        post_update_ref_path = post_reference

        shutil.copytree(src_partial, fetch_path)

        run_build(src_path=fetch_path, output_path=pre_update_ref_path)

        completed_process = run_update(src_path=fetch_path, catalog_path=TEST_ACCLOG_PATH)
        assert completed_process.returncode == 0

        run_build(src_path=fetch_path, output_path=post_update_ref_path)

        reference_pre = json.loads(pre_update_ref_path.read_text())
        pre_otu_dict = convert_to_dict(reference_pre["otus"])

        reference_post = json.loads(post_update_ref_path.read_text())
        post_otu_dict = convert_to_dict(reference_post["otus"])

        difference_counter = 0

        for otu_id in post_otu_dict:
            pre_accessions = get_otu_accessions(pre_otu_dict[otu_id])
            post_accessions = get_otu_accessions(post_otu_dict[otu_id])

            print(pre_accessions)
            print(post_accessions)

            if pre_accessions != post_accessions:
                difference_counter += 1

        # Any new data counts
        assert difference_counter > 0


@pytest.fixture()
def empty_repo_path(tmp_path):
    return tmp_path / "repo_empty"


def run_update(src_path, catalog_path=TEST_ACCLOG_PATH) -> CompletedProcess:
    complete_process = subprocess.run(
        [
            "virtool",
            "ref",
            "update",
            "reference",
            "-src",
            str(src_path),
            "-cat",
            str(catalog_path),
        ],
        capture_output=True,
    )

    return complete_process


def run_build(src_path, output_path) -> CompletedProcess:
    complete_process = subprocess.run(
        [
            "virtool",
            "ref",
            "build",
            "-src",
            str(src_path),
            "-o",
            str(output_path),
        ],
        capture_output=True,
    )

    return complete_process


def convert_to_dict(otu_list: list) -> dict:
    """
    Converts a list of OTUs to a dict keyed by Virtool ID

    :param otu_list: A list of deserialized OTU data
    :return: The contents of otu_list keyed by OTU id
    """
    otu_dict = {}
    for otu in otu_list:
        otu_dict[otu["_id"]] = otu
    return otu_dict


def get_otu_accessions(otu_dict: dict) -> set:
    """
    Gets all accessions from an OTU directory and returns a list

    :param otu_dict: Deserialized OTU data
    :return: The accessions included under the OTU directory in a set
    """
    accessions = set()

    for isolate in otu_dict["isolates"]:
        for sequence in isolate["sequences"]:
            accessions.add(sequence["accession"])

    return accessions
